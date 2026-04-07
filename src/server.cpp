/*
 * FlexQL Server
 *
 * Wire protocol (matches reference SQLite server exactly):
 *   Each row: "ROW N name1_len:name1value1_len:value1...\n"
 *   End:      "OK\nEND\n"  or  "ERROR:msg\nEND\n"
 *
 * Architecture:
 *   - WAL persistence (append-only log, replayed on startup)
 *   - Chunked arena storage (zero per-row malloc)
 *   - UNIX domain socket (localhost fast path, ~100x lower RTT than TCP)
 *   - Async double-buffer WAL (no disk wait on hot path)
 *   - LFU query cache
 *   - Lazy PK index
 *   - One thread per client, single global mutex
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <cmath>
#include <cerrno>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <stdexcept>
#include <list>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>

#define TCP_PORT   9000
#define UNIX_SOCK  "/tmp/flexql.sock"
#define DATA_DIR   "flexql_data"
#define WAL_FILE   "flexql_data/wal.log"
#define SNAP_FILE  "flexql_data/snapshot.bin"
#define BACKLOG    256
#define SBUF       (8*1024*1024)
#define SLAB_SZ    (64ULL*1024*1024)
#define CELL_SLAB  2000000

static const size_t CHECKPOINT_WAL_RECORDS = 1000000;

/* ── Arena ── */
struct Arena {
    struct Slab { char *data; size_t used; };
    std::vector<Slab> slabs;
    Arena()  { addSlab(); }
    ~Arena() { for (auto &s:slabs) free(s.data); }
    Arena(const Arena&)=delete; Arena& operator=(const Arena&)=delete;
    Arena(Arena&&)=default;     Arena& operator=(Arena&&)=default;
    void addSlab() {
        char *p=(char*)malloc(SLAB_SZ); if(!p) throw std::bad_alloc();
        slabs.push_back({p,0});
    }
    inline uint64_t append(const char *d, uint32_t l) {
        if(!l) return ~0ULL;
        if(__builtin_expect(slabs.back().used+l>SLAB_SZ,0)) addSlab();
        auto &s=slabs.back();
        uint64_t id=((uint64_t)(slabs.size()-1)<<32)|(uint32_t)s.used;
        memcpy(s.data+s.used,d,l); s.used+=l; return id;
    }
    inline uint64_t appendUninit(uint32_t l, char **out) {
        if(!l){ if(out) *out=nullptr; return ~0ULL; }
        if(__builtin_expect(slabs.back().used+l>SLAB_SZ,0)) addSlab();
        auto &s=slabs.back();
        uint64_t id=((uint64_t)(slabs.size()-1)<<32)|(uint32_t)s.used;
        if(out) *out=s.data+s.used;
        s.used+=l;
        return id;
    }
    inline uint64_t appendFromFile(FILE *fp, uint32_t l) {
        if(!l) return ~0ULL;
        char *dst=nullptr;
        uint64_t id=appendUninit(l,&dst);
        if(!dst) return ~0ULL;
        if(fread(dst,1,l,fp)!=l) throw std::runtime_error("snapshot read failed");
        return id;
    }
    inline const char *ptr(uint64_t id) const {
        if(__builtin_expect(id==~0ULL,0)) return "";
        return slabs[id>>32].data+(uint32_t)(id&0xFFFFFFFF);
    }
    void clear() {
        for(size_t i=1;i<slabs.size();i++) free(slabs[i].data);
        slabs.resize(1); slabs[0].used=0;
    }
};

/* ── CellStore ── */
struct Cell { uint64_t id; uint32_t len; };
struct CellStore {
    struct Chunk { Cell *data; size_t used; };
    std::vector<Chunk> chunks; size_t total=0;
    CellStore()  { addChunk(); }
    ~CellStore() { for(auto &c:chunks) free(c.data); }
    CellStore(const CellStore&)=delete; CellStore& operator=(const CellStore&)=delete;
    CellStore(CellStore&&)=default;     CellStore& operator=(CellStore&&)=default;
    void addChunk() {
        Cell *p=(Cell*)malloc(CELL_SLAB*sizeof(Cell)); if(!p) throw std::bad_alloc();
        chunks.push_back({p,0});
    }
    inline void push(uint64_t id, uint32_t l) {
        if(__builtin_expect(chunks.back().used==CELL_SLAB,0)) addChunk();
        auto &c=chunks.back(); c.data[c.used++]={id,l}; ++total;
    }
    inline Cell get(size_t idx) const {
        return chunks[idx/CELL_SLAB].data[idx%CELL_SLAB];
    }
    void clear() {
        for(size_t i=1;i<chunks.size();i++) free(chunks[i].data);
        chunks.resize(1); chunks[0].used=0; total=0;
    }
};

/* ── WAL: async double-buffer ── */
struct WAL {
    FILE  *fp=nullptr;
    char  *buf[2]={nullptr,nullptr};
    size_t used[2]={0,0};
    int    active=0;
    static const size_t HALF=4*1024*1024;
    std::mutex wmu; std::condition_variable wcv;
    bool writePending=false, stopping=false;
    std::thread wthread;

    bool open(const char *path) {
        fp=fopen(path,"ab+"); if(!fp){perror("WAL fopen");return false;}
        buf[0]=(char*)malloc(HALF); buf[1]=(char*)malloc(HALF);
        if(!buf[0]||!buf[1]) return false;
        wthread=std::thread([this]{writeLoop();});
        return true;
    }
    inline void append(const char *sql, size_t len) {
        if(!fp||len>HALF-8) return;
        int a=active;
        if(used[a]+4+len+1>HALF){triggerFlush();a=active;}
        uint32_t u=(uint32_t)len;
        memcpy(buf[a]+used[a],&u,4); used[a]+=4;
        memcpy(buf[a]+used[a],sql,len); used[a]+=len;
        buf[a][used[a]++]='\n';
    }
    void triggerFlush() {
        int old=active,nw=1-old;
        {std::unique_lock<std::mutex> lk(wmu); wcv.wait(lk,[this]{return !writePending;});}
        used[nw]=0; active=nw;
        {std::lock_guard<std::mutex> lk(wmu); writePending=true;}
        wcv.notify_one();
    }
    void writeLoop() {
        while(true) {
            int wi; size_t wl;
            {std::unique_lock<std::mutex> lk(wmu);
             wcv.wait(lk,[this]{return writePending||stopping;});
             if(stopping&&!writePending)break;
             wi=1-active; wl=used[wi];}
            if(wl>0){fwrite(buf[wi],1,wl,fp);}
            {std::lock_guard<std::mutex> lk(wmu); writePending=false;}
            wcv.notify_all();
            if(stopping)break;
        }
    }
    std::vector<std::string> readAll(const char *path) {
        std::vector<std::string> out;
        FILE *r=fopen(path,"rb"); if(!r)return out;
        for(;;){
            uint32_t len; if(fread(&len,1,4,r)!=4)break;
            std::string s(len,'\0');
            if((size_t)fread(&s[0],1,len,r)!=len)break;
            fgetc(r); out.push_back(std::move(s));
        }
        fclose(r); return out;
    }
    void flush() {
        if(!fp)return;
        int a=active;
        if(used[a]>0){fwrite(buf[a],1,used[a],fp);used[a]=0;}
        fflush(fp);
    }
    void close() {
        {std::lock_guard<std::mutex> lk(wmu); stopping=true;}
        wcv.notify_all();
        if(wthread.joinable())wthread.join();
        flush();
        if(fp){fclose(fp);fp=nullptr;}
        free(buf[0]); free(buf[1]); buf[0]=buf[1]=nullptr;
    }
};

/* ── Schema ── */
struct ColDef { std::string name; bool pk=false; };
struct Table {
    std::string name;
    std::vector<ColDef> cols;
    int nc=0, pkCol=0;
    Arena arena; CellStore cells;
    bool idxDirty=false;
    std::unordered_map<std::string,std::vector<size_t>> pkIdx;

    size_t nrows() const { return cells.total/(size_t)nc; }
    inline const char* cp(size_t r,int c) const { return arena.ptr(cells.get(r*nc+c).id); }
    inline uint32_t cl(size_t r,int c) const { return cells.get(r*nc+c).len; }
    std::string cs(size_t r,int c) const { return {cp(r,c),cp(r,c)+cl(r,c)}; }
    int colIdx(const std::string &n) const {
        for(int i=0;i<nc;i++) if(cols[i].name==n) return i; return -1;
    }
    void buildIndex() {
        pkIdx.clear(); pkIdx.reserve(nrows()*2);
        for(size_t r=0;r<nrows();r++) pkIdx[cs(r,pkCol)].push_back(r);
        idxDirty=false;
    }
    void clear(){ cells.clear(); arena.clear(); pkIdx.clear(); idxDirty=false; }
};

/* ── LFU Cache ── */
struct Cache {
    static const int CAP=16384;
    int minF=0;
    uint64_t gen=1;
    struct E{std::string v;int f;std::list<std::string>::iterator it;uint64_t g;};
    std::unordered_map<std::string,E> km;
    std::unordered_map<int,std::list<std::string>> fm;
    bool get(const std::string &k,std::string &out){
        auto i=km.find(k);if(i==km.end())return false;
        if(i->second.g!=gen) return false;
        touch(i);out=i->second.v;return true;}
    void put(const std::string &k,std::string v){
        auto i=km.find(k);
        if(i!=km.end()){
            if(i->second.g==gen){i->second.v=std::move(v);touch(i);return;}
            i->second.v=std::move(v);
            i->second.f=1;
            fm[1].push_front(k);
            i->second.it=fm[1].begin();
            i->second.g=gen;
            minF=1;
            return;
        }
        if((int)km.size()>=CAP)evict();
        fm[1].push_front(k);km[k]={std::move(v),1,fm[1].begin(),gen};minF=1;}
    void inv(const std::string &){
        ++gen;
        fm.clear(); minF=0;
        if(km.size()>(size_t)CAP*2) km.clear();
    }
private:
    using MI=decltype(km)::iterator;
    void touch(MI i){int f=i->second.f;fm[f].erase(i->second.it);
        if(fm[f].empty()){fm.erase(f);if(minF==f)++minF;}
        ++f;i->second.f=f;fm[f].push_front(i->first);i->second.it=fm[f].begin();}
    void erase(const std::string &k){auto i=km.find(k);if(i==km.end())return;
        fm[i->second.f].erase(i->second.it);
        if(fm[i->second.f].empty())fm.erase(i->second.f);km.erase(i);}
    void evict(){if(fm[minF].empty())return;erase(fm[minF].back());}
};

static std::unordered_map<std::string,Table> gDB;
static Cache gCache;
static WAL   gWAL;
static std::mutex gMu;
static std::atomic<uint64_t> gWALRecords{0};

static bool fileExists(const char *path){
    struct stat st{};
    return ::stat(path,&st)==0;
}

static bool writeU32(FILE *fp, uint32_t v){ return fwrite(&v,1,4,fp)==4; }
static bool writeU64(FILE *fp, uint64_t v){ return fwrite(&v,1,8,fp)==8; }
static bool readU32(FILE *fp, uint32_t &v){ return fread(&v,1,4,fp)==4; }
static bool readU64(FILE *fp, uint64_t &v){ return fread(&v,1,8,fp)==8; }

static bool writeStr(FILE *fp, const std::string &s){
    if(s.size()>0xFFFFFFFFu) return false;
    if(!writeU32(fp,(uint32_t)s.size())) return false;
    return s.empty() || fwrite(s.data(),1,s.size(),fp)==s.size();
}

static bool readStr(FILE *fp, std::string &out){
    uint32_t n=0;
    if(!readU32(fp,n)) return false;
    out.assign(n,'\0');
    return n==0 || fread(&out[0],1,n,fp)==n;
}

static bool saveSnapshotLocked(){
    std::string tmp = std::string(SNAP_FILE) + ".tmp";
    FILE *fp=fopen(tmp.c_str(),"wb");
    if(!fp) return false;

    bool ok=true;
    ok = ok && writeU32(fp,(uint32_t)gDB.size());
    for(auto &kv: gDB){
        Table &T = kv.second;
        ok = ok && writeStr(fp,T.name);
        ok = ok && writeU32(fp,(uint32_t)T.nc);
        ok = ok && writeU32(fp,(uint32_t)T.pkCol);
        ok = ok && writeU32(fp,(uint32_t)T.cols.size());
        for(auto &c: T.cols){
            ok = ok && writeStr(fp,c.name);
            ok = ok && writeU32(fp,c.pk?1u:0u);
        }
        ok = ok && writeU64(fp,(uint64_t)T.nrows());
        for(size_t r=0; ok && r<T.nrows(); r++){
            for(int ci=0; ci<T.nc; ci++){
                uint32_t l=T.cl(r,ci);
                ok = ok && writeU32(fp,l);
                if(l){
                    const char *p=T.cp(r,ci);
                    ok = ok && fwrite(p,1,l,fp)==l;
                }
            }
        }
        if(!ok) break;
    }

    if(fp) fclose(fp);
    if(!ok){ unlink(tmp.c_str()); return false; }
    if(rename(tmp.c_str(), SNAP_FILE)!=0){ unlink(tmp.c_str()); return false; }
    return true;
}

static bool loadSnapshot(){
    if(!fileExists(SNAP_FILE)) return false;
    FILE *fp=fopen(SNAP_FILE,"rb");
    if(!fp) return false;

    std::unordered_map<std::string,Table> next;
    uint32_t nt=0;
    bool ok = readU32(fp,nt);
    for(uint32_t ti=0; ok && ti<nt; ti++){
        Table T;
        ok = ok && readStr(fp,T.name);
        uint32_t nc=0, pk=0, ncols=0;
        ok = ok && readU32(fp,nc);
        ok = ok && readU32(fp,pk);
        ok = ok && readU32(fp,ncols);
        T.nc=(int)nc;
        T.pkCol=(int)pk;
        T.cols.clear();
        T.cols.reserve(ncols);
        for(uint32_t ci=0; ok && ci<ncols; ci++){
            ColDef c;
            ok = ok && readStr(fp,c.name);
            uint32_t ispk=0;
            ok = ok && readU32(fp,ispk);
            c.pk = (ispk!=0);
            T.cols.push_back(std::move(c));
        }
        uint64_t nrows=0;
        ok = ok && readU64(fp,nrows);
        for(uint64_t r=0; ok && r<nrows; r++){
            for(int ci=0; ok && ci<T.nc; ci++){
                uint32_t l=0;
                ok = ok && readU32(fp,l);
                if(!ok) break;
                if(l==0){
                    T.cells.push(~0ULL,0);
                } else {
                    try{
                        uint64_t id=T.arena.appendFromFile(fp,l);
                        T.cells.push(id,l);
                    }catch(...){ ok=false; break; }
                }
            }
        }
        T.idxDirty=true;
        next[T.name]=std::move(T);
    }

    if(fp) fclose(fp);
    if(!ok) return false;
    {
        std::lock_guard<std::mutex> lk(gMu);
        gDB.swap(next);
        gCache.inv("*");
    }
    return true;
}

static void checkpointIfNeeded(){
    if(gWALRecords.load(std::memory_order_relaxed) < (uint64_t)CHECKPOINT_WAL_RECORDS) return;
    {
        std::lock_guard<std::mutex> lk(gMu);
        if(!saveSnapshotLocked()) return;
    }
    /* truncate WAL after a successful snapshot */
    {
        std::lock_guard<std::mutex> lk(gWAL.wmu);
        /* best-effort: prevent the writer thread from racing with truncation */
        gWAL.flush();
        if(gWAL.fp){
            FILE *nf = freopen(WAL_FILE, "wb", gWAL.fp);
            if(!nf){
                /* if truncation fails, keep WAL as-is */
            } else {
                gWAL.fp = nf;
            }
        }
        gWAL.used[0]=gWAL.used[1]=0;
    }
    gWALRecords.store(0, std::memory_order_relaxed);
}

/* ── Helpers ── */
static std::string toUpper(std::string s){
    for(char &c:s)c=(char)toupper((unsigned char)c);return s;}

static bool cmpVals(const char *a,uint32_t al,const char *op,const char *b,uint32_t bl){
    char ba[72],bb[72]; bool num=false; double da=0,db=0;
    if(al<70&&bl<70){
        memcpy(ba,a,al);ba[al]=0;memcpy(bb,b,bl);bb[bl]=0;
        char *ea,*eb; da=strtod(ba,&ea);db=strtod(bb,&eb);
        num=(ea!=ba&&eb!=bb);
    }
    switch(op[0]){
        case'=':return num?(da==db):(al==bl&&memcmp(a,b,al)==0);
        case'!':return num?(da!=db):!(al==bl&&memcmp(a,b,al)==0);
        case'<':return op[1]=='='?(num?da<=db:std::string(a,al)<=std::string(b,bl))
                                 :(num?da< db:std::string(a,al)< std::string(b,bl));
        case'>':return op[1]=='='?(num?da>=db:std::string(a,al)>=std::string(b,bl))
                                 :(num?da> db:std::string(a,al)> std::string(b,bl));
    }
    return false;
}

/* ── Build a ROW line per the reference protocol ──
   Format: "ROW N name1_len:name1value1_len:value1...\n"
*/
static void appendRowLine(std::string &out,
                           const Table &T, size_t row,
                           const std::vector<int> &cols)
{
    out+="ROW ";
    out+=std::to_string((int)cols.size());
    out+=' ';
    for(int c:cols){
        const char *name=T.cols[c].name.c_str();
        size_t nlen=T.cols[c].name.size();
        const char *val=T.cp(row,c);
        uint32_t vlen=T.cl(row,c);
        out+=std::to_string(nlen); out+=':'; out.append(name,nlen);
        out+=std::to_string(vlen); out+=':'; out.append(val,vlen);
    }
    out+='\n';
}

static void appendRowLineJoin(std::string &out,
                               const Table &T1,size_t r1,
                               const Table &T2,size_t r2,
                               const std::vector<std::pair<int,int>> &cols)
{
    out+="ROW ";
    out+=std::to_string((int)cols.size());
    out+=' ';
    for(auto &[tbl,c]:cols){
        const Table &T=tbl?T2:T1;
        size_t row=tbl?r2:r1;
        const char *name=T.cols[c].name.c_str();
        size_t nlen=T.cols[c].name.size();
        const char *val=T.cp(row,c);
        uint32_t vlen=T.cl(row,c);
        out+=std::to_string(nlen); out+=':'; out.append(name,nlen);
        out+=std::to_string(vlen); out+=':'; out.append(val,vlen);
    }
    out+='\n';
}

/* ── Tokeniser ── */
enum class TT{ID,NUM,STR,CM,LP,RP,ST,SC,EQ,NE,LT,LE,GT,GE,DOT,END_};
struct Tok{TT t;std::string v;};

static std::vector<Tok> lex(const char *s,size_t n){
    std::vector<Tok> r; size_t i=0;
    while(i<n){
        while(i<n&&(unsigned char)s[i]<=' ')++i;
        if(i>=n)break; char c=s[i];
        if(c=='\''||c=='\"'){
            char q=c;
            ++i;std::string v;
            while(i<n&&s[i]!=q){
                if(s[i]=='\\'&&i+1<n){++i;v+=s[i++];}
                else v+=s[i++];
            }
            if(i<n)++i; r.push_back({TT::STR,std::move(v)});
        } else if(c=='-'&&i+1<n&&isdigit((unsigned char)s[i+1])){
            std::string v;v+=s[i++];
            while(i<n&&(isdigit((unsigned char)s[i])||s[i]=='.'))v+=s[i++];
            r.push_back({TT::NUM,std::move(v)});
        } else if(isdigit((unsigned char)c)){
            std::string v;
            while(i<n&&(isdigit((unsigned char)s[i])||s[i]=='.'))v+=s[i++];
            r.push_back({TT::NUM,std::move(v)});
        } else if(isalpha((unsigned char)c)||c=='_'){
            std::string v;
            while(i<n&&(isalnum((unsigned char)s[i])||s[i]=='_'))v+=s[i++];
            r.push_back({TT::ID,toUpper(std::move(v))});
        } else {
            switch(c){
            case',':r.push_back({TT::CM,","});break;
            case'(':r.push_back({TT::LP,"("});break;
            case')':r.push_back({TT::RP,")"});break;
            case'*':r.push_back({TT::ST,"*"});break;
            case';':r.push_back({TT::SC,";"});break;
            case'.':r.push_back({TT::DOT,"."});break;
            case'=':r.push_back({TT::EQ,"="});break;
            case'!':if(i+1<n&&s[i+1]=='='){r.push_back({TT::NE,"!="});++i;}break;
            case'<':if(i+1<n&&s[i+1]=='='){r.push_back({TT::LE,"<="});++i;}
                    else r.push_back({TT::LT,"<"});break;
            case'>':if(i+1<n&&s[i+1]=='='){r.push_back({TT::GE,">="});++i;}
                    else r.push_back({TT::GT,">"});break;
            default:break;}
            ++i;
        }
    }
    r.push_back({TT::END_,""});return r;
}

struct Parser{
    const std::vector<Tok>&t; size_t i=0;
    const Tok&cur()const{return t[i];}
    Tok  con(){return t[i++];}
    bool is(TT x)const{return cur().t==x;}
    bool kw(const char*v)const{return cur().t==TT::ID&&cur().v==v;}
    Tok  ex(TT x){if(!is(x))throw std::runtime_error("Expected token, got '"+cur().v+"'");return con();}
    Tok  exId(){if(!is(TT::ID))throw std::runtime_error("Expected id, got '"+cur().v+"'");return con();}
    std::pair<std::string,std::string> colRef(){
        std::string a=exId().v;
        if(is(TT::DOT)){con();return{a,exId().v};}
        return{"",a};}
    const char *opStr(){
        switch(cur().t){
        case TT::EQ:return"=";case TT::NE:return"!=";
        case TT::LT:return"<";case TT::LE:return"<=";
        case TT::GT:return">";case TT::GE:return">=";
        default:throw std::runtime_error("Expected operator, got '"+cur().v+"'");}
    }
};

/* ── WHERE clause ── */
struct WC{bool on=false;std::string tbl,col,op,val;};
static WC parseWhere(Parser &p){
    WC w; if(!p.kw("WHERE"))return w;
    p.con(); w.on=true;
    auto [t,c]=p.colRef(); w.tbl=t; w.col=c;
    w.op=p.opStr(); p.con(); w.val=p.con().v;
    return w;
}
struct OB{bool on=false;std::string tbl,col;bool asc=true;};
static OB parseOrder(Parser &p){
    OB o; if(!p.kw("ORDER"))return o;
    p.con();if(p.kw("BY"))p.con();o.on=true;
    auto [t,c]=p.colRef();o.tbl=t;o.col=c;
    if(p.kw("DESC")){p.con();o.asc=false;}else if(p.kw("ASC"))p.con();
    return o;
}

/* ── Fast INSERT scanner ── */
static int countCols(const char *p,const char *end){
    while(p<end&&*p!='(')++p; if(p>=end)return 1;
    ++p; int n=1,d=0; bool s=false;
    char q=0;
    while(p<end){char c=*p++;
        if(s){
            if(c=='\\')++p;
            else if(c==q){s=false; q=0;}
        }
        else if(c=='\''||c=='\"'){s=true; q=c;}
        else if(c=='(')++d; else if(c==')'){if(!d)break;--d;}
        else if(c==','&&!d)++n;} return n;
}

static void validateInsertInto(const char *p,const char *end,int nc){
    while(p<end){
        while(p<end&&*p!='(')++p;
        if(p>=end)break;
        const char *tup=p;
        int got=countCols(tup,end);
        /* reject empty tuple and wrong column count */
        if(got!=nc) throw std::runtime_error("INSERT value count mismatch");
        /* advance past tuple ')' */
        while(p<end&&*p!=')'){
            if(*p=='\''||*p=='\"'){
                char q=*p++; while(p<end&&*p!=q){ if(*p=='\\'&&p+1<end) p+=2; else ++p; }
                if(p<end) ++p;
            } else {
                ++p;
            }
        }
        if(p>=end||*p!=')') throw std::runtime_error("INSERT value count mismatch");
        ++p;
        while(p<end&&(*p==','||(unsigned char)*p<=' '))++p;
    }
}
static size_t parseInsertInto(const char *p,const char *end,int nc,
                               Arena &arena,CellStore &cells){
    size_t rows=0;
    while(p<end){
        while(p<end&&*p!='(')++p;
        if(p>=end)break;
        ++p;
        for(int i=0;i<nc;i++){
            while(p<end&&(unsigned char)*p<=' ')++p;
            if(p>=end||*p==')') throw std::runtime_error("INSERT value count mismatch");
            const char *s=p; uint32_t l=0;
            if(*p=='\'' || *p=='\"'){
                char q=*p;
                ++p; s=p;
                while(p<end && *p!=q){
                    if(*p=='\\') ++p;
                    if(p<end) ++p;
                }
                l=(uint32_t)(p-s);
                if(p<end) ++p;
            } else if((*p=='N'||*p=='n')&&p+4<=end&&
                      toupper(p[1])=='U'&&toupper(p[2])=='L'&&toupper(p[3])=='L'){
                s=""; l=0; p+=4;
            } else {
                while(p<end&&*p!=','&&*p!=')'&&*p!=';')++p;
                l=(uint32_t)(p-s);
                while(l>0&&(unsigned char)s[l-1]<=' ')--l;
            }
            cells.push(arena.append(s,l),l);
            while(p<end&&(unsigned char)*p<=' ')++p;
            if(i<nc-1){
                if(p>=end||*p!=',') throw std::runtime_error("INSERT value count mismatch");
                ++p;
            } else {
                if(p>=end||*p!=')') throw std::runtime_error("INSERT value count mismatch");
            }
        }
        if(p<end&&*p==')')++p; ++rows;
        while(p<end&&(*p==','||(unsigned char)*p<=' '))++p;
    }
    return rows;
}

/* ── CREATE TABLE ── */
static void doCreate(Parser &p){
    bool ine=false;
    if(p.kw("IF")){p.con();if(p.kw("NOT"))p.con();if(p.kw("EXISTS"))p.con();ine=true;}
    std::string nm=p.exId().v;
    if(ine&&gDB.count(nm)){while(!p.is(TT::END_))p.con();return;}
    bool replaceExisting=false;
    if(gDB.count(nm)) replaceExisting=true;
    if(!p.is(TT::LP)) throw std::runtime_error("CREATE TABLE requires column list: CREATE TABLE name(col TYPE, ...);");
    p.ex(TT::LP);
    Table tbl;tbl.name=nm;int pk=-1;
    while(!p.is(TT::RP)&&!p.is(TT::END_)){
        ColDef col; col.name=p.exId().v;
        p.exId(); /* type */
        if(p.is(TT::LP)){p.con();if(p.is(TT::NUM))p.con();p.ex(TT::RP);}
        while(!p.is(TT::CM)&&!p.is(TT::RP)&&!p.is(TT::END_)){
            if(p.kw("PRIMARY")){p.con();if(p.kw("KEY"))p.con();col.pk=true;pk=(int)tbl.cols.size();}
            else if(p.kw("NOT")){p.con();if(p.kw("NULL"))p.con();}
            else if(p.kw("NULL")||p.kw("UNIQUE"))p.con();
            else if(p.kw("DEFAULT")){p.con();p.con();}
            else break;
        }
        tbl.cols.push_back(col); if(p.is(TT::CM))p.con();
    }
    p.ex(TT::RP);
    tbl.pkCol=(pk>=0)?pk:0; tbl.nc=(int)tbl.cols.size();
    if(replaceExisting){
        const Table &old = gDB.at(nm);
        bool same = (old.nc==tbl.nc) && (old.pkCol==tbl.pkCol) && (old.cols.size()==tbl.cols.size());
        if(same){
            for(size_t i=0;i<tbl.cols.size();i++){
                if(old.cols[i].name!=tbl.cols[i].name || old.cols[i].pk!=tbl.cols[i].pk){ same=false; break; }
            }
        }
        if(!same) throw std::runtime_error("Table already exists with different schema: "+nm);
        gDB.erase(nm);
        gCache.inv(nm);
    }
    gDB[nm]=std::move(tbl);
}

/* ── INSERT (fast path) ── */
static std::string doInsert(const char *sql,size_t sqlLen,bool walWrite){
    const char *p=sql,*end=sql+sqlLen;
    /* skip INSERT INTO */
    while(p<end&&(unsigned char)*p>' ')++p;
    while(p<end&&(unsigned char)*p<=' ')++p;
    while(p<end&&(unsigned char)*p>' ')++p;
    while(p<end&&(unsigned char)*p<=' ')++p;
    /* table name */
    const char *ns=p;
    while(p<end&&(isalnum((unsigned char)*p)||*p=='_'))++p;
    std::string nm(ns,p); for(char &c:nm)c=(char)toupper((unsigned char)c);
    /* skip optional column list */
    while(p<end&&(unsigned char)*p<=' ')++p;
    if(p<end&&*p=='('){
        const char *q=p+1;
        while(q<end&&(unsigned char)*q<=' ')++q;
        if(q<end&&(isalpha((unsigned char)*q)||*q=='_')){
            ++p;while(p<end&&*p!=')')++p;if(p<end)++p;
        }
    }
    /* skip VALUES keyword */
    while(p<end&&(unsigned char)*p<=' ')++p;
    while(p<end&&(unsigned char)*p>' '&&*p!='(')++p;

    int nc=countCols(p,end);
    /* Validate outside the global DB lock to reduce contention under batch inserts. */
    validateInsertInto(p,end,nc);

    std::lock_guard<std::mutex> lk(gMu);
    auto it=gDB.find(nm);
    if(it==gDB.end())throw std::runtime_error("No such table: "+nm);
    Table &tbl=it->second;
    if(tbl.nc!=nc) throw std::runtime_error("INSERT value count mismatch");
    gCache.inv(nm);
    parseInsertInto(p,end,tbl.nc,tbl.arena,tbl.cells);
    tbl.idxDirty=true;
    if(walWrite){ gWAL.append(sql,sqlLen); gWALRecords.fetch_add(1, std::memory_order_relaxed); }
    return "OK\nEND\n";
}

/* ── DELETE ── */
static void doDelete(Parser &p,const std::string &sql,bool walWrite){
    p.exId(); /* FROM */
    std::string nm=p.exId().v;
    auto it=gDB.find(nm);
    if(it==gDB.end())throw std::runtime_error("No such table: "+nm);
    Table &tbl=it->second;
    if(walWrite){ gWAL.append(sql.c_str(),sql.size()); gWALRecords.fetch_add(1, std::memory_order_relaxed); }
    gCache.inv(nm);
    WC w=parseWhere(p);
    if(!w.on){tbl.clear();return;}
    int wci=tbl.colIdx(w.col);
    if(wci<0)throw std::runtime_error("Unknown column: "+w.col);
    Arena na; CellStore nc2;
    for(size_t r=0;r<tbl.nrows();r++){
        if(cmpVals(tbl.cp(r,wci),tbl.cl(r,wci),w.op.c_str(),
                   w.val.c_str(),(uint32_t)w.val.size())) continue;
        for(int c=0;c<tbl.nc;c++) nc2.push(na.append(tbl.cp(r,c),tbl.cl(r,c)),tbl.cl(r,c));
    }
    tbl.arena=std::move(na); tbl.cells=std::move(nc2);
    tbl.pkIdx.clear(); tbl.idxDirty=false;
}

/* ── SELECT ── */
static std::string doSelect(Parser &p, const std::string &sql){
    std::string cached;
    if(gCache.get(sql,cached)) return cached;

    bool star=false;
    std::vector<std::pair<std::string,std::string>> sc;
    if(p.is(TT::ST)){p.con();star=true;}
    else{do{sc.push_back(p.colRef());if(p.is(TT::CM))p.con();else break;}while(true);}

    p.exId(); /* FROM */
    std::string t1=p.exId().v;
    auto it1=gDB.find(t1);
    if(it1==gDB.end())throw std::runtime_error("No such table: "+t1);
    Table &T1=it1->second;

    bool hasJ=false; std::string jc1,jc2,jo,t2nm; Table *T2=nullptr;
    if(p.kw("JOIN")||p.kw("INNER")||p.kw("LEFT")||p.kw("RIGHT")||p.kw("CROSS")){
        if(!p.kw("JOIN")) p.con();
        if(p.kw("JOIN")) p.con();
        t2nm=p.exId().v;
        auto it2=gDB.find(t2nm);
        if(it2==gDB.end())throw std::runtime_error("No such table: "+t2nm);
        T2=&it2->second; hasJ=true;
        if(p.kw("ON")){
            p.con();
            auto[a,ac]=p.colRef();
            jo=p.opStr();
            p.con();
            auto[b,bc]=p.colRef();
            jc1=ac; jc2=bc;
        }
    }

    WC w=parseWhere(p);
    OB o=parseOrder(p);

    std::string res; res.reserve(64*1024);

    if(!hasJ){
        int wci=-1;
        if(w.on){wci=T1.colIdx(w.col);if(wci<0)throw std::runtime_error("Unknown column: "+w.col);}

        /* resolve output columns */
        std::vector<int> oc;
        if(star){for(int i=0;i<T1.nc;i++)oc.push_back(i);}
        else{
            for(auto &[qt,qc]:sc){
                int ci=T1.colIdx(qc);
                if(ci<0)throw std::runtime_error("Unknown column: "+qc);
                oc.push_back(ci);
            }
        }

        /* collect matching rows */
        std::vector<size_t> matched;
        bool usePK = w.on && w.op=="=" && wci==T1.pkCol && !o.on;
        if(usePK){
            if(T1.idxDirty)T1.buildIndex();
            auto hit=T1.pkIdx.find(w.val);
            if(hit!=T1.pkIdx.end())
                for(size_t r:hit->second) matched.push_back(r);
        } else {
            matched.reserve(T1.nrows());
            for(size_t r=0;r<T1.nrows();r++){
                if(!w.on||cmpVals(T1.cp(r,wci),T1.cl(r,wci),
                                   w.op.c_str(),w.val.c_str(),(uint32_t)w.val.size()))
                    matched.push_back(r);
            }
        }

        if(o.on){
            int oci=T1.colIdx(o.col);
            if(oci<0)throw std::runtime_error("Unknown ORDER BY column: "+o.col);
            bool asc=o.asc;
            std::stable_sort(matched.begin(),matched.end(),[&](size_t a,size_t b){
                const char *av=T1.cp(a,oci);uint32_t al=T1.cl(a,oci);
                const char *bv=T1.cp(b,oci);uint32_t bl=T1.cl(b,oci);
                char ba[72],bb[72];bool num=false;double da=0,db=0;
                if(al<70&&bl<70){memcpy(ba,av,al);ba[al]=0;memcpy(bb,bv,bl);bb[bl]=0;
                    char *ea,*eb;da=strtod(ba,&ea);db=strtod(bb,&eb);num=(ea!=ba&&eb!=bb);}
                bool less=num?(da<db):(al==bl?memcmp(av,bv,al)<0:
                               std::string(av,al)<std::string(bv,bl));
                return asc?less:!less;});
        }

        for(size_t ri:matched) appendRowLine(res,T1,ri,oc);
    }
    else {
        /* JOIN */
        int j1=T1.colIdx(jc1), j2=T2->colIdx(jc2);
        if(j1<0)throw std::runtime_error("Unknown join column: "+jc1);
        if(j2<0)throw std::runtime_error("Unknown join column: "+jc2);

        int wci1=-1,wci2=-1; bool wonT1=true;
        if(w.on){
            if(!w.tbl.empty()){
                wonT1=(w.tbl==t1);
                int ci=wonT1?T1.colIdx(w.col):T2->colIdx(w.col);
                if(ci<0)throw std::runtime_error("Unknown column: "+w.col);
                if(wonT1)wci1=ci; else wci2=ci;
            } else {
                wci1=T1.colIdx(w.col); wci2=T2->colIdx(w.col);
                if(wci1>=0)wonT1=true; else if(wci2>=0)wonT1=false;
                else throw std::runtime_error("Unknown column: "+w.col);
            }
        }

        /* output columns: pair<table_idx, col_idx> (0=T1,1=T2) */
        std::vector<std::pair<int,int>> oc;
        if(star){
            for(int i=0;i<T1.nc;i++)oc.push_back({0,i});
            for(int i=0;i<T2->nc;i++)oc.push_back({1,i});
        } else {
            for(auto &[qt,qc]:sc){
                if(!qt.empty()){
                    bool o1=(qt==t1);
                    int ci=o1?T1.colIdx(qc):T2->colIdx(qc);
                    if(ci<0)throw std::runtime_error("Unknown column: "+qc);
                    oc.push_back({o1?0:1,ci});
                } else {
                    int c1=T1.colIdx(qc),c2=T2->colIdx(qc);
                    if(c1>=0)oc.push_back({0,c1});
                    else if(c2>=0)oc.push_back({1,c2});
                    else throw std::runtime_error("Unknown column: "+qc);
                }
            }
        }

        std::vector<std::pair<size_t,size_t>> joined;
        joined.reserve(T1.nrows());
        if(jo.empty() || jo=="="){
            /* fast path: hash join */
            std::unordered_map<std::string,std::vector<size_t>> h2;
            h2.reserve(T2->nrows()*2);
            for(size_t r2=0;r2<T2->nrows();r2++) h2[T2->cs(r2,j2)].push_back(r2);

            for(size_t r1=0;r1<T1.nrows();r1++){
                auto hit=h2.find(T1.cs(r1,j1));
                if(hit==h2.end())continue;
                for(size_t r2:hit->second){
                    if(w.on){
                        int wci=wonT1?wci1:wci2;
                        const char *wv=wonT1?T1.cp(r1,wci):T2->cp(r2,wci);
                        uint32_t wl=wonT1?T1.cl(r1,wci):T2->cl(r2,wci);
                        if(!cmpVals(wv,wl,w.op.c_str(),w.val.c_str(),(uint32_t)w.val.size())) continue;
                    }
                    joined.push_back({r1,r2});
                }
            }
        } else {
            /* general (slower) path: nested-loop join for non-equality operators */
            for(size_t r1=0;r1<T1.nrows();r1++){
                const char *lv=T1.cp(r1,j1);
                uint32_t ll=T1.cl(r1,j1);
                for(size_t r2=0;r2<T2->nrows();r2++){
                    const char *rv=T2->cp(r2,j2);
                    uint32_t rl=T2->cl(r2,j2);
                    if(!cmpVals(lv,ll,jo.c_str(),rv,rl)) continue;
                    if(w.on){
                        int wci=wonT1?wci1:wci2;
                        const char *wv=wonT1?T1.cp(r1,wci):T2->cp(r2,wci);
                        uint32_t wl=wonT1?T1.cl(r1,wci):T2->cl(r2,wci);
                        if(!cmpVals(wv,wl,w.op.c_str(),w.val.c_str(),(uint32_t)w.val.size())) continue;
                    }
                    joined.push_back({r1,r2});
                }
            }
        }

        if(o.on){
            int o1=T1.colIdx(o.col),o2=T2->colIdx(o.col);bool asc=o.asc;
            std::stable_sort(joined.begin(),joined.end(),[&](auto &a,auto &b){
                const char *av,*bv;uint32_t al,bl2;
                if(o1>=0){av=T1.cp(a.first,o1);al=T1.cl(a.first,o1);
                           bv=T1.cp(b.first,o1);bl2=T1.cl(b.first,o1);}
                else{av=T2->cp(a.second,o2);al=T2->cl(a.second,o2);
                     bv=T2->cp(b.second,o2);bl2=T2->cl(b.second,o2);}
                bool less=(al==bl2?memcmp(av,bv,al)<0:std::string(av,al)<std::string(bv,bl2));
                return asc?less:!less;});
        }

        for(auto &[r1,r2]:joined) appendRowLineJoin(res,T1,r1,*T2,r2,oc);
    }

    gCache.put(sql,res);
    return res;
}

/* ── Main executor ── */
static std::string execSQL(const char *raw,size_t rawLen,bool walWrite=true){
    while(rawLen>0&&((unsigned char)raw[rawLen-1]<=' '||raw[rawLen-1]==';'))--rawLen;
    size_t skip=0;
    while(skip<rawLen&&(unsigned char)raw[skip]<=' ')++skip;
    raw+=skip; rawLen-=skip;
    if(rawLen==0)return"OK\nEND\n";

    /* fast INSERT path */
    if(rawLen>=6){
        unsigned char c0=toupper((unsigned char)raw[0]);
        unsigned char c1=toupper((unsigned char)raw[1]);
        if(c0=='I'&&c1=='N'){
            try{ return doInsert(raw,rawLen,walWrite); }
            catch(const std::exception &e){
                return std::string("ERROR:")+e.what()+"\nEND\n";}
        }
    }

    try{
        auto toks=lex(raw,rawLen); Parser p{toks};
        std::string kw=p.exId().v, rows;
        std::lock_guard<std::mutex> lk(gMu);
        if(kw=="CREATE"){
            std::string sub=p.exId().v;
            if(sub=="TABLE") doCreate(p);
            else throw std::runtime_error("Unknown CREATE: "+sub);
            if(walWrite){ gWAL.append(raw,rawLen); gWALRecords.fetch_add(1, std::memory_order_relaxed); }
        }
        else if(kw=="DELETE"){
            doDelete(p,std::string(raw,rawLen),walWrite);
        }
        else if(kw=="SELECT"){
            rows=doSelect(p,std::string(raw,rawLen));
        }
        else if(kw=="DROP"){
            if(p.kw("TABLE"))p.con();
            bool ine2=false;
            if(p.kw("IF")){p.con();if(p.kw("EXISTS"))p.con();ine2=true;}
            std::string nm=p.exId().v;
            if(!ine2&&!gDB.count(nm))throw std::runtime_error("No such table: "+nm);
            gDB.erase(nm); gCache.inv(nm);
            if(walWrite){ gWAL.append(raw,rawLen); gWALRecords.fetch_add(1, std::memory_order_relaxed); }
        }
        else{ throw std::runtime_error("Unknown command: "+kw); }
        return rows+"OK\nEND\n";
    }
    catch(const std::exception &e){
        return std::string("ERROR:")+e.what()+"\nEND\n";}
}

static void replayWAL(){
    FILE *r=fopen(WAL_FILE,"rb");
    if(!r) return;
    std::cout<<"[WAL] replaying...\n";
    size_t recs=0;
    for(;;){
        uint32_t len=0;
        if(fread(&len,1,4,r)!=4) break;
        std::string sql(len,'\0');
        if(len>0 && fread(&sql[0],1,len,r)!=len) break;
        fgetc(r);
        ++recs;
        try{ execSQL(sql.c_str(),sql.size(),false); }catch(...){ }
        if((recs%200000)==0) std::cout<<"[WAL] replayed "<<recs<<" records...\n";
    }
    fclose(r);
    gWALRecords.store((uint64_t)recs, std::memory_order_relaxed);
    std::cout<<"[WAL] replayed "<<recs<<" records\n";
    std::cout<<"[WAL] done\n";
}

/* ── Network ── */
static void tuneSock(int fd,bool isTCP){
    if(isTCP){int y=1;setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&y,sizeof y);}
    int sz=SBUF;
    setsockopt(fd,SOL_SOCKET,SO_RCVBUF,&sz,sizeof sz);
    setsockopt(fd,SOL_SOCKET,SO_SNDBUF,&sz,sizeof sz);
}

static void handleClient(int cfd,bool isTCP){
    tuneSock(cfd,isTCP);
    std::string pend; pend.reserve(512*1024);
    std::string resp; resp.reserve(512*1024);
    char buf[262144];
    while(true){
        ssize_t r=recv(cfd,buf,sizeof buf,0); if(r<=0)break;
        pend.append(buf,(size_t)r);
        resp.clear();
        size_t pos=0;
        while(true){
            size_t semi=pend.find(';',pos); if(semi==std::string::npos)break;
            resp+=execSQL(pend.c_str()+pos,semi-pos+1); pos=semi+1;
        }
        pend.erase(0,pos);
        if(!resp.empty()){
            size_t sent=0;
            while(sent<resp.size()){
                ssize_t w=send(cfd,resp.c_str()+sent,resp.size()-sent,MSG_NOSIGNAL);
                if(w<=0)goto done; sent+=(size_t)w;}
        }
    }
done: close(cfd);
}

static void acceptLoop(int lfd,bool isTCP){
    while(true){
        struct sockaddr_storage ca{}; socklen_t cl=sizeof ca;
        int cfd=accept(lfd,(sockaddr*)&ca,&cl);
        if(cfd<0){if(errno==EINTR)continue;continue;}
        std::thread([cfd,isTCP]{handleClient(cfd,isTCP);}).detach();
    }
}

int main(){
    signal(SIGPIPE,SIG_IGN);
    mkdir(DATA_DIR,0755);
    if(!gWAL.open(WAL_FILE)){std::cerr<<"Cannot open WAL\n";return 1;}
    if(loadSnapshot()){
        std::cout<<"[SNAPSHOT] loaded "<<SNAP_FILE<<"\n";
    }
    replayWAL();

    std::thread([](){
        int ticks=0;
        while(true){
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            gWAL.flush();
            if(++ticks>=10){
                ticks=0;
                checkpointIfNeeded();
            }
        }
    }).detach();

    int tcpFd=socket(AF_INET,SOCK_STREAM,0);
    {int o=1;setsockopt(tcpFd,SOL_SOCKET,SO_REUSEADDR,&o,sizeof o);}
    tuneSock(tcpFd,true);
    struct sockaddr_in addr{};
    addr.sin_family=AF_INET; addr.sin_addr.s_addr=INADDR_ANY; addr.sin_port=htons(TCP_PORT);
    if(bind(tcpFd,(sockaddr*)&addr,sizeof addr)<0){perror("bind"); close(tcpFd); gWAL.close(); return 1;}
    listen(tcpFd,BACKLOG);

    unlink(UNIX_SOCK);
    int unixFd=socket(AF_UNIX,SOCK_STREAM,0);
    {struct sockaddr_un ua{};ua.sun_family=AF_UNIX;
     strncpy(ua.sun_path,UNIX_SOCK,sizeof ua.sun_path-1);
     if(bind(unixFd,(sockaddr*)&ua,sizeof ua)<0){perror("unix bind"); close(unixFd); unixFd=-1;}
     else{listen(unixFd,BACKLOG);tuneSock(unixFd,false);}}

    std::cout<<"FlexQL server on TCP:"<<TCP_PORT<<"\n";
    if(unixFd>=0)std::cout<<"Fast path: "<<UNIX_SOCK<<"\n";

    if(unixFd>=0)
        std::thread([unixFd]{acceptLoop(unixFd,false);}).detach();
    acceptLoop(tcpFd,true);
    gWAL.close(); return 0;
}
