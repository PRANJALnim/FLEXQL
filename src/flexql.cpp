/*
 * FlexQL client — lock-free pipeline + background drainer.
 * 
 * Pipeline INSERT calls are tracked with atomics only (no mutex).
 * The mutex is only used for sync (SELECT/CREATE/etc) handoffs.
 */
#include "flexql.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#define UNIX_SOCK    "/tmp/flexql.sock"
#define RBUF_SZ      (8*1024*1024)
#define SOCK_BUF     (16*1024*1024)
#define MAX_INFLIGHT 131072  /* 128k max in-flight inserts */

struct FlexQL {
    int fd;
    std::thread drainer;

    char   *rbuf;
    size_t  rbufsz, rfilled;

    /* lock-free pipeline counters */
    std::atomic<long long> sent{0};
    std::atomic<long long> acked{0};

    /* back-pressure: main thread waits here when too many in-flight */
    std::mutex              bpMu;
    std::condition_variable bpCV;

    /* sync call state (mutex protected) */
    std::mutex              syncMu;
    std::condition_variable syncCV;
    std::atomic<bool>       syncActive{false};
    bool                    syncDone{false};
    int                     syncResult{0};
    std::string             syncErr;
    int(*syncCb)(void*,int,char**,char**){nullptr};
    void                   *syncArg{nullptr};

    std::atomic<bool> stop{false};
};

static bool isLocal(const char *h){
    return !strcmp(h,"127.0.0.1")||!strcmp(h,"localhost")||!strcmp(h,"::1");}

static bool parseRow(const char *pay, size_t plen,
                     std::vector<std::string> &vals,
                     std::vector<std::string> &cols){
    vals.clear(); cols.clear();
    const char *p=pay, *end=pay+plen;
    const char *sp=(const char*)memchr(p,' ',(size_t)(end-p));
    if(!sp||sp==p) return false;
    int n=atoi(std::string(p,sp).c_str());
    if(n<0||n>1024) return false;
    p=sp+1;
    for(int i=0;i<n;i++){
        const char *c=(const char*)memchr(p,':',(size_t)(end-p));
        if(!c) return false;
        size_t nl=(size_t)atoll(std::string(p,c).c_str());
        p=c+1; if((size_t)(end-p)<nl) return false;
        cols.emplace_back(p,nl); p+=nl;
        c=(const char*)memchr(p,':',(size_t)(end-p));
        if(!c) return false;
        size_t vl=(size_t)atoll(std::string(p,c).c_str());
        p=c+1; if((size_t)(end-p)<vl) return false;
        vals.emplace_back(p,vl); p+=vl;
    }
    return p==end;
}

static void drainerLoop(FlexQL *db){
    char *buf=db->rbuf;
    size_t &filled=db->rfilled;

    while(true){
        ssize_t r=recv(db->fd, buf+filled, db->rbufsz-filled, 0);
        if(r<=0) break;
        filled+=(size_t)r;

        size_t ls=0;
        while(true){
            char *nl=(char*)memchr(buf+ls,'\n',filled-ls);
            if(!nl) break;
            size_t ll=(size_t)(nl-(buf+ls));
            char *line=buf+ls;
            ls=(size_t)(nl-buf)+1;

            if(ll==3&&line[0]=='E'&&line[1]=='N'&&line[2]=='D'){
                if(db->syncActive.load(std::memory_order_acquire)){
                    /* sync response complete */
                    std::lock_guard<std::mutex> lk(db->syncMu);
                    db->syncDone=true;
                    db->syncCV.notify_one();
                    /* wait until main consumes the sync response */
                    /* (syncActive will be cleared by main thread) */
                } else {
                    /* pipeline response */
                    long long a=db->acked.fetch_add(1,std::memory_order_release)+1;
                    long long s=db->sent.load(std::memory_order_acquire);
                    if(s-a < MAX_INFLIGHT/2){
                        /* wake main if it was waiting for back-pressure */
                        db->bpCV.notify_one();
                    }
                    if(a>=s){
                        /* all caught up - wake close() */
                        db->bpCV.notify_all();
                    }
                }
                continue;
            }
            if(ll==2&&line[0]=='O'&&line[1]=='K') continue;
            if(ll>=6&&line[0]=='E'&&line[1]=='R'&&line[2]=='R'){
                if(db->syncActive.load(std::memory_order_acquire)){
                    std::lock_guard<std::mutex> lk(db->syncMu);
                    db->syncErr=std::string(line+6,ll-6);
                    db->syncResult=-1;
                    db->syncDone=true;
                    db->syncCV.notify_one();
                } else {
                    /* pipeline error - ack it anyway */
                    long long a=db->acked.fetch_add(1,std::memory_order_release)+1;
                    long long s=db->sent.load(std::memory_order_acquire);
                    if(a>=s) db->bpCV.notify_all();
                }
                continue;
            }
            if(ll>=4&&line[0]=='R'&&line[1]=='O'&&line[2]=='W'&&line[3]==' '){
                if(db->syncActive.load(std::memory_order_acquire)&&db->syncCb){
                    std::vector<std::string> vals,cols;
                    if(parseRow(line+4,ll-4,vals,cols)){
                        std::vector<char*> av,ac;
                        for(auto &v:vals) av.push_back((char*)v.c_str());
                        for(auto &c:cols) ac.push_back((char*)c.c_str());
                        db->syncCb(db->syncArg,(int)av.size(),av.data(),ac.data());
                    } else {
                        char save=line[ll]; line[ll]='\0';
                        char *rv=line+4,*rc=(char*)"row";
                        db->syncCb(db->syncArg,1,&rv,&rc); line[ll]=save;
                    }
                }
                continue;
            }
        }

        if(ls>0){memmove(buf,buf+ls,filled-ls);filled-=ls;}
        if(filled==db->rbufsz){
            size_t ns=db->rbufsz*2;
            char *nb=(char*)realloc(buf,ns);
            if(nb){db->rbuf=buf=nb;db->rbufsz=ns;}
        }
    }
    db->bpCV.notify_all();
    db->syncCV.notify_all();
}

extern "C" {

int flexql_open(const char *host,int port,FlexQL **out){
    if(!host||!out) return FLEXQL_ERROR;
    int fd=-1;
    if(isLocal(host)){
        int u=socket(AF_UNIX,SOCK_STREAM,0);
        if(u>=0){struct sockaddr_un ua{};ua.sun_family=AF_UNIX;
            strncpy(ua.sun_path,UNIX_SOCK,sizeof ua.sun_path-1);
            if(connect(u,(sockaddr*)&ua,sizeof ua)==0) fd=u; else close(u);}
    }
    if(fd<0){
        fd=socket(AF_INET,SOCK_STREAM,0); if(fd<0) return FLEXQL_ERROR;
        {int y=1;setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&y,sizeof y);}
        struct sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons((uint16_t)port);
        if(inet_pton(AF_INET,host,&a.sin_addr)<=0){close(fd);return FLEXQL_ERROR;}
        if(connect(fd,(sockaddr*)&a,sizeof a)<0){close(fd);return FLEXQL_ERROR;}
    }
    {int sz=SOCK_BUF;setsockopt(fd,SOL_SOCKET,SO_RCVBUF,&sz,sizeof sz);}
    {int sz=SOCK_BUF;setsockopt(fd,SOL_SOCKET,SO_SNDBUF,&sz,sizeof sz);}
    FlexQL *db=new FlexQL();
    db->fd=fd; db->rbufsz=RBUF_SZ; db->rfilled=0;
    db->rbuf=(char*)malloc(RBUF_SZ);
    if(!db->rbuf){close(fd);delete db;return FLEXQL_ERROR;}
    db->drainer=std::thread(drainerLoop,db);
    *out=db; return FLEXQL_OK;
}

int flexql_close(FlexQL *db){
    if(!db) return FLEXQL_ERROR;
    /* drain all pending pipeline responses */
    {
        std::unique_lock<std::mutex> lk(db->bpMu);
        db->bpCV.wait(lk,[db]{
            return db->acked.load(std::memory_order_acquire)>=
                   db->sent.load(std::memory_order_acquire)
                   || db->stop.load();
        });
    }
    db->stop.store(true);
    shutdown(db->fd,SHUT_RDWR);
    /* ensure drainer isn't stuck in syncCV wait */
    {std::lock_guard<std::mutex> lk(db->syncMu);
     db->syncActive.store(false,std::memory_order_release);
     db->syncDone=true;}
    db->syncCV.notify_all();
    db->bpCV.notify_all();
    if(db->drainer.joinable()) db->drainer.join();
    close(db->fd);
    free(db->rbuf);
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(FlexQL *db,const char *sql,
                int(*cb)(void*,int,char**,char**),void *arg,char **errmsg)
{
    if(!db||!sql){
        if(errmsg){*errmsg=(char*)malloc(9);if(*errmsg)strcpy(*errmsg,"null arg");}
        return FLEXQL_ERROR;
    }
    size_t len=strlen(sql);
    size_t i=0;
    while(i<len&&(unsigned char)sql[i]<=' ')++i;
    bool isIns=(i<len&&toupper((unsigned char)sql[i])=='I');
    bool pipeline=isIns&&!cb;

    if(!pipeline){
        /* wait for ALL pipeline responses to drain first */
        {
            std::unique_lock<std::mutex> lk(db->bpMu);
            db->bpCV.wait(lk,[db]{
                return db->acked.load(std::memory_order_acquire)>=
                       db->sent.load(std::memory_order_acquire)
                       || db->stop.load();
            });
        }
        /* set sync mode BEFORE sending */
        {
            std::lock_guard<std::mutex> lk(db->syncMu);
            db->syncActive.store(true,std::memory_order_release);
            db->syncCb=cb; db->syncArg=arg;
            db->syncDone=false; db->syncResult=0; db->syncErr.clear();
        }
    } else {
        /* back-pressure: slow down if too many in-flight */
        long long inflight=db->sent.load(std::memory_order_relaxed)
                          -db->acked.load(std::memory_order_relaxed);
        if(inflight>=MAX_INFLIGHT){
            std::unique_lock<std::mutex> lk(db->bpMu);
            db->bpCV.wait(lk,[db]{
                return (db->sent.load(std::memory_order_acquire)
                       -db->acked.load(std::memory_order_acquire))<MAX_INFLIGHT/2
                       ||db->stop.load();
            });
        }
    }

    /* send */
    bool hasSemi=false;
    for(int j=(int)len-1;j>=0;j--){
        if(sql[j]==';'){hasSemi=true;break;}
        if(!isspace((unsigned char)sql[j]))break;
    }
    bool ok;
    if(!hasSemi)
        ok=(send(db->fd,sql,len,MSG_NOSIGNAL|MSG_MORE)>0)&&
           (send(db->fd,";",1,MSG_NOSIGNAL)>0);
    else
        ok=(send(db->fd,sql,len,MSG_NOSIGNAL)>0);

    if(!ok){
        if(!pipeline){
            std::lock_guard<std::mutex> lk(db->syncMu);
            db->syncActive.store(false,std::memory_order_release);
            db->syncCV.notify_all();
        }
        if(errmsg){*errmsg=(char*)malloc(12);if(*errmsg)strcpy(*errmsg,"send failed");}
        return FLEXQL_ERROR;
    }

    if(pipeline){
        db->sent.fetch_add(1,std::memory_order_release);
        return FLEXQL_OK;
    }

    /* wait for drainer to signal syncDone */
    int result; std::string err;
    {
        std::unique_lock<std::mutex> lk(db->syncMu);
        db->syncCV.wait(lk,[db]{return db->syncDone||db->stop.load();});
        result=db->syncResult;
        err=std::move(db->syncErr);
        /* clear sync mode so drainer can proceed */
        db->syncActive.store(false,std::memory_order_release);
        db->syncDone=false;
    }

    if(result<0){
        if(errmsg){*errmsg=(char*)malloc(err.size()+1);
                   if(*errmsg)memcpy(*errmsg,err.c_str(),err.size()+1);}
        return FLEXQL_ERROR;
    }
    return FLEXQL_OK;
}

void flexql_free(void *ptr){free(ptr);}

} /* extern "C" */
