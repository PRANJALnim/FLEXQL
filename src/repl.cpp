/*
 * FlexQL Interactive REPL Client
 *
 * Usage:
 *   ./flexql-client [host] [port]
 *   ./flexql-client              # connects to 127.0.0.1:9000
 *   ./flexql-client 192.168.1.5 9000
 *
 * Commands:
 *   Any SQL statement ending with ';'
 *   .exit / .quit  — disconnect and exit
 *   .help          — show help
 *   .tables        — not supported (send: SELECT ...)
 */

#include "flexql.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <chrono>

/* ANSI color codes */
#define C_RESET  "\033[0m"
#define C_BOLD   "\033[1m"
#define C_GREEN  "\033[32m"
#define C_CYAN   "\033[36m"
#define C_RED    "\033[31m"
#define C_YELLOW "\033[33m"
#define C_GREY   "\033[90m"

struct RowData {
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string>              colNames;
    bool                                  firstRow{true};
};

static int rowCallback(void *data, int argc, char **argv, char **azColName){
    RowData *rd = static_cast<RowData*>(data);
    if(rd->firstRow){
        rd->firstRow = false;
        for(int i=0;i<argc;i++) rd->colNames.push_back(azColName[i] ? azColName[i] : "");
    }
    std::vector<std::string> row;
    for(int i=0;i<argc;i++) row.push_back(argv[i] ? argv[i] : "NULL");
    rd->rows.push_back(std::move(row));
    return 0;
}

static void printTable(const RowData &rd){
    if(rd.rows.empty()){
        printf(C_GREY "(no rows)\n" C_RESET);
        return;
    }

    int ncols = (int)rd.colNames.size();
    std::vector<size_t> widths(ncols, 0);

    /* column header widths */
    for(int c=0;c<ncols;c++)
        widths[c] = rd.colNames[c].size();

    /* data widths */
    for(auto &row:rd.rows)
        for(int c=0;c<(int)row.size();c++)
            if(c<ncols) widths[c]=std::max(widths[c],row[c].size());

    /* top border */
    printf(C_GREY "+");
    for(int c=0;c<ncols;c++){
        for(size_t i=0;i<widths[c]+2;i++) printf("-");
        printf("+");
    }
    printf(C_RESET "\n");

    /* header */
    printf(C_GREY "|" C_RESET);
    for(int c=0;c<ncols;c++){
        printf(C_BOLD C_CYAN " %-*s " C_RESET,
               (int)widths[c], rd.colNames[c].c_str());
        printf(C_GREY "|" C_RESET);
    }
    printf("\n");

    /* separator */
    printf(C_GREY "+");
    for(int c=0;c<ncols;c++){
        for(size_t i=0;i<widths[c]+2;i++) printf("-");
        printf("+");
    }
    printf(C_RESET "\n");

    /* rows */
    for(auto &row:rd.rows){
        printf(C_GREY "|" C_RESET);
        for(int c=0;c<ncols;c++){
            const char *val = (c<(int)row.size()) ? row[c].c_str() : "";
            printf(" %-*s ", (int)widths[c], val);
            printf(C_GREY "|" C_RESET);
        }
        printf("\n");
    }

    /* bottom border */
    printf(C_GREY "+");
    for(int c=0;c<ncols;c++){
        for(size_t i=0;i<widths[c]+2;i++) printf("-");
        printf("+");
    }
    printf(C_RESET "\n");
    printf(C_GREY "%zu row(s)\n" C_RESET, rd.rows.size());
}

static void printHelp(){
    printf(C_BOLD "FlexQL Interactive Client\n" C_RESET);
    printf("\n");
    printf(C_CYAN "SQL Commands (end with ';'):\n" C_RESET);
    printf("  CREATE TABLE name(col TYPE, ...);\n");
    printf("  INSERT INTO name VALUES (...);\n");
    printf("  SELECT [cols|*] FROM name [WHERE col op val];\n");
    printf("  SELECT ... FROM t1 INNER JOIN t2 ON t1.col = t2.col;\n");
    printf("  DELETE FROM name [WHERE col op val];\n");
    printf("  DROP TABLE name;\n");
    printf("\n");
    printf(C_CYAN "REPL Commands:\n" C_RESET);
    printf("  .help    — show this help\n");
    printf("  .exit    — exit the client\n");
    printf("  .quit    — exit the client\n");
    printf("\n");
    printf(C_CYAN "Supported WHERE operators:\n" C_RESET);
    printf("  =   !=   <   <=   >   >=\n");
    printf("\n");
}

static bool isBlank(const std::string &s){
    for(char c:s) if(!isspace((unsigned char)c)) return false;
    return true;
}

static void ltrim(std::string &s){
    size_t i=0;
    while(i<s.size() && isspace((unsigned char)s[i])) ++i;
    if(i) s.erase(0,i);
}

static void rtrim(std::string &s){
    while(!s.empty() && isspace((unsigned char)s.back())) s.pop_back();
}

static bool isDashCommentLine(const std::string &line){
    size_t i=0;
    while(i<line.size() && isspace((unsigned char)line[i])) ++i;
    return (i+1<line.size() && line[i]=='-' && line[i+1]=='-');
}

int main(int argc, char **argv){
    const char *host = "127.0.0.1";
    int port = 9000;
    if(argc >= 2) host = argv[1];
    if(argc >= 3) port = atoi(argv[2]);

    printf(C_BOLD "FlexQL Client" C_RESET " — connecting to %s:%d...\n", host, port);

    FlexQL *db = nullptr;
    if(flexql_open(host, port, &db) != FLEXQL_OK){
        fprintf(stderr, C_RED "Error: Cannot connect to FlexQL server at %s:%d\n" C_RESET,
                host, port);
        fprintf(stderr, "Make sure the server is running: ./server\n");
        return 1;
    }
    printf(C_GREEN "Connected." C_RESET " Type SQL statements ending with ';', or .help\n\n");

    std::string buffer;   /* accumulates multi-line input */
    bool multiLine = false;

    while(true){
        /* print prompt */
        if(multiLine)
            printf(C_GREY "   ...> " C_RESET);
        else
            printf(C_BOLD C_GREEN "flexql> " C_RESET);
        fflush(stdout);

        /* read line */
        std::string line;
        if(!std::getline(std::cin, line)){
            /* EOF (Ctrl-D) */
            printf("\n");
            break;
        }

        /* check for REPL commands */
        std::string trimmed = line;
        while(!trimmed.empty() && isspace((unsigned char)trimmed.front())) trimmed.erase(trimmed.begin());
        while(!trimmed.empty() && isspace((unsigned char)trimmed.back()))  trimmed.pop_back();

        if(trimmed == ".exit" || trimmed == ".quit" || trimmed == "exit" || trimmed == "quit"){
            printf("Bye!\n");
            break;
        }
        if(trimmed == ".help" || trimmed == "help"){
            printHelp();
            continue;
        }
        if(isDashCommentLine(line)) continue;
        if(isBlank(line)) continue;

        /* accumulate SQL */
        if(!buffer.empty()) buffer += '\n';
        buffer += line;

        bool ranAny=false;
        while(true){
            size_t semi = buffer.find(';');
            if(semi == std::string::npos) break;

            std::string sql = buffer.substr(0, semi+1);
            buffer.erase(0, semi+1);

            ltrim(sql); rtrim(sql);
            if(isBlank(sql) || sql==";") continue;

            ranAny=true;
            char *errMsg = nullptr;
            RowData rd;
            auto start = std::chrono::steady_clock::now();
            int rc = flexql_exec(db, sql.c_str(), rowCallback, &rd, &errMsg);
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();

            if(rc != FLEXQL_OK){
                printf(C_RED "Error: %s\n" C_RESET, errMsg ? errMsg : "unknown error");
                if(errMsg) flexql_free(errMsg);
            } else {
                if(!rd.rows.empty() || !rd.colNames.empty()){
                    printTable(rd);
                } else {
                    printf(C_GREEN "OK" C_RESET " (%lldms)\n", (long long)elapsed);
                }
            }
        }

        multiLine = !ranAny;
    }

    flexql_close(db);
    return 0;
}
