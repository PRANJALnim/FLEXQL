#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

static bool exec_no_rows(FlexQL *db, const string &sql) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (rc != FLEXQL_OK) {
        cerr << "ERROR: " << (err ? err : "unknown") << "\n";
        if (err) flexql_free(err);
        return false;
    }
    return true;
}

static bool run_for_batch(FlexQL *db, long long total_rows, int batch_size) {
    if (!exec_no_rows(db, "DROP TABLE IF EXISTS TUNE_USERS;")) return false;
    if (!exec_no_rows(db,
                      "CREATE TABLE TUNE_USERS(ID DECIMAL, NAME VARCHAR(64), AGE DECIMAL);")) {
        return false;
    }

    auto start = steady_clock::now();

    long long inserted = 0;
    while (inserted < total_rows) {
        int in_batch = 0;
        stringstream ss;
        ss << "INSERT INTO TUNE_USERS VALUES ";
        while (in_batch < batch_size && inserted < total_rows) {
            long long id = inserted + 1;
            ss << "(" << id << ", 'u" << id << "', " << (18 + (id % 50)) << ")";
            inserted++;
            in_batch++;
            if (in_batch < batch_size && inserted < total_rows) ss << ",";
        }
        ss << ";";
        if (!exec_no_rows(db, ss.str())) return false;
    }

    auto end = steady_clock::now();
    auto ms = duration_cast<milliseconds>(end - start).count();
    double sec = ms / 1000.0;
    long long rps = (sec > 0) ? (long long)(total_rows / sec) : total_rows;

    cout << "batch=" << batch_size << " rows=" << total_rows << " elapsed_ms=" << ms
         << " throughput_rps=" << rps << "\n";
    return true;
}

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 9000;
    long long total_rows = 200000;

    if (argc >= 2) total_rows = atoll(argv[1]);

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        cerr << "Cannot connect to FlexQL at " << host << ":" << port << "\n";
        return 1;
    }

    vector<int> batches = {10, 25, 50, 100, 200, 500, 1000, 2000};
    for (int b : batches) {
        if (!run_for_batch(db, total_rows, b)) {
            flexql_close(db);
            return 1;
        }
    }

    flexql_close(db);
    return 0;
}
