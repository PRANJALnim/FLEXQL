#!/bin/bash
set -e
CXX=${CXX:-g++}
FLAGS="-O3 -march=native -std=c++17 -Iinclude -funroll-loops -pthread"

echo "Building server..."
$CXX $FLAGS src/server.cpp -o server
echo "  -> server"

echo "Building client library + benchmark..."
$CXX $FLAGS src/flexql.cpp benchmark_flexql.cpp -o benchmark
echo "  -> benchmark"

echo "Building interactive REPL client..."
$CXX $FLAGS src/flexql.cpp src/repl.cpp -o flexql-client
echo "  -> flexql-client"

echo "Creating client alias..."
$CXX $FLAGS src/flexql.cpp src/repl.cpp -o client
echo "  -> client"

echo ""
echo "Done. Usage:"
echo "  ./server                   # start server (keep running)"
echo "  ./benchmark --unit-test    # correctness: 21/21 tests"
echo "  ./benchmark 10000000       # 10M row benchmark"
echo "  ./flexql-client            # interactive SQL shell"
echo "  ./client                   # interactive SQL shell (alias)"
