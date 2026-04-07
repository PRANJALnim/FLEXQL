CXX ?= g++
CXXFLAGS ?= -O3 -march=native -std=c++17 -Iinclude -funroll-loops -pthread

.PHONY: all clean

all: server benchmark flexql-client client

server: src/server.cpp
	$(CXX) $(CXXFLAGS) $< -o $@

benchmark: src/flexql.cpp benchmark_flexql.cpp include/flexql.h
	$(CXX) $(CXXFLAGS) src/flexql.cpp benchmark_flexql.cpp -o $@

flexql-client: src/flexql.cpp src/repl.cpp include/flexql.h
	$(CXX) $(CXXFLAGS) src/flexql.cpp src/repl.cpp -o $@

client: src/flexql.cpp src/repl.cpp include/flexql.h
	$(CXX) $(CXXFLAGS) src/flexql.cpp src/repl.cpp -o $@

clean:
	rm -f server benchmark flexql-client client
