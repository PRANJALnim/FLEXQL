# FlexQL

High-performance SQL database driver — client-server, WAL persistence, C++17.

## Build

```bash
bash compile.sh
```

Produces: `server`, `benchmark`, `flexql-client`

## Run

```bash
# Terminal 1 — start server
./server

# Terminal 2 — correctness tests (21/21)
./benchmark --unit-test

# Terminal 2 — 10M row benchmark
./benchmark 10000000

# Terminal 2 — interactive SQL shell (REPL)
./flexql-client
```

## REPL Usage

```
FlexQL Client — connecting to 127.0.0.1:9000...
Connected. Type SQL statements ending with ';', or .help

flexql> CREATE TABLE users(id DECIMAL, name VARCHAR(64), salary DECIMAL);
OK (0ms)
flexql> INSERT INTO users VALUES (1, 'Alice', 90000);
OK (0ms)
flexql> SELECT * FROM users WHERE salary > 50000;
+----+-------+--------+
| ID | NAME  | SALARY |
+----+-------+--------+
| 1  | Alice | 90000  |
+----+-------+--------+
1 row(s)
flexql> .exit
Bye!
```

## Architecture

- **Server:** single binary, TCP:9000 + UNIX socket, WAL persistence, multithreaded
- **Client library:** async INSERT pipeline (background drainer thread), UNIX socket fast path
- **REPL:** interactive terminal using client APIs, formatted table output

## Requirements

- Linux (Ubuntu 18.04+)
- g++ with C++17 support (`g++ --version` should show ≥7)
- No external libraries
