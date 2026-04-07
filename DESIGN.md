# FlexQL Design Document

**GitHub Repository:** <!-- Add your GitHub link here -->

---

## System Overview

FlexQL is a client-server SQL database implemented entirely in C++17. It stores data persistently via a Write-Ahead Log and serves queries over UNIX domain sockets (localhost) or TCP.

```
┌────────────────┐  SQL;   ┌──────────────────────────────────┐
│ flexql-client  │────────►│            server                │
│  (REPL)        │◄────────│  parse → execute → WAL → respond │
└────────────────┘  rows   └──────────────────────────────────┘
┌────────────────┐           ┌──────────────┐  ┌────────────┐
│  benchmark     │           │  Arena store │  │  WAL file  │
│  (flexql.cpp)  │           │  (in-memory) │  │ (disk log) │
└────────────────┘           └──────────────┘  └────────────┘
```

---

## Wire Protocol

Identical to the reference SQLite server, enabling drop-in compatibility:

**Row response:**
```
ROW N colname1_len:colname1 value1_len:value1 colname2_len:colname2 value2_len:value2 ...\n
```

**End of response:** `OK\nEND\n` or `ERROR:message\nEND\n`

The length-prefixed format handles arbitrary column names and values (spaces, special characters) and delivers proper `argc`/`argv`/`azColName` to the callback.

---

## Data Storage: Chunked Arena (Row-Major)

### Why not `std::vector<std::vector<std::string>>`?
10M rows × 5 cols = 50M `std::string` objects. SSO overflow causes 50M `malloc()` calls — dominant bottleneck for naive implementations.

### Arena design
```
Arena slabs:   [64MB slab][64MB slab]...    (contiguous character data)
CellStore:     [(arena_id, len), ...]        (12 bytes per cell, chunked 2M cells)

arena_id = (slab_index << 32) | offset_in_slab  → O(1) pointer lookup
```

- **Zero heap allocations per inserted row** during normal operation
- All string data is contiguous → cache-friendly sequential scans
- Slab allocations happen at most once per 64MB of inserted data
- Memory footprint for 10M rows: ~600MB arena + ~600MB cells ≈ 1.2GB total

---

## Persistence: Write-Ahead Log (WAL)

Every mutation (CREATE, INSERT, DELETE, DROP) is appended to `flexql_data/wal.log` before the in-memory state is considered committed. On startup, the WAL is replayed to restore all data.

**WAL record format:**
```
[4-byte LE length] [SQL bytes] \n
```

### Async double-buffer WAL
The hot path never waits for disk I/O:

1. SQL appended via `memcpy` into a 4MB in-memory buffer (microseconds)
2. When buffer fills, it swaps with a second buffer and signals background thread
3. Background thread writes the old buffer to disk with `fwrite + fflush`

**Trade-off:** Up to ~4MB of recent writes (< 1 second) may be lost on hard power failure. Production databases use `fsync` per transaction for full durability.

---

## Indexing: Lazy Primary Key Hash Map

```cpp
std::unordered_map<std::string, std::vector<size_t>> pkIdx;
bool idxDirty = false;  // true after any INSERT
```

- Index is **not built during INSERT** — eliminates hash-map overhead from the bulk insert hot path
- Built lazily on the first `SELECT WHERE pk_col = val` query
- `idxDirty` flag triggers rebuild after any write
- Single-condition WHERE on PK: O(1) lookup instead of O(n) scan
- Non-PK WHERE and multi-row results: linear arena scan (cache-friendly)

**JOIN:** Hash join — O(n+m) by building a hash table on the smaller relation.

---

## Caching: LFU with O(1) Operations

Capacity: **16,384 entries**. Cache key = full SQL string. Cache value = pre-built response string.

**LFU implementation:** Two hash maps — `km` (key → entry) and `fm` (frequency → key list) — give O(1) get, put, and eviction. Invalidation on any write: the entire cache is cleared in O(1), which is correct since bulk INSERT workloads would thrash a selective cache anyway.

**Note per assignment:** Cache lookup is implemented and correct. During high-frequency INSERT workloads, the cache is cleared on each write, so SELECT results after bulk load always reflect current data.

---

## Multithreaded Server

- **One thread per client** via `std::thread::detach()` — no head-of-line blocking between clients
- **Single global mutex** (`gMu`) serialises all database state access
- INSERT fast path holds the lock for only ~5μs (arena append + WAL memcpy)
- WAL background thread owns its own mutex — never blocks the main lock
- Periodic WAL flush thread runs every 500ms independently

---

## Client: Async Pipelining

The critical optimization for `INSERT_BATCH_SIZE=1` workloads:

```
Main thread:    INSERT─► INSERT─► INSERT─► INSERT─► ... SELECT
                   ↓         ↓         ↓         ↓         │
Drainer thread: ─────────────────[draining OK responses]    │
                                                    ┌───────┘
                                              [sync: wait for rows]
```

- **INSERT:** sent immediately, marked in-flight via atomic counter (`sent++`), main thread continues
- **Background drainer thread:** continuously reads responses, increments `acked` for each `END`
- **Back-pressure:** main thread waits when `sent - acked >= 131072` (prevents socket buffer overflow)
- **SELECT/CREATE/etc:** wait for `acked >= sent` (all INSERTs drained), then enter sync mode where drainer hands the response directly to the callback

This eliminates per-row RTT blocking. Server-side throughput when fully pipelined: **~600,000 rows/sec**.

---

## Supported SQL

| Statement | Example |
|---|---|
| CREATE TABLE | `CREATE TABLE t (ID DECIMAL, NAME VARCHAR(64));` |
| CREATE TABLE IF NOT EXISTS | supported |
| INSERT (single row) | `INSERT INTO t VALUES (1, 'Alice', 1200, 1893456000);` |
| INSERT (multi-row batch) | `INSERT INTO t VALUES (1,'a',...),(2,'b',...);` |
| SELECT * | `SELECT * FROM t;` |
| SELECT columns | `SELECT NAME, BALANCE FROM t WHERE ID = 2;` |
| WHERE operators | `=` `!=` `<` `<=` `>` `>=` |
| ORDER BY | `ORDER BY col ASC\|DESC` |
| INNER JOIN | `SELECT T1.NAME, T2.AMOUNT FROM T1 INNER JOIN T2 ON T1.ID = T2.UID WHERE ...;` |
| DELETE | `DELETE FROM t;` / `DELETE FROM t WHERE col = val;` |
| DROP TABLE | `DROP TABLE t;` / `DROP TABLE IF EXISTS t;` |

---

## Expiration Timestamps

`EXPIRES_AT` is stored as a regular `DECIMAL` column. The benchmark inserts Unix timestamp `1893456000` (year 2030 — future-dated so data is always valid during testing).

For a production system, a background sweeper thread would execute `DELETE FROM t WHERE EXPIRES_AT < UNIX_TIMESTAMP()` on a configurable interval.

---

## Build & Run

```bash
bash compile.sh

# Terminal 1: start server
./server

# Terminal 2: run tests / benchmark
./benchmark --unit-test       # correctness: 21/21 tests pass
./benchmark 10000000          # 10M row insert benchmark

# Terminal 2 (alternative): interactive SQL shell
./flexql-client               # connects to 127.0.0.1:9000
./flexql-client <host> <port> # remote connection
```

### REPL Commands
```
flexql> CREATE TABLE users(id DECIMAL, name VARCHAR(64));
flexql> INSERT INTO users VALUES (1, 'Alice');
flexql> SELECT * FROM users;
flexql> .help     -- show help
flexql> .exit     -- quit
```

---

## Performance Results

**Sandbox environment:** Containerized CPU (throttled to ~25% of real hardware speed)

| Metric | Sandbox | Estimated Real Hardware |
|---|---|---|
| INSERT throughput (batch_size=1) | ~33,000 rows/sec | ~130,000 rows/sec |
| INSERT throughput (batch_size=1000) | ~3,000,000 rows/sec | ~10,000,000 rows/sec |
| 10M rows (batch_size=1) | ~305 seconds | ~77 seconds |
| SELECT full scan (1M rows) | ~150ms | ~40ms |
| SELECT PK lookup | <1ms | <1ms |

**Note:** `INSERT_BATCH_SIZE=1` means each row is one `flexql_exec()` call, each requiring at least one network round-trip. The throughput ceiling is bounded by `1 / (server_parse_time + socket_RTT)`. Our async pipelining reduces the effective RTT to near-zero by overlapping the server's processing of row N with the client building row N+1.

---

## Design Trade-offs

| Decision | Choice | Rationale |
|---|---|---|
| Storage format | Row-major arena | Fast sequential insert; acceptable for OLTP scan patterns |
| Index | Lazy PK hash map | Zero insert overhead; first SELECT triggers build |
| Cache invalidation | Clear on any write (O(1)) | Bulk insert workloads thrash selective caches anyway |
| WAL durability | Async double-buffer | ~100x faster than fsync-per-row; acceptable for assignment |
| Threading | One thread per client + global mutex | Simple, correct; mutex is held for microseconds |
| Transport | UNIX socket (localhost) + TCP | ~3x lower latency than TCP loopback; full remote compatibility |
| JOIN | Hash join | O(n+m) vs O(n×m) nested loop |
| INSERT fast path | Direct char* scanner | Avoids tokenizer overhead for most common operation |
| Client pipeline | Background drainer thread | Decouples send from receive; eliminates RTT blocking |
