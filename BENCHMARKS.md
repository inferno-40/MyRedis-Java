# Benchmark Results

Measured with [`scripts/loadtest.py`](scripts/loadtest.py), a raw-socket RESP client (no external Redis client
library) that opens one persistent TCP connection per worker thread and fires requests sequentially on each
connection, then reports aggregate throughput and latency percentiles.

## Methodology

- **Machine**: macOS, local loopback (`127.0.0.1`), single machine, no other load.
- **Server**: compiled with `javac` and run directly via `java Main` (no RDB persistence enabled).
- **Client**: `scripts/loadtest.py`, run from the same machine.
- These are self-measured, informal numbers meant to characterize the server's behavior — not a
  production-representative or third-party-audited benchmark.

## Results

| Test | Requests | Throughput | Avg latency | p50 | p99 | Errors |
|---|---|---|---|---|---|---|
| `PING`, 1 connection | 2,000 | ~33,300 req/sec | 0.029ms | 0.019ms | 0.098ms | 0 |
| `SET`, 1 connection | 2,000 | ~33,100 req/sec | 0.029ms | 0.022ms | 0.054ms | 0 |
| `GET`, 1 connection | 2,000 | ~29,400 req/sec | 0.032ms | 0.029ms | 0.079ms | 0 |
| `PING`, 10 concurrent connections | 20,000 | ~44,500 req/sec | 0.222ms | 0.197ms | 0.624ms | 0 |
| `SET`, 10 concurrent connections | 20,000 | ~43,900 req/sec | 0.224ms | 0.202ms | 0.609ms | 0 |
| `GET`, 10 concurrent connections | 20,000 | ~42,700 req/sec | 0.231ms | 0.209ms | 0.611ms | 0 |
| `PING`, 50 concurrent connections | 50,000 | ~45,300 req/sec | 0.658ms | 0.197ms | 0.609ms | 0 |

## Observations

- Throughput scales from a single connection (~33K req/sec) up to the server's fixed 10-thread connection pool
  (~44-45K req/sec aggregate), after which it plateaus — confirming the `ExecutorService` thread pool size
  (`Executors.newFixedThreadPool(10)` in [`Main.java`](src/main/java/Main.java)) is the throughput ceiling.
- Pushing concurrency to 50 (5x the pool size) holds the same aggregate throughput, but tail latency grows
  (max latency reached 885ms in that run) as excess connections queue for a free worker thread — the expected
  behavior for a fixed-size thread pool under overload.
- Zero failed requests across all runs, including 50,000 sequential requests over 50 concurrent connections.

## Reproducing

```sh
# Terminal 1: compile and start the server
javac -d out $(find src/main/java -name "*.java")
java -cp out Main

# Terminal 2: run a benchmark
python3 scripts/loadtest.py PING 10 2000
python3 scripts/loadtest.py SET 10 2000
python3 scripts/loadtest.py GET 10 2000
```
