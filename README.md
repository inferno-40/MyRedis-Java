[![progress-banner](https://backend.codecrafters.io/progress/redis/e1236dee-3a7d-4242-ab87-7904f32d5f17)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# MyRedis-Java

A Redis server implemented from scratch in Java — no external Redis libraries, just raw sockets and the RESP wire protocol. Built as part of the CodeCrafters ["Build Your Own Redis"](https://codecrafters.io/challenges/redis) challenge.

The server speaks the real Redis protocol, so it can be tested with `redis-cli` or any standard Redis client.

## Features

- **TCP server** that accepts multiple concurrent client connections, each handled on its own thread via a fixed thread pool (`ExecutorService`).
- **RESP (REdis Serialization Protocol) parsing and encoding** for arrays and bulk strings, implemented by hand.
- **In-memory key-value store** backed by a `ConcurrentHashMap` for thread-safe access across client connections.
- **Key expiry**, supported via `SET ... PX <milliseconds>` and lazily evaluated (expired keys are removed on read).
- **RDB persistence** — a minimal reader/writer for the Redis RDB binary file format, so the store can load an existing `dump.rdb` on startup and (optionally) persist state back out.

### Supported commands

| Command | Description |
|---|---|
| `PING` | Health check, replies `PONG`. |
| `ECHO <message>` | Echoes back the given message. |
| `SET <key> <value> [PX <ms>]` | Sets a key, with an optional expiry in milliseconds. |
| `GET <key>` | Returns a key's value, or a null bulk reply if missing/expired. |
| `KEYS *` | Returns all keys currently in the store. |
| `CONFIG GET <parameter>` | Returns configuration values (e.g. `dir`, `dbfilename`) when RDB support is enabled. |

## Project structure

```
src/main/java/
├── Main.java                       # Entry point: opens the server socket, dispatches clients to a thread pool
├── ClientHandler.java               # Per-connection loop: reads RESP input, routes commands, writes RESP output
├── config/
│   └── RDBConfig.java                # Parses --dir/--dbfilename CLI args, holds RDB configuration
├── rdb/
│   ├── RDBParser.java                 # Reads an existing RDB file into the in-memory store on startup
│   └── RDBCreator.java                 # Serializes the in-memory store out to an RDB file
├── redisdatastructure/
│   └── RedisCache.java                 # Thread-safe in-memory key-value store with expiry metadata
└── resp/
    └── RespConvertor.java               # Encodes Java values into RESP bulk strings / arrays
```

## Getting started

### Prerequisites

- Java 21
- Maven

### Build and run

```sh
./your_program.sh
```

This compiles the project with Maven and starts the server on port `6379`, the default Redis port.

To enable RDB persistence, pass a data directory and filename:

```sh
./your_program.sh --dir /tmp/redis-data --dbfilename dump.rdb
```

### Try it out

With the server running, connect using `redis-cli` (or `nc`) in another terminal:

```sh
redis-cli PING
redis-cli SET foo bar
redis-cli GET foo
redis-cli SET foo bar PX 100
redis-cli KEYS "*"
```

## Notes

This is a learning project built incrementally while working through the CodeCrafters challenge stages — it implements a useful subset of Redis rather than the full command set (no pub/sub, transactions, replication, or additional data types like lists/hashes/sets yet).
