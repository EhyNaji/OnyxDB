OnyxDB

A Redis-compatible, in-memory key-value store written from scratch in Rust — with native JSON path queries, dual-protocol networking, and built-in replication.

OnyxDB is not a Redis clone. It speaks the RESP protocol Redis clients already understand, but it adds capabilities Redis itself doesn't ship with out of the box — most notably a JSON document type you can read and write at the field level, without pulling the whole document over the wire.

Why OnyxDB?

Redis is mature, fast, and battle-tested — this project doesn't try to out-Redis Redis on raw throughput. Instead, it targets a gap: applications that need simple, fast, path-addressable JSON storage (session state, agent memory, configuration blobs, nested user profiles) without reaching for a module like RedisJSON or a heavier document database like MongoDB.

JSON.SET user:42 $ {"name":"Marco","address":{"city":"Rome"},"tags":["dev","rust"]}
JSON.GET user:42 $.address.city        → "Rome"
JSON.SET user:42 $.address.city "Milan"
JSON.NUMINCRBY user:42 $.visits 1
JSON.ARRAPPEND user:42 $.tags "backend"

Every JSON.* command is fully persisted (binlog + snapshot) and replicated to connected replicas — it isn't a bolted-on feature, it's part of the core write path.
Note: the codebase is developed by an Italian author, and a fair amount of internal comments are still in Italian. The public interface (commands, this README, error messages) is in English throughout; comment translation is on the to-do list.


Features:
Data types — strings, lists, hashes, sets, and JSON documents with path-level access ($.field, $.nested.field, $.array[N]).

Two wire protocols — RESP (Redis-compatible, works with existing Redis clients and tooling) and OBP (OnyxDB Binary Protocol, a compact custom protocol for lower-overhead access), served on adjacent ports simultaneously.

Replication — asynchronous master/replica replication with partial resync. Each master generates a replication ID on startup; a replica reconnecting after a network blip resumes from its last offset only if the master is provably the same process it was talking to before, otherwise it falls back to a full resync automatically. Optional --auto-failover lets a replica self-promote if its master stays unreachable past a configurable timeout (single-replica setups only — no split-brain coordination across multiple replicas yet).

Persistence — a binary write-ahead log (binlog) for durability plus periodic gzip-compressed snapshot compaction, with configurable fsync policy (always / everysec / no). Startup recovery tolerates a truncated or corrupted binlog without crashing — damaged records are skipped and logged, not fatal.

Access control — multi-user authentication (--user name:password, repeatable) with AUTH user pass, plus legacy single-password mode via --requirepass for compatibility.

Transactions — MULTI / EXEC / DISCARD, queuing commands and executing them as a batch.

Pub/Sub — SUBSCRIBE, UNSUBSCRIBE, PUBLISH, with dynamic channel subscription while a connection is already listening.

Memory management — optional --maxmemory limit with noeviction, allkeys-lru, volatile-lru, allkeys-random, or volatile-random eviction policies.

Observability — Prometheus-formatted metrics exposed over plain HTTP (/metrics) on a dedicated port, alongside a Redis-style INFO command.

Sharded engine — 64 independently-locked shards (FNV-1a hashed) to keep lock contention limited to keys that happen to collide on the same shard, instead of a single global lock.

Quick start

Requires a recent stable Rust toolchain.

bash
git clone https://github.com/<EhyNaji>/onyxdb.git
cd onyxdb
cargo build --release

Start a server:

bash
cargo run --release -- --port 6380

This opens three listeners:

6380 — RESP (Redis protocol)
6381 — OBP (OnyxDB binary protocol)
7380 — Prometheus metrics (http://127.0.0.1:7380/metrics)

Connect with the bundled CLI:

bash
cargo run --release --bin onyx-cli -- --port 6380

Or with any RESP-compatible client, including redis-cli.

Replication

Start a master, then point a second instance at it:

bash
cargo run --release -- --port 6380
cargo run --release -- --port 6385 --replica-of 127.0.0.1:6380

Writes on the master appear on the replica in real time. Replicas reject direct writes (READONLY) — all mutation flows through the master and gets replicated downstream.

Command reference

Strings — SET key value [EX sec|PX ms|EXAT ts] [NX|XX], GET, GETSET, SETNX, MSET, MGET, APPEND, STRLEN, INCR, INCRBY, DECRBY

Keys — DEL, EXISTS, TYPE, EXPIRE [NX|XX], EXPIREAT, TTL, RENAME, COPY, KEYS pattern

Lists — LPUSH, RPUSH, LPOP, RPOP, LRANGE start stop, LLEN

Hashes — HSET, HGET, HGETALL, HDEL, HKEYS, HVALS

Sets — SADD, SREM, SMEMBERS, SISMEMBER

JSON — JSON.SET key path value, JSON.GET key [path], JSON.DEL key path, JSON.TYPE key [path], JSON.NUMINCRBY key path delta, JSON.ARRAPPEND key path value, JSON.ARRLEN key [path], JSON.OBJKEYS key [path]

Path syntax supports field access ($.field) and array indexing ($.field[N]), including nesting ($.a.b[2].c). Wildcards and filters are not supported.

Transactions — MULTI, EXEC, DISCARD

Pub/Sub — SUBSCRIBE channel [channel ...], UNSUBSCRIBE [channel ...], PUBLISH channel message

Server — PING, INFO, SAVE, AUTH [user] password, REPLICAOF NO ONE

Configuration flags
Flag	Description
--port <n>	RESP listener port (default 6380); OBP binds to port+1, metrics to port+1000
--replica-of <host:port>	Start as a replica of the given master
--requirepass <password>	Enable single-password auth (legacy, maps to the default user)
--user <name:password>	Add an authenticated user (repeatable)
--appendfsync <always|everysec|no>	Binlog fsync policy (default everysec)
--maxmemory <size>	Memory limit, accepts suffixes like 100mb, 1gb (default: unlimited)
--maxmemory-policy <policy>	Eviction policy when over the limit (default noeviction)
--auto-failover	Let a replica self-promote after losing its master (single-replica setups only)
--failover-timeout <secs>	Unreachable-master threshold before self-promotion (default 30)
Architecture notes

Storage is a custom sharded engine (OnyxEngine), not a wrapper around an existing embedded database — 64 shards, each behind its own mutex, keyed by an FNV-1a hash. Cross-shard operations (like RENAME) lock shards in a fixed ascending order to avoid deadlocks between concurrent operations touching the same two shards.

Every write command flows through a single choke point (persist_and_replicate) that assigns a monotonic replication offset, pushes the command onto a bounded in-memory backlog (for partial resync), broadcasts it to any subscribed replicas, and appends a binary record to the write-ahead log. Because there's exactly one path for this, JSON commands, string commands, and everything else stay consistent by construction rather than by convention.

The binlog uses a compact, versioned binary format — each record is length-prefixed and self-describing enough that corruption or truncation (e.g. from a crash mid-write) causes that single record to be skipped during recovery, not a failed startup.

Testing
bash
cargo test

The suite (95+ tests at last count) covers binlog encode/decode round-trips for every persisted command, snapshot serialization, the JSON path parser and its edge cases (missing intermediate nodes, out-of-range indices, type mismatches), and the replication resync decision logic — including a regression test for a bug where a replica reconnecting after a master restart could silently miss writes (fixed by binding partial resync to a replication ID, not just an offset number).

Known limitations
--auto-failover has no cross-replica coordination — safe with exactly one replica per master, not with multiple.
JSON paths support field access and array indexing only; no wildcards, no filter expressions.
Vector storage (OnyxValue::Vector) exists in the engine but has no commands wired up yet.
No cluster mode / sharding across multiple nodes — single-master replication only.
The OBP binary protocol currently exposes a small subset of commands (GET/SET/DEL/PING); RESP is the primary interface.
Contributing

Issues and pull requests are welcome. If you're proposing a new command or behavior change, a short description of the use case helps a lot — this project tries to stay focused on solving problems Redis doesn't already solve well, rather than re-implementing its full surface area.

License

MIT — see LICENSE.