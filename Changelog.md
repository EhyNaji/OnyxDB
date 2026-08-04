# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] — Initial public release

### Added
- Core sharded storage engine (64 shards, FNV-1a hashed, per-shard locking)
- RESP protocol support (Redis-compatible clients work out of the box)
- OnyxDB Binary Protocol (OBP) — a compact custom protocol served alongside RESP
- Data types: strings, lists, hashes, sets
- **JSON document type with path-level access**: `JSON.SET`, `JSON.GET`, `JSON.DEL`,
  `JSON.TYPE`, `JSON.NUMINCRBY`, `JSON.ARRAPPEND`, `JSON.ARRLEN`, `JSON.OBJKEYS`,
  supporting nested field access (`$.a.b`) and array indexing (`$.a[N]`)
- Binary write-ahead log (binlog) with per-command persistence and crash-tolerant recovery
- Gzip-compressed snapshot compaction
- Configurable fsync policy (`always` / `everysec` / `no`)
- Master/replica replication with partial resync based on a per-process replication ID,
  falling back to full resync automatically when a master restarts
- Optional `--auto-failover` for single-replica setups
- Multi-user authentication (`--user name:password`, repeatable) plus legacy
  single-password mode (`--requirepass`)
- `MULTI` / `EXEC` / `DISCARD` transactions
- `SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH` pub/sub
- Configurable memory limit (`--maxmemory`) with `noeviction`, `allkeys-lru`,
  `volatile-lru`, `allkeys-random`, `volatile-random` eviction policies
- Prometheus-formatted metrics endpoint
- 96 automated tests covering the JSON path parser, binlog round-trips for every
  persisted command, snapshot serialization, and replication resync logic

### Fixed during development (pre-release)
- `DECRBY` was written to the binlog but never decoded on replay, silently
  losing decrements across a restart
- A replica reconnecting after a master restart could be told "you're already
  up to date" based on a stale offset it had no way to verify, silently
  missing writes — fixed by requiring the replica to present a replication ID
  that matches the master's current process, not just an offset number
- `JSON.SET` / `JSON.DEL` were assigned binlog op-codes but had no serialization
  logic wired up, so JSON writes were never actually persisted despite appearing to work in memory