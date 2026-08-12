# OnyxDB

OnyxDB is an experimental in-memory data platform written in Rust. It exposes a
bounded RESP interface for a Redis-like command subset and a compact native
binary protocol (OBP). Its distinguishing data model is a native JSON value with
path-level reads and mutations.

The project is in active development. The persistence and replication paths have
strong regression coverage, but OnyxDB is not yet presented as a production
replacement for Redis. Review the [known limitations](#known-limitations) before
using it with important data.

## Highlights

- Strings, integers, lists, hashes, sets, and native JSON documents.
- JSON field and array access such as `$.profile.name` and `$.items[2]`.
- RESP and OBP listeners with bounded, fail-closed frame parsing.
- Checksummed write-ahead logging and versioned gzip-compressed snapshots.
- Ordered asynchronous master/replica synchronization with authenticated
  upstream connections, partial resynchronization, and full-state replacement.
- Projected `maxmemory` admission with optional LRU or random eviction.
- Bounded `MULTI`/`EXEC` queues and atomic visibility for committed transaction
  batches.
- Prometheus-formatted metrics and a Redis-style `INFO` response.

## Quick start

OnyxDB requires a recent stable Rust toolchain.

```bash
git clone https://github.com/EhyNaji/OnyxDB.git
cd OnyxDB
cargo build --release --locked
cargo run --release --locked -- --port 6380
```

The server binds only to loopback and opens three listeners derived from the
configured port:

| Listener | Default address | Purpose |
| --- | --- | --- |
| RESP | `127.0.0.1:6380` | Primary client protocol |
| OBP | `127.0.0.1:6381` | Native binary protocol |
| Metrics | `127.0.0.1:7380` | HTTP `/metrics` endpoint |

Connect with the bundled interactive client:

```bash
cargo run --release --locked --bin onyx-cli -- --port 6380
```

The client is intentionally small and is not a full `redis-cli` replacement.
For exact argument boundaries, binary data, or scripted use, prefer a RESP
client library.

## JSON example

```text
JSON.SET user:42 $ {"name":"Morgan","address":{"city":"Rome"},"tags":["dev","rust"]}
JSON.GET user:42 $.address.city
JSON.SET user:42 $.address.city "Milan"
JSON.NUMINCRBY user:42 $.visits 1
JSON.ARRAPPEND user:42 $.tags "backend"
```

Paths support object fields and non-negative array indices. Wildcards, filters,
recursive descent, and automatic creation of missing intermediate objects are
not supported.

## Command surface

- Strings and numbers: `SET`, `GET`, `GETSET`, `SETNX`, `MSET`, `MGET`,
  `APPEND`, `STRLEN`, `INCR`, `INCRBY`, `DECRBY`
- Keys: `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `EXPIREAT`, `TTL`, `RENAME`,
  `COPY`, `KEYS`
- Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
- Hashes: `HSET`, `HGET`, `HGETALL`, `HDEL`, `HKEYS`, `HVALS`
- Sets: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`
- JSON: `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE`,
  `JSON.NUMINCRBY`, `JSON.ARRAPPEND`, `JSON.ARRLEN`, `JSON.OBJKEYS`
- Transactions: `MULTI`, `EXEC`, `DISCARD`
- Pub/Sub: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- Server: `PING`, `INFO`, `SAVE`, `AUTH`, `REPLICAOF NO ONE`

This is a compatible subset, not a claim of complete Redis protocol or command
compatibility.

## Configuration

| Option | Meaning |
| --- | --- |
| `--port <n>` | RESP port; must leave room for OBP at `port+1` and metrics at `port+1000` (default `6380`) |
| `--replica-of <host:port>` | Start as a replica of the specified master |
| `--masterauth <password>` | Password used to authenticate to the master |
| `--masteruser <name>` | Master authentication user; defaults to `default` and requires a password |
| `--requirepass <password>` | Configure the legacy `default` user password |
| `--user <name:password>` | Configure an authenticated user; repeatable |
| `--appendfsync <always\|everysec\|no>` | Binlog synchronization policy (default `everysec`) |
| `--maxmemory <size>` | Logical dataset limit, for example `100mb` or `1gb`; zero/unset disables it |
| `--maxmemory-policy <policy>` | `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-random`, or `volatile-random` |
| `--auto-failover` | Self-promote after the upstream remains unavailable beyond the timeout |
| `--failover-timeout <seconds>` | Auto-failover threshold (default `30`) |

`ONYXDB_PASSWORD`, `ONYXDB_MASTER_USER`, and `ONYXDB_MASTER_PASSWORD` are
supported. Command-line values take precedence. Environment variables reduce
credential exposure through process listings, but OnyxDB does not currently
provide TLS.

## Persistence

Runtime data is written in the process working directory:

- `onyx.binlog`: ordered committed-effect records with sequence numbers and
  CRC32 checksums.
- `onyx.snapshot`: the current versioned, gzip-compressed snapshot.
- `onyx.snapshot.previous`: the previous snapshot retained across replacement.
- `onyx.replica`: durable replica lifecycle and synchronization identity.

A mutation is published to replicas only after its binlog append succeeds. The
acknowledgement durability depends on `--appendfsync`: `always` synchronizes each
batch, `everysec` synchronizes in the background, and `no` relies on the
operating system after userspace flush.

Recovery installs a snapshot and replays only binlog sequences after the
snapshot watermark. A recognizable incomplete final record may be truncated;
complete corrupted records, sequence gaps, and ambiguous legacy data fail
startup rather than being skipped. Compaction installs and synchronizes the new
snapshot before truncating the binlog.

## Replication

Start a master and a replica in separate working directories so their runtime
files do not overlap:

```bash
# Master directory
cargo run --release --locked -- --port 6380

# Replica directory
cargo run --release --locked -- \
  --port 6385 \
  --replica-of 127.0.0.1:6380
```

Replicas reject direct mutations. Incremental synchronization uses a master
process identity and a contiguous committed sequence; otherwise the replica
installs a complete snapshot. Full synchronization is staged and becomes
visible only after its durable baseline is installed. Promotion first cancels
and drains the old upstream lifecycle, then durably detaches the replica before
accepting local writes.

Upstream authentication is available through `--masteruser` and
`--masterauth`, or their environment-variable equivalents.

`--auto-failover` is safe only when external deployment rules guarantee a
single promotion candidate. OnyxDB has no quorum, fencing service, or consensus
protocol and therefore cannot prevent split brain among multiple replicas.

## Transactions and memory admission

Transaction queues are limited to 1,024 commands and approximately 16 MiB of
encoded arguments. A transaction containing writes is serialized through the
same authoritative write order as a single mutation. Its successful state
changes become one persistence and replication batch. Runtime errors for one
queued command are returned in the result array and do not imply Redis-style
rollback of unrelated successful commands.

`maxmemory` is enforced against projected logical dataset usage after a
mutation. Growth that cannot fit is rolled back. Eviction victims are part of
the same committed batch, so recovery and replicas observe the same result.
This limit is a stable logical accounting measure, not a process RSS limit.

## Verification

Run the same checks used by continuous integration:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
git diff --check
```

The suite includes unit, malformed-input, concurrency, persistence restart,
replication, lifecycle, and real network/subprocess coverage.

The bundled `onyx-bench` supports bounded, repeatable GET, SET, mixed, and native
JSON workloads with warmup, concurrency, pipelining, error accounting,
p50/p95/p99/p99.9 completion latency, and JSON output. See
[docs/benchmarking.md](docs/benchmarking.md) for the methodology and comparison
rules. Benchmark output is evidence from one environment, not a general
performance claim.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the current module
boundaries, mutation ordering, persistence and replication invariants, and the
incremental decomposition plan.

## Known limitations

- Loopback-only listeners; no configurable external bind address.
- No TLS and no command-level authorization. Authenticated users have the same
  command permissions.
- No cluster mode, consensus, quorum, or automatic multi-replica fencing.
- Replication is asynchronous; acknowledged master writes can be ahead of a
  replica.
- Pub/Sub is ephemeral and is neither persisted nor replicated.
- OBP exposes only a small command subset and is not a stable public protocol.
- JSON path support is deliberately limited as described above.
- The engine contains an internal vector value representation, but no public
  vector commands are implemented.
- The server runtime remains concentrated in `main.rs`; decomposition is in
  progress and is being performed in reviewable, invariant-preserving steps.

## License

OnyxDB is licensed under the MIT License. See [License](License).
