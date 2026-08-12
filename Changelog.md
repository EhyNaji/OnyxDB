# Changelog

All notable changes to OnyxDB are documented in this file.

## Unreleased

### Reliability

- Replaced command-text persistence with canonical committed-effect batches so
  argument boundaries, expirations, non-idempotent mutations, eviction victims,
  and transaction results recover exactly.
- Established one authoritative sequence order across concurrent client writes,
  binlog records, the partial-sync backlog, and live replication.
- Made snapshot compaction install and synchronize a replacement snapshot before
  binlog truncation. Recovery replays only sequences after the snapshot
  watermark.
- Added faithful SYNC3 full and partial replication for binary keys, TTLs, and
  all internal value types, with staged atomic full-state installation.
- Added durable `READY`, `INSTALLING`, and `DETACHED` replica lifecycle states.
- Added upstream authentication, sequence-bound heartbeats, liveness deadlines,
  and a hard promotion boundary that cancels and drains former-upstream tasks.
- Added checksummed ONX4 binlog records and fail-closed recovery for complete
  corruption, sequence gaps, and ambiguous legacy histories.
- Enforced projected logical `maxmemory` admission for new keys and existing
  value growth across RESP, OBP, transactions, recovery, and replication.
- Bounded RESP and OBP headers, frames, aggregate arguments, pre-validation
  allocation, idle peers, partial frames, and replication transfers.
- Unified expired, missing, present, and wrong-type semantics across engine and
  server mutation paths, including atomic deletion of empty collections.
- Made write-containing transactions atomic in visibility, persistence, and
  replication order, with rollback on batch persistence failure.

### Architecture and maintainability

- Extracted typed startup configuration parsing and validation into
  `src/config.rs`, including command-line/environment precedence and redacted
  secret debug output.
- Rejected server ports that cannot safely reserve the derived OBP and metrics
  listeners.
- Removed unused concurrency/CPU dependencies and the empty storage module
  placeholder.
- Normalized repository source comments, diagnostics, test names, and benchmark
  fixtures to professional English.
- Added current architecture and reliability invariant documentation.
- Made continuous integration run locked formatting, lint, and all-target tests.

## 0.1.0 - Initial development baseline

### Added

- A 64-shard in-memory engine with strings, integers, lists, hashes, sets, JSON,
  and an internal vector representation.
- RESP and OBP listeners.
- JSON field and array path commands.
- Write-ahead logging, gzip-compressed snapshots, and asynchronous replication.
- Authentication, bounded transactions, Pub/Sub, memory policies, metrics, a
  minimal CLI, and a development benchmark.

The `0.1.0` codebase was an early development baseline. The unreleased
reliability work above supersedes its original persistence and replication
behavior.
