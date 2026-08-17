# Benchmarking OnyxDB

`onyx-bench` provides a bounded RESP benchmark methodology for development and
repeatable comparison. It reports client-observed throughput, completion
latency percentiles, and errors. It does not make performance claims by itself.

## Build and run

Build the server and benchmark in release mode:

```bash
cargo build --release --locked
cargo run --release --locked -- --port 6380 --appendfsync everysec
cargo run --release --locked --bin onyx-bench -- \
  --address 127.0.0.1:6380 \
  --metrics-address 127.0.0.1:7380 \
  --label onyxdb \
  --workload mixed \
  --requests 1000000 \
  --warmup 100000 \
  --concurrency 32 \
  --pipeline 1 \
  --keyspace 100000 \
  --payload-size 128 \
  --repeats 5
```

Run `onyx-bench --help` for the complete option list. Use `--output json` for a
machine-readable report.

## Server observability

Pass `--metrics-address` to capture the OnyxDB Prometheus endpoint immediately
before each measured run and again after the commit coordinator and automatic
compaction become quiescent. Metrics sampling and the bounded quiescence wait
are outside the measured elapsed time. Human reports summarize commit groups,
logical batches, physical binlog appends, records per append, compaction time,
queue wait, and queue high-water. JSON methodology version 2 includes complete
`before`, `after`, and monotonic-counter `delta` maps for deeper analysis.

The option is deliberately explicit. A benchmark without `--metrics-address`
does not contact a metrics endpoint and retains the same traffic shape as the
original methodology. OnyxDB exposes metrics without authentication, so only
target a trusted interface.

### Compaction workload

Use enough committed mutations to cross the 100,000-record automatic
compaction threshold. Keep short and long cases separate so normal commit-path
latency is not confused with snapshot pauses. For example:

```bash
cargo run --release --locked --bin onyx-bench -- \
  --address 127.0.0.1:6380 \
  --metrics-address 127.0.0.1:7380 \
  --label onyxdb-compaction \
  --workload set \
  --requests 600000 \
  --warmup 100000 \
  --concurrency 20 \
  --pipeline 32 \
  --keyspace 100000 \
  --payload-size 64 \
  --repeats 3
```

Report commit latency percentiles together with compaction count, total and
maximum duration, and the barrier, snapshot-capture, snapshot-write, and
rotation phase counters. Dataset cardinality materially changes snapshot cost.

## Workloads

| Workload | Operation mix | Dataset preparation | Redis comparison |
| --- | --- | --- | --- |
| `get` | 100% `GET` | Preloads every key with `SET` | Yes |
| `set` | 100% `SET` | None | Yes |
| `mixed` | Alternating `SET` and `GET` | Preloads every key | Yes |
| `json-get` | 100% `JSON.GET key $.payload` | Preloads native JSON documents | No; OnyxDB-specific |
| `json-set` | 100% root `JSON.SET` | None | No; OnyxDB-specific |

Operation and key selection are deterministic. Each operation selects
`operation_index % keyspace`. The default key prefix contains the benchmark PID
and start time to avoid overlapping unrelated runs. Set `--key-prefix` when an
identical namespace is required.

The benchmark deletes its keyspace after the final run. `--keep-data` disables
cleanup for inspection or a follow-up workload. Cleanup is not included in the
measurement.

## Measurement boundary

The benchmark performs these stages:

1. Validate all configuration and projected client batch memory.
2. Connect and authenticate a dataset-setup client, when required.
3. Prepare the keyspace outside the measurement.
4. Establish worker connections outside the measurement.
5. Run an unmeasured warmup phase.
6. For each repeat, establish fresh worker connections, start the timer, submit
   the requested operations, consume every response, and stop the timer after
   all workers complete.
7. When server metrics are enabled, wait for coordinator and automatic
   compaction quiescence and capture the second metrics sample outside the
   measurement.
8. Delete the benchmark keyspace outside the measurement.

Throughput is completed responses divided by measured wall-clock time. A run is
unsuccessful when a response has the wrong type, the server returns an error, a
transport operation fails, or completed responses differ from requested
operations.

Latency is recorded for every completed response from the instant its pipeline
batch is submitted. With pipeline depth one, this is request/response latency.
With deeper pipelines, it is per-response completion latency from the common
batch-submission time and therefore includes position within the batch. Reports
use nearest-rank p50, p95, p99, and p99.9 values.

Connection establishment, dataset preparation, warmup, and cleanup are
deliberately excluded. Task creation at the start of a measured run is included.

## Authentication

Use environment variables so credentials do not appear in process arguments:

```bash
export ONYXDB_BENCH_USER=benchmark
export ONYXDB_BENCH_PASSWORD=secret
```

`ONYXDB_BENCH_USER` is optional; without it, `AUTH password` targets the
`default` user. Credentials are not included in either report format.

## Comparing OnyxDB and Redis

Only `get`, `set`, and `mixed` are directly comparable. Use the same:

- benchmark executable and host;
- payload, keyspace, request count, warmup, concurrency, and pipeline settings;
- persistence policy and durability expectation;
- authentication state;
- CPU affinity, network placement, operating-system settings, and background
  load;
- number and order of repeated runs.

Change only `--address` and `--label`. Store each JSON report with the server
configuration and hardware description. Do not compare OnyxDB `appendfsync no`
against a Redis durability mode that performs physical synchronization, or vice
versa.

## Resource bounds

The benchmark rejects:

- more than 10 million measured or warmup operations per run;
- more than 1,024 concurrent connections;
- more than 4,096 commands per pipeline;
- more than one million keys;
- payloads that exceed the server command boundary;
- pipeline/payload combinations projected above the 64 MiB client batch limit;
- oversized labels, addresses, and key prefixes.

Latency samples require eight bytes per completed operation before vector
overhead. The operation limit bounds this storage. Dataset setup dynamically
reduces its batch size for large payloads.

## Current limitations

- The methodology uses a fixed operation count rather than a fixed-duration
  steady-state interval.
- It does not correct for coordinated omission.
- Environment reporting includes OS, architecture, logical CPU count, and
  benchmark version, but not CPU model, memory topology, kernel tuning, or
  server configuration. Record those externally.
- Client and server CPU consumption can interfere when run on the same host.
- OBP workloads are not implemented yet. RESP and OBP must not be compared until
  the native client has equivalent validation and measurement semantics.
- The benchmark is not a profiler. Use profiling evidence before changing server
  hot paths.
