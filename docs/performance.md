# Performance

The design's v1 targets are defined on an 8 vCPU / 32 GiB machine with data in S3-compatible
storage and datasets up to 100 M spans / 1 TB of payloads. The numbers below were measured in
the development environment, which is much smaller: **4 vCPU, 15 GiB RAM, local disk, the load
generator running on the same cores**, and datasets of up to ~1.2 M spans. They show the shape
of the system, not the release-check numbers; those still need a run on the reference machine
against S3 (`forge bench` and the CI `bench` job are set up for it).

All runs use release builds and `forge bench`, which generates agent-shaped traces: depth up to
8, fan-out 4, GenAI chat / tool / retriever / agent spans, ~2 KB median payload, 20% of spans
posted open and closed by a later upsert, 2% duplicates, out-of-order delivery within windows.

## Against the targets

| Metric | Target | Measured here | Dataset / notes |
| --- | --- | --- | --- |
| Ingest throughput | ≥ 50,000 spans/s sustained | 23–28 k spans/s in-process; 13.4 k spans/s over OTLP/HTTP | 4 vCPU shared with the generator; HTTP run was rate-capped at 15 k/s with 16 connections |
| Ingest-to-queryable | ≤ 2 s p99 | 0 s: a span is in the memtable before the request is acknowledged | `forge-server/tests/api.rs` reads traces immediately after export |
| Trace tree load | ≤ 100 ms p50, ≤ 500 ms p99 | 28 ms p50, 48 ms p99 (with payloads); 13 ms p50 without payloads | 2,000-span trace in 1.22 M spans, warm |
| Trace tree load, cold | ≤ 400 ms p50 | 67 ms | fresh process, local disk (OS page cache warm) |
| Huge trace load | ≤ 3 s | 2.7 s p50 warm with payloads (4.1 s p99), 1.3 s without payloads; 3.7 s cold | 100,000-span trace, still in L0 (merge on read) |
| List runs, newest 50 with 2 filters | ≤ 300 ms p50 | 63 ms p50, 120 ms p99 | 1.22 M spans (target: 100 M) |
| Full-text search, newest 50 hits | ≤ 500 ms p50 | 14 ms p50, 19 ms p99 | 1.22 M spans after compaction, query `refund policy` |
| Memory at idle | ≤ 300 MiB RSS | 30 MiB | empty server, static musl build |

Also measured on the 1.22 M-span dataset: a SQL `GROUP BY run_type` with `avg(total_tokens)`
over all spans, 21 ms p50; compaction of 50 L0 segments (317 MB after compaction) into 6 L1
segments in 46 s.

For huge traces about half of the warm time is payload handling (2.7 s with payloads vs 1.3 s
without: reading, resolving and parsing 100 k JSON payloads); the rest is reading and merging
the rows and building the nested tree. The cold run is over the target. Streaming the tree
instead of materializing it, and skipping the merge for spans with a single version, are the
obvious next steps; neither has been profiled in detail yet.

## Soak (shortened)

The M3 exit test is a 24-hour soak at 20 k spans/s. What was run here is a 3-minute soak at
13.4 k spans/s over OTLP/HTTP, with flushes every 5 s and compaction every 15 s:

| t | RSS | live segments (L0) |
| --- | --- | --- |
| 20 s | 2.1 GB | 13 (13) |
| 60 s | 2.3 GB | 13 (9) |
| 120 s | 3.1 GB | 23 (13) |
| 160 s | 2.5 GB | 20 (6) |
| after load | 0.55 GB | 17 (2) |

An earlier version of the compactor rewrote the whole open hour on every round; the same soak
grew to 11 GB RSS. The as-built policy (L0 → L1 runs, size-tiered run merges, full rewrite when
the hour closes, merges bounded by rows) is described in `docs/architecture.md`.

## Where the time goes

- **Flush** is dominated by building the `.fts` sidecar (tokenizing every payload). After
  optimization (borrowed term buffers, hash postings with per-row dedup, parallel build) a
  23 k-row L0 segment takes ~0.6 s to encode and ~0.6 s to index, concurrently.
- **Trace loads** read one trace's rows from 1–3 segments; segments are sorted by trace id, so
  Vortex zone maps prune to a narrow range. Resolving and parsing payload JSON is about half of
  the warm time.
- **Search** cost is bounded by the number of candidates verified, not by data size: candidates
  are verified newest first in small chunks and the scan stops at the limit.

## Criterion benchmarks

`cargo bench -p forge-model -p forge-index` (same 4 vCPU machine):

| Benchmark | Time |
| --- | --- |
| `merge_versions`, 50 k rows, 20% upserts | 23.5 ms |
| `merge_versions`, 50 k unique rows | 7.6 ms |
| normalize 500 GenAI spans (OTLP → rows) | 2.8 ms |
| `dotted_order` fill, 5 k spans | 5.4 ms |
| `rows_to_batch`, 5 k rows | 0.67 ms |
| build `.fts` sidecar, 20 k rows × 2 KB | 208 ms |
| search a sidecar, 2 terms over all columns | 5.3 µs |

## Reproducing

```sh
cargo build --release
forge bench ingest --traces 2000 --spans-per-trace 50 --concurrency 8
forge bench query --traces 20000 --spans-per-trace 50 --big-trace-spans 2000 --iterations 20 --compact
# against a running server
forge bench ingest --target http://127.0.0.1:4318 --rate 20000 --seconds 600 --concurrency 16
```
