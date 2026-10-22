# Forge architecture (as built)

This describes what the code does today, crate by crate, and where it departs from the
original design document and why.

## Process

`forge serve` runs every role as Tokio tasks in one process:

| Task | Port | Code |
| --- | --- | --- |
| OTLP/gRPC `TraceService/Export` (gzip, zstd) | 4317 | `forge-server/src/otlp.rs` |
| OTLP/HTTP `POST /v1/traces` (protobuf, JSON, gzip) | 4318 | `forge-server/src/otlp.rs` |
| JSON API | 8080 | `forge-server/src/api.rs`, `runs.rs`, `search.rs` |
| `/metrics`, `/healthz`, `/readyz` | 9090 | `forge-server/src/lib.rs`, `metrics.rs` |
| Flusher (freeze on size/age, flush, WAL truncate) | | `forge-ingest/src/ingester.rs` |
| Compactor, retention, GC | | `forge-ingest/src/lifecycle.rs` |

On shutdown (Ctrl-C) listeners drain, background tasks stop and the memtables are flushed.
Anything not flushed is still in the WAL and replays on the next start.

Ingest and query share only the memtables, the cache and the metastore, as designed.

## Data model

One table, `spans` (`forge-model/src/schema.rs`), keyed by `(project_id, trace_id, span_id)`,
with a `version` per row. Columns match the design with these choices:

- **Ids.** `trace_id` / `span_id` / `parent_span_id` are `FixedSizeBinary(16|8)` in the logical
  (DataFusion) schema. Vortex 0.89 has no fixed-size binary type, so segments store them as
  `Binary` and the read path casts back (`to_storage` / `from_storage`).
- **Dictionary columns.** `run_type`, `status`, `model`, `provider` are plain `Utf8`; Vortex's
  compressor dictionary-encodes them on disk, which is what the design wanted from
  `dictionary utf8`.
- **`status`.** `NULL` means OTel `unset`. This matters for the merge rule: a later "unset"
  version can never overwrite an earlier "ok"/"error". The API renders `NULL` as `"unset"`.
- **Optional numbers** (`tokens`, `cost_usd`) are `NULL` when absent, for the same reason.
- **Timestamps** use timezone `+00:00` rather than `UTC` so arrow needs no tz database.
- **`version`** is a strictly increasing nanosecond timestamp taken at ingest
  (`max(previous + 1, now)`), and on restart starts above both the WAL and the metastore's
  `max_version`. It orders writes and doubles as ingest time.

### Merge rule

`forge-model/src/merge.rs`: sort by key then `version` desc; for every column independently,
take the value from the newest version where it is non-null; drop keys whose merged `deleted`
is true. Property tests check order independence, idempotence and a reference model.

### Stable columns

`project_id`, `trace_id`, `span_id`, `parent_span_id` and `start_time` are the same on every
version of a span, so filters on them may run *before* the merge (in the Vortex reader). Ingest
guarantees this: OTLP always carries them, and the native `PATCH` fills them from the run's
existing state (memtable, then segments via `trace_index`). Every other filter runs after the
merge.

### Partitioning

A row's partition is `(project_id, hour(start_time))` (or the hour of its version when it has no
start time). Because `start_time` is stable, all versions of a span land in one partition,
which is what lets compaction apply upserts and drop tombstones partition by partition.

## Write path (`forge-ingest`)

1. **Normalize** (`forge-model/src/otlp.rs`, `genai.rs`): OTel GenAI semantic conventions first,
   then OpenInference, OpenLLMetry/Traceloop and LangSmith attribute names. Payload attributes
   (`gen_ai.input.messages`, `llm.input_messages.*`, `gen_ai.prompt.*`, `input.value`,
   `traceloop.entity.input`, GenAI message events, ...) become `inputs`/`outputs` and are removed
   from the residual `attributes`. Cost comes from a longest-prefix pricing table
   (`pricing.rs`), overridable in `forge.toml`.
2. **Blobs**: payload values over `ingest.blob_threshold` (1 MiB) are stored at
   `blobs/<project>/<sha256>`; the column holds `{"$forge_blob": "<sha256>", "size": N}` and the
   trace API resolves it.
3. **`dotted_order`**: filled from parents in the same request or from a byte-bounded cache of
   recently seen spans; otherwise left for compaction.
4. **WAL** (`wal.rs`): crc-framed Arrow IPC records, group commit (10 ms / 1 MiB), `fsync`
   (`sync_data`) before acknowledging. Local mode writes `<dir>/<seq>.wal`, rotated per memtable;
   `object_store` mode writes one object per group commit at `wal/<node_id>/<seq>.wal`. Replay
   stops at a torn tail.
5. **Memtable** (`memtable.rs`): immutable Arrow batches plus a `(project, trace)` index. A
   shared/exclusive rotation lock spans WAL append + memtable insert vs. freeze, so every WAL
   record lands in the memtable whose WAL boundary covers it.
6. **Flush**: freeze at `flush_bytes` or `flush_interval`; merge versions (keeping tombstones),
   split by partition, sort by `(trace_id, dotted_order, start_time, span_id)`, write the Vortex
   segment and build its `.fts` sidecar in parallel, commit all segments and their
   `trace_index` rows in one metastore transaction, truncate the WAL, drop the memtable.
7. **Backpressure**: above `memtable.memory_budget`, OTLP/HTTP returns 429 with `Retry-After`,
   gRPC returns `RESOURCE_EXHAUSTED`.

Crash safety is tested by `forge-cli/tests/crash.rs`, which `kill -9`s a real server six
times while it ingests and flushes every ~100 ms, and checks every acknowledged span is
present exactly once after recovery.

## Storage (`forge-storage`)

- `store::from_url`: `file://`, `memory://`, `s3://`, `gs://`, `az://` (and the other schemes
  `object_store` parses), with `[storage.options]` passed to the cloud builders.
- `layer()` wraps the store in `RetryStore` (transient errors, exponential backoff, bodies
  buffered so mid-stream failures retry) and `CachingStore`:
  - memory: LRU of byte ranges keyed by `(path, start, end)`;
  - disk: whole immutable objects (`segments/`, `blobs/`), LRU-evicted; objects written through
    the store are copied to disk at write time, and objects first read by range are downloaded
    in the background.
- Segments are Vortex files written with Vortex's default write strategy (BtrBlocks compressor,
  zone maps). Scans push down projection, the stable-column predicate (as a Vortex expression)
  and a roaring row selection.

The design suggested foyer for the cache; the cache here is ~300 lines on `moka` and the
filesystem, which was enough for the access pattern (immutable objects, range reads).

## Metastore (`forge-meta`)

Embedded SQLite (WAL journal, `synchronous=FULL`), tables `projects`, `segments`,
`trace_index`, `compaction_jobs`, `schema_versions` with ordered migrations. The `Metastore`
trait is the seam for a PostgreSQL implementation; that implementation is not written (the
design lists it for later multi-node setups) and `postgres://` URLs are rejected with a clear
error.

## Query (`forge-query`)

- `Reader` (`reader.rs`) is the shared read path. It always takes the memtable snapshot
  *before* listing segments; flush commits segments before dropping the memtable, so a read can
  see a row twice (merged away) but never miss it.
- `SpansTableProvider` (`provider.rs`): filters on stable columns become a `ScanPredicate`
  (project, trace ids, span ids, start-time range, roots only) that prunes segments in the
  metastore and runs inside the Vortex reader. All filters are reported `Inexact`, so DataFusion
  re-applies them above the merge. Plan:
  `MergeVersionsExec → SpansScanExec` (one partition per segment plus one for the memtables).
  The merge is skipped when the inputs are compacted L1 segments with disjoint trace ranges and
  no memtable rows. `EXPLAIN` prints which columns are read; tests assert list-shaped plans read
  no payload columns.
- UDFs (`udf.rs`): `json_key`, `json_key_search`, `search`, and the `trace_tree` table function.
  `search`/`json_key_search` conjuncts are turned into index lookups by the scan (L1 segments are
  narrowed to candidate rows; segments whose dictionary lacks a term are skipped); the functions
  then evaluate exactly.
- SQL sessions are read-only (`SQLOptions` without DDL/DML/statements), with a memory pool of
  `query.memory_limit` and a `query.timeout` deadline. Project keys only ever see their project.
- Fast paths (`fastpath.rs`, `search.rs`):
  - **trace tree**: `trace_index` lookup → read that trace's rows from its segments (predicate
    on `trace_id`, pruned by zone maps since segments are sorted by trace) + memtable → merge →
    depth-first order → nested JSON with blobs resolved;
  - **runs list**: newest partition hour first, roots only, no payload columns, filters after
    the merge, stops once `limit` rows are found; cursor = `(start_time, trace_id, span_id)`;
  - **search**: per hour, newest first: sidecar lookups give candidate rows, their keys and start
    times are read, then candidates are verified newest first in chunks by re-reading all
    versions of those keys, merging, and re-checking the query, so a match on a superseded
    version never leaks through.

Differential tests (`forge-query/tests/query.rs`) run the runs list and search through the fast
paths and through plain SQL across pages, partitions, segments and memtables, and require
identical results.

## Full-text index (`forge-index`)

- Tokenizer: Unicode word segmentation (UAX #29), lowercase, ASCII folding, tokens over 64
  bytes dropped, 1 MiB of text per field per span. The design asked to reuse tantivy's tokenizer
  crate; tantivy's tokenizers live in the full `tantivy` crate, so this reimplements the same
  three-step pipeline on `unicode-segmentation` (about 50 lines).
- Terms: `<column>\0<token>` and, for JSON columns, `<column>\0<json path>\0<token>` with array
  indices normalized to `[*]`, so `search` and `json_key_search` share one index.
- `.fts` layout: postings (one serialized `RoaringBitmap` per term), then an FST mapping term →
  `(offset << 24 | length)`, then a 44-byte footer. A lookup costs one suffix read for footer +
  dictionary (cached afterwards) and one coalesced read for the postings. Roaring's 64K-row
  containers stand in for the design's explicit 64K row groups. Positions (phrase queries) are
  not stored, as in the design's v1.
- Build is parallel over row chunks; flushes build the sidecar while the segment uploads.

## Compaction, retention, GC (`forge-ingest/src/lifecycle.rs`)

The design said "merge L0 into L1 per (project, hour)". Doing that literally for the open hour
means rewriting the whole hour every round, which in a soak test blew memory up to 11 GB. As
built:

- **Open hour**: once it has `min_l0_segments` L0 segments, those L0s are merged into a new L1
  *run*. Runs are merged size-tiered: four or more runs within a factor of four in size become
  one. Runs of one partition may overlap, so reads merge them (`needs_merge`), and a partial
  merge keeps tombstones when older versions may live outside it.
- **Closed hour** (ended more than 5 minutes ago) with any L0 or overlapping runs, or any
  partition under `forge compact --project`: rewritten into L1 segments with disjoint trace
  ranges, upserts applied and tombstones dropped.
- Every merge works through the partition in trace-id ranges bounded by both
  `compaction.target_segment` and `compaction.max_rows_per_merge`, so memory is bounded by one
  merge step, not by the hour.
- Missing `dotted_order` is backfilled, including ancestors in other hours (found through
  `trace_index`).
- A lease in `compaction_jobs` serializes compactors per partition; outputs and the deletion of
  inputs commit in one transaction. Deleted segments stay readable for `compaction.gc_grace`.
- Retention marks partitions past a project's `retention_days` deleted; GC deletes the objects
  of deleted segments after the grace period, objects no metastore row names (orphans from a
  crash between upload and commit) after `compaction.orphan_age`, and blobs older than their
  project's retention (blobs are rewritten on every ingest that references them, so their
  modification time tracks their newest reference).

## Native runs API

`POST /v1/runs`, `PATCH /v1/runs/{id}`, `POST /v1/runs/batch` accept LangSmith-shaped runs.
Run UUIDs map to `trace_id` directly (a root run's trace id is its own id); span ids are the
first 8 bytes of SHA-256 of the run UUID, since OTel span ids are 8 bytes. The original run id
is kept in `attributes["langsmith.run_id"]`. Patches fill identity fields and, when they report
token usage, compute cost with the model of the run they update.

## Not built, or built differently

- PostgreSQL metastore: trait only (see above).
- `refs/` clones: agents read the exact sources from `~/.cargo/registry` (same versions as
  compiled); `scripts/clone-refs.sh` clones the six core repositories at the pinned versions for
  anyone who wants them.
- Golden fixtures are hand-written in the attribute shapes OpenLLMetry, OpenInference and the
  GenAI conventions emit, not captured from live LangGraph runs (no model traffic was available
  in the build environment).
- MinIO for CI: MinIO's community binaries are no longer published, so the S3 test runs against
  `moto_server`; it exercises the real `object_store` S3 client.
