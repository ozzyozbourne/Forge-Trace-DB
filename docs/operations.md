# Operating Forge

## Running

```sh
forge serve --config /etc/forge/forge.toml      # or: forge serve --data-dir /var/lib/forge
```

`--data-dir DIR` points storage (`file://DIR/data`), the metastore (`DIR/meta.db`), the WAL
(`DIR/wal`) and the disk cache (`DIR/cache`) at one directory, which is convenient on a laptop.

Docker:

```sh
docker build -t forge .
docker run -p 4317:4317 -p 4318:4318 -p 8080:8080 -p 9090:9090 \
  -v forge:/var/lib/forge forge
```

Static release binaries (Linux x86-64 and arm64 musl, macOS arm64) and a `ghcr.io` image are
built by `.github/workflows/release.yml` when a `v*` tag is pushed.

## Sending traces

Any OTLP exporter works. With the OpenTelemetry SDK environment variables:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://forge:4318        # or grpc://forge:4317
export OTEL_EXPORTER_OTLP_HEADERS="x-api-key=fk_..."        # when auth is enabled
export OTEL_RESOURCE_ATTRIBUTES="forge.project=my-agent"    # when auth is disabled
```

LangSmith-style clients can post runs to `POST /v1/runs` / `POST /v1/runs/batch` on port 8080.

## Configuration

Every key in `forge.toml` (see the annotated example at the repository root) can be set with
`FORGE_<SECTION>_<KEY>`, for example `FORGE_STORAGE_URL`, `FORGE_MEMTABLE_FLUSH_BYTES=128MiB`,
`FORGE_AUTH_ENABLED=true`. Sizes accept `KiB/MiB/GiB` (and `KB/MB/GB`); durations accept
`ms/s/m/h/d`.

| Section | Key | Default | Notes |
| --- | --- | --- | --- |
| server | otlp_grpc_addr / otlp_http_addr / api_addr / metrics_addr | 4317 / 4318 / 8080 / 9090 | |
| server | node_id | node-0 | names the object-store WAL prefix |
| storage | url | file:///var/lib/forge/data | `s3://`, `gs://`, `az://`, `memory://` |
| storage | options | {} | passed to the cloud builder, e.g. `aws_endpoint`, `aws_region` |
| metastore | url | sqlite:///var/lib/forge/meta.db | |
| wal | mode | local | `object_store` for disk-less deployments (adds a PUT per group commit) |
| wal | dir / commit_interval / commit_bytes | /var/lib/forge/wal / 10ms / 1MiB | |
| memtable | flush_bytes / flush_interval | 64MiB / 30s | freeze triggers |
| memtable | memory_budget | 1GiB | above it, ingest returns 429 / RESOURCE_EXHAUSTED |
| cache | memory / disk / disk_dir | 512MiB / 10GiB / /var/lib/forge/cache | set both to 0 to disable |
| compaction | enabled / interval | true / 60s | |
| compaction | min_l0_segments | 8 | open-hour L0 merge trigger |
| compaction | target_segment / max_rows_per_merge | 256MiB / 200000 | bound each merge step |
| compaction | gc_grace / orphan_age | 5m / 24h | |
| query | memory_limit / timeout | 2GiB / 30s | per SQL query |
| auth | enabled / admin_key | false / none | `admin_key` is required when enabled |
| ingest | max_request / blob_threshold / default_project | 64MiB / 1MiB / default | |
| pricing | `"<model prefix>" = { input = .., output = .. }` | built-in table | USD per million tokens |

### Object storage

S3 (and S3-compatible stores):

```toml
[storage]
url = "s3://my-bucket/forge"
[storage.options]
aws_region = "us-east-1"
# aws_endpoint = "http://minio:9000"   # S3-compatible endpoints
# aws_allow_http = "true"
```

Credentials come from the usual AWS environment variables or instance roles. GCS (`gs://`)
and Azure (`az://`) use the equivalent `google_*` / `azure_*` options and environment.

## Authentication

With `auth.enabled = true`:

- the admin key (`auth.admin_key`) can create projects, rotate keys, and query across projects;
- `POST /v1/projects {"id": "team-a", "retention_days": 30}` returns the project's API key once;
  only its SHA-256 is stored;
- project keys are sent as `x-api-key: <key>` or `Authorization: Bearer <key>`; writes made with
  a project key always land in that project, and its SQL sees only that project;
- `POST /v1/projects/{id}/rotate-key` issues a new key and invalidates the old one.

With auth disabled (the default, for local use) any request is accepted, and the project comes
from the `x-forge-project` header, the `forge.project` / `openinference.project.name` /
`langsmith.project` resource attribute, or `ingest.default_project`.

## Durability and backups

Durable state is exactly:

1. objects under the storage URL (`segments/`, `blobs/`, and `wal/` in `object_store` WAL mode);
2. the metastore file.

Local disk otherwise holds only the WAL (local mode) and the cache. Losing the cache loses
nothing. Losing the local WAL loses at most the spans acknowledged since the last flush.

To back up: snapshot the metastore with `sqlite3 meta.db ".backup meta-backup.db"` (safe while
running), and rely on bucket versioning or replication for objects. Objects are immutable and
only deleted by GC after `gc_grace`, so a metastore backup taken while running stays consistent
with the bucket as long as it is restored within that window, or GC's orphan pass is disabled
(`orphan_age` large) until it is.

## Lifecycle

- `forge compact [--project P]` runs compaction now (all due partitions, or every partition of
  `P`); the server also runs it every `compaction.interval`.
- `forge gc` applies retention and removes deleted segments past the grace period, orphans and
  expired blobs.
- `forge inspect segments [--project P] [--all]` lists segments;
  `forge inspect segment <path>` prints a segment's footer, statistics, metastore row and index
  summary.
- `forge sql "SELECT ..." [--server http://host:8080] [--project P]` runs read-only SQL against
  a server, or offline against the data directory (flushed data only).

These commands can run next to a live server: the metastore is SQLite in WAL mode and
compaction takes a lease per partition.

## Monitoring

`GET :9090/metrics` (Prometheus text format):

| Metric | Meaning |
| --- | --- |
| `forge_ingest_spans_total`, `forge_ingest_requests_total` | accepted span versions / batches |
| `forge_ingest_rejected_total` | batches rejected by backpressure; sustained growth means flush cannot keep up |
| `forge_memtable_bytes` vs `forge_memtable_budget_bytes` | buffered, unflushed data |
| `forge_flushes_total`, `forge_flush_errors_total` | flush attempts; errors retry with backoff |
| `forge_segments{level}`, `forge_segment_bytes{level}`, `forge_segment_rows` | live data; L0 count should stay bounded |
| `forge_cache_requests_total{result}`, `forge_object_store_read_bytes_total` | cache effectiveness |
| `forge_http_requests_total{listener,route,status}`, `forge_http_request_duration_seconds` | API and OTLP/HTTP traffic |

`/healthz` answers once the process is up; `/readyz` once all listeners are bound (and returns
503 while shutting down).

Logs go to stderr; set `RUST_LOG` (e.g. `RUST_LOG=info,forge_ingest=debug`) for detail. The
flusher logs per-segment timings at debug level.

## Sizing notes

Measured on a 4 vCPU / 15 GiB VM with local disk (see `docs/performance.md`):

- flush throughput is dominated by building the `.fts` sidecar; it runs in parallel with the
  segment upload and uses all cores;
- under sustained ingest, RSS is roughly `memory_budget` + cache memory + one merge step;
- the open hour holds a handful of overlapping L1 runs plus recent L0s until it closes, then
  compacts into disjoint segments.
