# Forge Trace DB

Forge is a single Rust binary that ingests agent traces over OpenTelemetry, stores them as
[Vortex](https://github.com/vortex-data/vortex) files in object storage (or a local directory),
and serves trace-tree loads, run lists, full-text search and ad-hoc SQL through an embedded
[Apache DataFusion](https://datafusion.apache.org/) engine. No Kubernetes, ClickHouse or external
Postgres required.

It follows SmithDB's published design: an LSM write path (WAL → memtable → immutable segments →
compaction), object storage as the only durable store, a small embedded metastore (SQLite), and a
custom inverted index laid out for object storage.

## Quick start

```sh
cargo build --release
./target/release/forge serve --data-dir ./data
```

Point any OpenTelemetry exporter at it:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_RESOURCE_ATTRIBUTES=forge.project=my-agent
```

Then:

```sh
curl 'localhost:8080/v1/runs?project=my-agent&limit=20'                  # newest runs
curl 'localhost:8080/v1/traces/<trace_id>?project=my-agent'               # full tree, nested JSON
curl -XPOST localhost:8080/v1/search -d '{"project":"my-agent","query":"refund"}' -H 'content-type: application/json'
./target/release/forge sql --server http://localhost:8080 \
  "SELECT model, count(*), sum(cost_usd) FROM spans WHERE run_type = 'chat_model' GROUP BY model"
```

Or generate load and time the query paths: `forge bench query --traces 1000`.

## What it does

- **Ingest**: OTLP/gRPC (4317), OTLP/HTTP protobuf + JSON (4318), and a LangSmith-shaped runs API
  (`POST /v1/runs`, `PATCH /v1/runs/{id}`, `POST /v1/runs/batch`). Understands OTel GenAI
  semantic conventions, OpenInference and OpenLLMetry attributes: run type, model, provider,
  tokens, cost, inputs/outputs, sessions, tags, metadata. Spans can be posted at start and again
  at end; out-of-order and duplicate delivery is fine ("latest non-null wins" per field).
- **Durability**: acknowledged after a group-committed WAL write (local disk or the bucket); a
  `kill -9` crash test checks nothing acknowledged is lost or duplicated.
- **Storage**: Vortex segments + `.fts` index sidecars on `file://`, `s3://`, `gs://`, `az://`,
  behind retries and a memory + disk cache; compaction per project-hour; retention and GC.
- **Query**: trace tree, runs list (cursor-paginated, filters on project, time, type, status,
  latency, tokens, cost, tags, metadata), full-text and JSON-path search, read-only SQL with
  `json_key`, `json_key_search`, `search` and `trace_tree(...)`.
- **Operations**: one binary, one config file (`FORGE_*` env overrides), per-project API keys,
  Prometheus metrics, backpressure (429 / `RESOURCE_EXHAUSTED`), Docker image, static release
  binaries.

## Documentation

- [docs/architecture.md](docs/architecture.md): how it works, crate by crate, and where it
  departs from the original design
- [docs/api.md](docs/api.md): HTTP API and SQL reference
- [docs/operations.md](docs/operations.md): configuration, storage backends, auth, backups,
  lifecycle commands, metrics
- [docs/performance.md](docs/performance.md): measured numbers against the targets

## Workspace

| Crate | Role |
| --- | --- |
| `forge-model` | `spans` Arrow schema, OTLP + GenAI normalization, `dotted_order`, version merge, config, shared traits |
| `forge-meta` | Metastore on embedded SQLite (projects, segments, trace index, compaction jobs) |
| `forge-storage` | Object stores from URLs, retries, memory + disk cache, Vortex segments, blobs, fault injection |
| `forge-index` | Tokenizer, `.fts` sidecar (FST term dictionary + roaring postings), query parser |
| `forge-ingest` | WAL, memtables, flush, compaction, retention, GC |
| `forge-query` | DataFusion table provider, merge-on-read, UDFs, trace-tree / runs / search fast paths |
| `forge-server` | OTLP receivers, JSON API, auth, metrics, process assembly |
| `forge-cli` | The `forge` binary: `serve`, `sql`, `inspect`, `compact`, `gc`, `bench` |

## Development

```sh
cargo test --workspace            # unit, property, golden, crash, differential, fault-injection tests
cargo clippy --workspace --all-targets
cargo bench -p forge-model -p forge-index
FORGE_TEST_S3_ENDPOINT=http://127.0.0.1:5555 cargo test -p forge-server --test s3   # needs moto_server or another S3
```

## Status

All seven milestones of the design (M0–M6) are implemented. Exit criteria that need hardware or
time this environment does not have (100 M-span datasets, a 24-hour soak, numbers on real S3)
are not yet demonstrated; `docs/performance.md` lists what was measured and how.
