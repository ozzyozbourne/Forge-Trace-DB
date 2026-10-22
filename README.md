# Forge Trace DB

Forge is a single Rust binary that ingests agent traces over OpenTelemetry, stores them as
[Vortex](https://github.com/vortex-data/vortex) files in object storage (or a local directory),
and serves trace-tree loads, run lists, full-text search and ad-hoc SQL through an embedded
[Apache DataFusion](https://datafusion.apache.org/) engine. No Kubernetes, ClickHouse or external
Postgres required.

It follows SmithDB's published design: an LSM write path (WAL → memtable → immutable segments →
compaction), object storage as the only durable store, a small embedded metastore (SQLite) and a
custom inverted index built for object storage.

## Quick start

```sh
cargo build --release
./target/release/forge serve --data-dir ./data
curl localhost:9090/healthz
```

See [`forge.toml`](forge.toml) for configuration; every key can be overridden with
`FORGE_<SECTION>_<KEY>` environment variables.

## Workspace

| Crate | Role |
| --- | --- |
| `forge-model` | `spans` Arrow schema, OTLP + GenAI normalization, `dotted_order`, version merge, config, shared traits |
| `forge-meta` | Metastore trait implementation on embedded SQLite (projects, segments, trace index, compaction jobs) |
| `forge-storage` | Object-store construction from URL, Vortex segment write/scan, blobs, cache |
| `forge-index` | Tokenizer, `.fts` sidecar (FST term dictionary + roaring postings), query parser |
| `forge-ingest` | WAL, memtable, flush, compaction, retention, GC |
| `forge-query` | DataFusion table provider, merge-on-read, UDFs, trace-tree / runs / search fast paths |
| `forge-server` | OTLP gRPC/HTTP receivers, JSON API, auth, metrics, process assembly |
| `forge-cli` | The `forge` binary |

## Status

Built milestone by milestone following the design doc (M0 skeleton → M6 release).
