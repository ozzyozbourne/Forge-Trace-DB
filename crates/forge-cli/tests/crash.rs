//! Crash tests: `kill -9` the server while it ingests and flushes constantly (so kills land
//! during WAL writes, segment uploads and metastore commits), restart, and check that every
//! acknowledged span is present exactly once.

use std::collections::HashSet;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use forge_model::config::Config;
use forge_model::merge::{MergeOptions, merge_batches};
use forge_model::opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use forge_model::opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use forge_model::opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use forge_model::row::batch_to_rows;
use forge_model::schema::spans_schema;
use forge_model::traits::{ScanRequest, SegmentFilter, SegmentStoreRef};
use prost::Message;
use rand::Rng;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Server {
    child: Child,
    http: u16,
}

fn start(dir: &Path) -> Server {
    let (grpc, http, api, ops) = (free_port(), free_port(), free_port(), free_port());
    let child = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["serve", "--data-dir"])
        .arg(dir)
        .env("FORGE_SERVER_OTLP_GRPC_ADDR", format!("127.0.0.1:{grpc}"))
        .env("FORGE_SERVER_OTLP_HTTP_ADDR", format!("127.0.0.1:{http}"))
        .env("FORGE_SERVER_API_ADDR", format!("127.0.0.1:{api}"))
        .env("FORGE_SERVER_METRICS_ADDR", format!("127.0.0.1:{ops}"))
        .env("FORGE_MEMTABLE_FLUSH_BYTES", "32KiB")
        .env("FORGE_MEMTABLE_FLUSH_INTERVAL", "100ms")
        .env("FORGE_COMPACTION_ENABLED", "false")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn forge");
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::net::TcpStream::connect(("127.0.0.1", ops)).is_err() {
        assert!(Instant::now() < deadline, "server did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
    Server { child, http }
}

fn request(batch: u64, spans: u64) -> ExportTraceServiceRequest {
    let mut trace_id = [0u8; 16];
    trace_id[..8].copy_from_slice(&batch.to_be_bytes());
    trace_id[15] = 1;
    let spans = (0..spans)
        .map(|i| {
            let mut span_id = [0u8; 8];
            span_id[..4].copy_from_slice(&(batch as u32).to_be_bytes());
            span_id[4..].copy_from_slice(&(i as u32 + 1).to_be_bytes());
            Span {
                trace_id: trace_id.to_vec(),
                span_id: span_id.to_vec(),
                parent_span_id: vec![],
                name: format!("span {batch}/{i}"),
                start_time_unix_nano: 1_761_148_800_000_000_000 + batch * 1_000_000 + i,
                end_time_unix_nano: 1_761_148_800_000_000_000 + batch * 1_000_000 + i + 10,
                attributes: vec![KeyValue {
                    key: "input.value".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("x".repeat(500))),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Every distinct (trace, span) key visible after recovery, and whether any appear twice
/// after the version merge.
async fn recovered_keys(dir: &Path) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let config = Config::default().with_data_dir(dir);
    let meta = forge_meta::open(&config.metastore.url).unwrap();
    let store = forge_storage::from_url(&config.storage.url, &config.storage.options).unwrap();
    let segs: SegmentStoreRef = Arc::new(forge_storage::VortexSegmentStore::new(store.clone()));
    let ing = forge_ingest::Ingester::open(config, meta.clone(), segs.clone(), store)
        .await
        .unwrap();
    let mut batches = ing.snapshot().scan(&Default::default()).unwrap();
    for s in meta.list_segments(&SegmentFilter::default()).await.unwrap() {
        let stream = segs.scan_segment(&s, ScanRequest::default()).await.unwrap();
        batches.extend(forge_storage::segment::collect(stream).await.unwrap());
    }
    let merged = merge_batches(&spans_schema(), &batches, MergeOptions::default()).unwrap();
    let rows = batch_to_rows(&merged);
    let keys: HashSet<_> = rows
        .iter()
        .map(|r| (r.trace_id.to_vec(), r.span_id.to_vec()))
        .collect();
    assert_eq!(keys.len(), rows.len(), "duplicate spans after merge");
    keys
}

#[test]
fn kill_dash_nine_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut acked: HashSet<(Vec<u8>, Vec<u8>)> = HashSet::new();
    let mut batch = 0u64;
    let mut rng = rand::rng();
    for round in 0..6 {
        let mut server = start(dir.path());
        let kill_at = Instant::now() + Duration::from_millis(rng.random_range(200..1200));
        let url = format!("http://127.0.0.1:{}/v1/traces", server.http);
        // Ingest from a background thread so the kill lands mid-request.
        let (tx, rx) = std::sync::mpsc::channel();
        let c = client.clone();
        let start_batch = batch;
        let sender = std::thread::spawn(move || {
            let mut b = start_batch;
            loop {
                let req = request(b, 20);
                let res = c
                    .post(&url)
                    .header("content-type", "application/x-protobuf")
                    .body(req.encode_to_vec())
                    .send();
                match res {
                    Ok(r) if r.status().is_success() => {
                        if tx.send(req).is_err() {
                            return b;
                        }
                    }
                    Ok(r) if r.status().as_u16() == 429 => {}
                    _ => return b,
                }
                b += 1;
            }
        });
        while Instant::now() < kill_at {
            std::thread::sleep(Duration::from_millis(10));
        }
        server.child.kill().unwrap(); // SIGKILL
        server.child.wait().unwrap();
        batch = sender.join().unwrap() + 1;
        for req in rx.try_iter() {
            for s in &req.resource_spans[0].scope_spans[0].spans {
                acked.insert((s.trace_id.clone(), s.span_id.clone()));
            }
        }
        let recovered = rt.block_on(recovered_keys(dir.path()));
        let missing: Vec<_> = acked.difference(&recovered).collect();
        assert!(
            missing.is_empty(),
            "round {round}: {} acknowledged spans lost",
            missing.len()
        );
        eprintln!(
            "round {round}: {} acked, {} recovered",
            acked.len(),
            recovered.len()
        );
    }
    assert!(
        acked.len() > 100,
        "test ingested too little to be meaningful"
    );
}
