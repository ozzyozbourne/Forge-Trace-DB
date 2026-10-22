//! `forge bench`: a load generator for agent-shaped traces, and query timings.
//!
//! Traces have configurable depth and fan-out, LLM/tool/retriever spans with GenAI attributes,
//! payloads around a median size (optionally with base64 "image" blobs), spans that stay open
//! and are closed by a later upsert, and out-of-order and duplicate delivery.

use std::sync::Arc;
use std::time::{Duration, Instant};

use forge_model::opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use forge_model::opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use forge_model::opentelemetry_proto::tonic::resource::v1::Resource;
use forge_model::opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status, status,
};
use forge_query::fastpath::{self, RunFilter};
use forge_server::Forge;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone, clap::Args)]
pub struct GenOptions {
    /// Number of traces.
    #[arg(long, default_value_t = 1000)]
    pub traces: usize,
    /// Spans per trace.
    #[arg(long, default_value_t = 50)]
    pub spans_per_trace: usize,
    /// Maximum tree depth.
    #[arg(long, default_value_t = 8)]
    pub depth: usize,
    /// Children per node before going deeper.
    #[arg(long, default_value_t = 4)]
    pub fanout: usize,
    /// Median payload bytes per span (inputs + outputs).
    #[arg(long, default_value_t = 2048)]
    pub payload_bytes: usize,
    /// Fraction of LLM spans carrying a base64 image of this many bytes (0 = none).
    #[arg(long, default_value_t = 0)]
    pub image_bytes: usize,
    /// Fraction of spans posted at start and closed by a separate end upsert.
    #[arg(long, default_value_t = 0.2)]
    pub open_fraction: f64,
    /// Fraction of spans delivered twice.
    #[arg(long, default_value_t = 0.02)]
    pub duplicate_fraction: f64,
    /// Spans per OTLP request.
    #[arg(long, default_value_t = 500)]
    pub batch: usize,
    #[arg(long, default_value = "bench")]
    pub project: String,
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
}

fn kv(k: &str, v: any_value::Value) -> KeyValue {
    KeyValue {
        key: k.into(),
        value: Some(AnyValue { value: Some(v) }),
        ..Default::default()
    }
}

fn s(v: impl Into<String>) -> any_value::Value {
    any_value::Value::StringValue(v.into())
}

const WORDS: &[&str] = &[
    "refund",
    "order",
    "shipping",
    "invoice",
    "customer",
    "agent",
    "policy",
    "warranty",
    "account",
    "password",
    "delivery",
    "tracking",
    "payment",
    "subscription",
    "cancel",
    "upgrade",
    "billing",
    "support",
    "ticket",
    "escalate",
    "lisbon",
    "flight",
    "hotel",
    "weather",
    "summary",
    "report",
    "analysis",
    "database",
    "query",
    "timeout",
];

fn text(rng: &mut StdRng, bytes: usize) -> String {
    let mut out = String::with_capacity(bytes + 16);
    while out.len() < bytes {
        out.push_str(WORDS[rng.random_range(0..WORDS.len())]);
        out.push(' ');
        if rng.random_bool(0.05) {
            out.push_str(&format!("#{} ", rng.random_range(1000..99999)));
        }
    }
    out
}

/// Generate one trace's spans, in delivery order (shuffled, with duplicates and end upserts).
pub fn gen_trace(o: &GenOptions, rng: &mut StdRng, trace_no: u64, t0: u64) -> Vec<Span> {
    let mut trace_id = [0u8; 16];
    rng.fill(&mut trace_id);
    trace_id[..8].copy_from_slice(&trace_no.to_be_bytes());
    let n = o.spans_per_trace.max(1);
    // Assign parents: breadth-first with fan-out, capped depth.
    let mut parent: Vec<Option<usize>> = vec![None];
    let mut depth = vec![0usize];
    let mut frontier = 0usize;
    while parent.len() < n {
        if depth[frontier] + 1 >= o.depth {
            frontier = rng.random_range(0..parent.len());
            continue;
        }
        for _ in 0..o.fanout {
            if parent.len() >= n {
                break;
            }
            parent.push(Some(frontier));
            depth.push(depth[frontier] + 1);
        }
        frontier += 1;
        if frontier >= parent.len() {
            frontier = 0;
        }
    }
    let span_ids: Vec<[u8; 8]> = (0..n)
        .map(|i| {
            let mut id = [0u8; 8];
            id[..4].copy_from_slice(&(trace_no as u32).to_be_bytes());
            id[4..].copy_from_slice(&(i as u32 + 1).to_be_bytes());
            id
        })
        .collect();
    let mut out = Vec::with_capacity(n + n / 4);
    let mut start = t0;
    for i in 0..n {
        start += rng.random_range(1_000..2_000_000);
        let dur = rng.random_range(1_000_000u64..3_000_000_000);
        let kind = if i == 0 { 0 } else { rng.random_range(0..10) };
        let payload = (o.payload_bytes as f64 * rng.random_range(0.3..1.7)) as usize;
        let mut attrs = Vec::new();
        let name;
        match kind {
            0 => {
                name = "agent".to_string();
                attrs.push(kv("gen_ai.operation.name", s("invoke_agent")));
                attrs.push(kv(
                    "gen_ai.conversation.id",
                    s(format!("conv-{}", trace_no % 997)),
                ));
                attrs.push(kv(
                    "input.value",
                    s(format!("{{\"question\":\"{}\"}}", text(rng, payload / 2))),
                ));
            }
            1..=4 => {
                let model = [
                    "gpt-4o",
                    "gpt-4o-mini",
                    "claude-sonnet-4",
                    "gemini-2.5-flash",
                ][rng.random_range(0..4)];
                name = format!("chat {model}");
                attrs.push(kv("gen_ai.operation.name", s("chat")));
                attrs.push(kv("gen_ai.request.model", s(model)));
                attrs.push(kv(
                    "gen_ai.usage.input_tokens",
                    any_value::Value::IntValue(rng.random_range(10..4000)),
                ));
                attrs.push(kv(
                    "gen_ai.usage.output_tokens",
                    any_value::Value::IntValue(rng.random_range(1..800)),
                ));
                let mut content = text(rng, payload / 2);
                if o.image_bytes > 0 && rng.random_bool(0.1) {
                    let blob: String = (0..o.image_bytes)
                        .map(|_| {
                            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
                                [rng.random_range(0..64)] as char
                        })
                        .collect();
                    content.push_str(&format!("data:image/png;base64,{blob}"));
                }
                attrs.push(kv(
                    "gen_ai.input.messages",
                    s(serde_json::json!([{"role": "user", "parts": [{"type": "text", "content": content}]}]).to_string()),
                ));
                attrs.push(kv(
                    "gen_ai.output.messages",
                    s(serde_json::json!([{"role": "assistant", "parts": [{"type": "text", "content": text(rng, payload / 2)}]}]).to_string()),
                ));
            }
            5..=7 => {
                name = format!("execute_tool {}", WORDS[rng.random_range(0..WORDS.len())]);
                attrs.push(kv("gen_ai.operation.name", s("execute_tool")));
                attrs.push(kv(
                    "gen_ai.tool.call.arguments",
                    s(format!("{{\"q\":\"{}\"}}", text(rng, payload / 3))),
                ));
                attrs.push(kv(
                    "gen_ai.tool.call.result",
                    s(format!("{{\"r\":\"{}\"}}", text(rng, payload / 3))),
                ));
            }
            8 => {
                name = "retrieve".to_string();
                attrs.push(kv("openinference.span.kind", s("RETRIEVER")));
                attrs.push(kv("input.value", s(text(rng, payload / 2))));
                attrs.push(kv("output.value", s(text(rng, payload / 2))));
            }
            _ => {
                name = "step".to_string();
                attrs.push(kv("traceloop.span.kind", s("task")));
            }
        }
        let failed = rng.random_bool(0.03);
        let span = Span {
            trace_id: trace_id.to_vec(),
            span_id: span_ids[i].to_vec(),
            parent_span_id: parent[i].map(|p| span_ids[p].to_vec()).unwrap_or_default(),
            name,
            start_time_unix_nano: start,
            end_time_unix_nano: start + dur,
            attributes: attrs,
            status: Some(Status {
                code: if failed {
                    status::StatusCode::Error
                } else {
                    status::StatusCode::Ok
                } as i32,
                message: if failed {
                    "upstream timeout".into()
                } else {
                    String::new()
                },
            }),
            ..Default::default()
        };
        if rng.random_bool(o.open_fraction) {
            // Start post without end, then an end post.
            let mut open = span.clone();
            open.end_time_unix_nano = 0;
            open.status = None;
            out.push(open);
            let mut end = span.clone();
            end.attributes.clear();
            out.push(end);
        } else {
            out.push(span.clone());
        }
        if rng.random_bool(o.duplicate_fraction) {
            out.push(span);
        }
    }
    // Out-of-order delivery within a window.
    for chunk in out.chunks_mut(16) {
        chunk.shuffle(rng);
    }
    out
}

pub fn requests(o: &GenOptions, spans: Vec<Span>) -> Vec<ExportTraceServiceRequest> {
    spans
        .chunks(o.batch.max(1))
        .map(|c| ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![
                        kv("forge.project", s(o.project.clone())),
                        kv("service.name", s("bench-agent")),
                    ],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: c.to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        })
        .collect()
}

pub fn now_ns() -> u64 {
    forge_model::now_ns() as u64
}

/// Send generated traces to a running server's OTLP/HTTP endpoint, optionally rate-limited.
pub async fn ingest_http(
    target: &str,
    o: &GenOptions,
    concurrency: usize,
    rate: Option<f64>,
    duration: Option<Duration>,
) -> anyhow::Result<(u64, Duration)> {
    use prost::Message;
    let started = Instant::now();
    let url = format!("{}/v1/traces", target.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let (tx, rx) = async_channel(concurrency * 4);
    let o2 = o.clone();
    let deadline = duration.map(|d| started + d);
    let producer = tokio::task::spawn_blocking(move || {
        let mut rng = StdRng::seed_from_u64(o2.seed);
        let mut t = 0u64;
        loop {
            if deadline.is_none() && t as usize >= o2.traces {
                return;
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return;
            }
            let spans = gen_trace(&o2, &mut rng, t, now_ns() - 60_000_000_000);
            for r in requests(&o2, spans) {
                if tx.send_blocking(r).is_err() {
                    return;
                }
            }
            t += 1;
        }
    });
    let sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let rx = rx.clone();
        let client = client.clone();
        let url = url.clone();
        let sent = sent.clone();
        workers.push(tokio::spawn(async move {
            while let Ok(req) = rx.recv().await {
                let n = req.resource_spans[0].scope_spans[0].spans.len() as u64;
                if let Some(rate) = rate {
                    // Pace globally: wait until this batch is due.
                    let due = sent.load(std::sync::atomic::Ordering::Relaxed) as f64 / rate;
                    let now = started.elapsed().as_secs_f64();
                    if due > now {
                        tokio::time::sleep(Duration::from_secs_f64(due - now)).await;
                    }
                }
                let body = req.encode_to_vec();
                loop {
                    let r = client
                        .post(&url)
                        .header("content-type", "application/x-protobuf")
                        .body(body.clone())
                        .send()
                        .await?;
                    if r.status().as_u16() == 429 {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    anyhow::ensure!(r.status().is_success(), "server returned {}", r.status());
                    break;
                }
                sent.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    drop(rx);
    producer.await?;
    for w in workers {
        w.await??;
    }
    Ok((
        sent.load(std::sync::atomic::Ordering::Relaxed),
        started.elapsed(),
    ))
}

/// Ingest in-process through the full normalize → WAL → memtable path.
pub async fn ingest(
    forge: &Arc<Forge>,
    o: &GenOptions,
    concurrency: usize,
) -> anyhow::Result<(u64, Duration)> {
    let started = Instant::now();
    let t0 = now_ns() - 3_600_000_000_000;
    let (tx, rx) = async_channel(concurrency * 2);
    let producer = {
        let o = o.clone();
        tokio::task::spawn_blocking(move || {
            let mut rng = StdRng::seed_from_u64(o.seed);
            for t in 0..o.traces {
                let spans = gen_trace(&o, &mut rng, t as u64, t0 + t as u64 * 1_000_000);
                for r in requests(&o, spans) {
                    if tx.send_blocking(r).is_err() {
                        return;
                    }
                }
            }
        })
    };
    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let rx = rx.clone();
        let f = forge.clone();
        workers.push(tokio::spawn(async move {
            let mut n = 0u64;
            while let Ok(req) = rx.recv().await {
                loop {
                    match f.ingest_otlp(&req, None).await {
                        Ok(k) => {
                            n += k as u64;
                            break;
                        }
                        Err(forge_model::ForgeError::Backpressure(_)) => {
                            tokio::time::sleep(Duration::from_millis(20)).await
                        }
                        Err(e) => return Err(anyhow::anyhow!(e)),
                    }
                }
            }
            Ok::<u64, anyhow::Error>(n)
        }));
    }
    drop(rx);
    producer.await?;
    let mut total = 0;
    for w in workers {
        total += w.await??;
    }
    Ok((total, started.elapsed()))
}

/// Minimal MPMC channel on top of tokio primitives.
fn async_channel(cap: usize) -> (Sender, Receiver) {
    let (tx, rx) = tokio::sync::mpsc::channel(cap);
    (Sender(tx), Receiver(Arc::new(tokio::sync::Mutex::new(rx))))
}

struct Sender(tokio::sync::mpsc::Sender<ExportTraceServiceRequest>);
impl Sender {
    fn send_blocking(&self, r: ExportTraceServiceRequest) -> Result<(), ()> {
        self.0.blocking_send(r).map_err(|_| ())
    }
}

#[derive(Clone)]
struct Receiver(Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<ExportTraceServiceRequest>>>);
impl Receiver {
    async fn recv(&self) -> Result<ExportTraceServiceRequest, ()> {
        self.0.lock().await.recv().await.ok_or(())
    }
}

pub struct Timing {
    pub p50: Duration,
    pub p99: Duration,
    pub n: usize,
}

impl std::fmt::Display for Timing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "p50 {:>8.2} ms   p99 {:>8.2} ms   (n={})",
            self.p50.as_secs_f64() * 1e3,
            self.p99.as_secs_f64() * 1e3,
            self.n
        )
    }
}

pub fn timing(mut samples: Vec<Duration>) -> Timing {
    samples.sort();
    let n = samples.len();
    let at = |q: f64| samples[((n as f64 - 1.0) * q).round() as usize];
    Timing {
        p50: at(0.5),
        p99: at(0.99),
        n,
    }
}

pub async fn time<F, Fut, T>(iters: usize, mut f: F) -> anyhow::Result<Timing>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = forge_model::Result<T>>,
{
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f().await?;
        samples.push(t.elapsed());
    }
    Ok(timing(samples))
}

/// Query benchmarks against data already in `forge`.
pub async fn queries(
    forge: &Arc<Forge>,
    project: &str,
    big_trace: Option<[u8; 16]>,
    iters: usize,
) -> anyhow::Result<()> {
    let reader = &forge.query.reader;
    if let Some(t) = big_trace {
        let r = time(iters, || fastpath::trace_tree(reader, project, &t, true)).await?;
        println!("trace tree load (with payloads)      {r}");
        let r = time(iters, || fastpath::trace_tree(reader, project, &t, false)).await?;
        println!("trace tree load (no payloads)        {r}");
    }
    let f = RunFilter {
        status: Some("ok".into()),
        run_type: Some("agent".into()),
        ..Default::default()
    };
    let r = time(iters, || fastpath::list_runs(reader, project, &f, 50, None)).await?;
    println!("list runs, newest 50, 2 filters      {r}");
    let expr = forge_index::parse_query("refund policy", None)?;
    let all = RunFilter::default();
    let r = time(iters, || {
        forge_query::search::search(reader, project, &expr, &all, false, 50, None)
    })
    .await?;
    println!("full-text search, newest 50 hits     {r}");
    let r = time(iters.min(20), || {
        forge.query.sql(
            "SELECT run_type, count(*), avg(total_tokens) FROM spans GROUP BY run_type",
            Some(project.into()),
        )
    })
    .await?;
    println!("SQL group-by over all spans          {r}");
    Ok(())
}
