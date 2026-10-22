//! Prometheus metrics on the ops port: request counts and latency per route, plus ingest,
//! flush, cache, memtable and segment gauges read at scrape time.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use forge_model::traits::{SegmentFilter, SegmentState};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};

use crate::Forge;

pub struct Metrics {
    registry: Registry,
    requests: IntCounterVec,
    latency: HistogramVec,
}

impl Default for Metrics {
    fn default() -> Self {
        let registry = Registry::new();
        let requests = IntCounterVec::new(
            Opts::new(
                "forge_http_requests_total",
                "HTTP requests by listener, route and status",
            ),
            &["listener", "route", "status"],
        )
        .expect("metric");
        let latency = HistogramVec::new(
            HistogramOpts::new(
                "forge_http_request_duration_seconds",
                "HTTP request latency",
            )
            .buckets(vec![
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &["listener", "route"],
        )
        .expect("metric");
        registry
            .register(Box::new(requests.clone()))
            .expect("register");
        registry
            .register(Box::new(latency.clone()))
            .expect("register");
        Metrics {
            registry,
            requests,
            latency,
        }
    }
}

impl Metrics {
    pub fn observe(&self, listener: &str, route: &str, status: u16, seconds: f64) {
        self.requests
            .with_label_values(&[listener, route, &status.to_string()])
            .inc();
        self.latency
            .with_label_values(&[listener, route])
            .observe(seconds);
    }
}

async fn track(listener: &'static str, f: Arc<Forge>, req: Request, next: Next) -> Response {
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".into());
    let start = Instant::now();
    let resp = next.run(req).await;
    f.metrics.observe(
        listener,
        &route,
        resp.status().as_u16(),
        start.elapsed().as_secs_f64(),
    );
    resp
}

pub async fn track_api(State(f): State<Arc<Forge>>, req: Request, next: Next) -> Response {
    track("api", f, req, next).await
}

pub async fn track_otlp(State(f): State<Arc<Forge>>, req: Request, next: Next) -> Response {
    track("otlp_http", f, req, next).await
}

fn line(out: &mut String, kind: &str, name: &str, help: &str, samples: &[(&str, f64)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
    for (labels, v) in samples {
        if labels.is_empty() {
            let _ = writeln!(out, "{name} {v}");
        } else {
            let _ = writeln!(out, "{name}{{{labels}}} {v}");
        }
    }
}

pub async fn render(State(f): State<Arc<Forge>>) -> impl IntoResponse {
    let mut out = String::new();
    let s = &f.ingester.stats;
    let c = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed) as f64;
    line(
        &mut out,
        "counter",
        "forge_ingest_spans_total",
        "Span versions ingested",
        &[("", c(&s.spans))],
    );
    line(
        &mut out,
        "counter",
        "forge_ingest_requests_total",
        "Ingest batches accepted",
        &[("", c(&s.requests))],
    );
    line(
        &mut out,
        "counter",
        "forge_ingest_rejected_total",
        "Ingest batches rejected by backpressure",
        &[("", c(&s.rejected))],
    );
    line(
        &mut out,
        "counter",
        "forge_flushes_total",
        "Memtable flushes",
        &[("", c(&s.flushes))],
    );
    line(
        &mut out,
        "counter",
        "forge_flush_errors_total",
        "Failed flush attempts",
        &[("", c(&s.flush_errors))],
    );
    line(
        &mut out,
        "counter",
        "forge_segments_written_total",
        "L0 segments written",
        &[("", c(&s.segments_written))],
    );
    line(
        &mut out,
        "counter",
        "forge_flushed_bytes_total",
        "Bytes of L0 segments written",
        &[("", c(&s.bytes_flushed))],
    );
    line(
        &mut out,
        "gauge",
        "forge_memtable_bytes",
        "Bytes buffered in memtables",
        &[("", f.ingester.buffered_bytes() as f64)],
    );
    line(
        &mut out,
        "gauge",
        "forge_memtable_budget_bytes",
        "Memtable budget before backpressure",
        &[("", f.config.memtable.memory_budget.bytes() as f64)],
    );
    if let Some(cs) = &f.cache_stats {
        line(
            &mut out,
            "counter",
            "forge_cache_requests_total",
            "Object reads by cache outcome",
            &[
                ("result=\"memory\"", c(&cs.memory_hits)),
                ("result=\"disk\"", c(&cs.disk_hits)),
                ("result=\"miss\"", c(&cs.misses)),
            ],
        );
        line(
            &mut out,
            "counter",
            "forge_object_store_read_bytes_total",
            "Bytes read from the object store",
            &[("", c(&cs.bytes_from_store))],
        );
    }
    if let Ok(segs) = f
        .metastore
        .list_segments(&SegmentFilter {
            states: Some(vec![SegmentState::Live, SegmentState::Compacting]),
            ..Default::default()
        })
        .await
    {
        let (mut n0, mut n1, mut b0, mut b1, mut rows) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for s in &segs {
            rows += s.row_count as f64;
            if s.level == 0 {
                n0 += 1.0;
                b0 += s.byte_size as f64;
            } else {
                n1 += 1.0;
                b1 += s.byte_size as f64;
            }
        }
        line(
            &mut out,
            "gauge",
            "forge_segments",
            "Live segments by level",
            &[("level=\"0\"", n0), ("level=\"1\"", n1)],
        );
        line(
            &mut out,
            "gauge",
            "forge_segment_bytes",
            "Live segment bytes by level",
            &[("level=\"0\"", b0), ("level=\"1\"", b1)],
        );
        line(
            &mut out,
            "gauge",
            "forge_segment_rows",
            "Rows (versions) in live segments",
            &[("", rows)],
        );
    }
    let mut buf = Vec::new();
    if TextEncoder::new()
        .encode(&f.metrics.registry.gather(), &mut buf)
        .is_ok()
    {
        out.push_str(&String::from_utf8_lossy(&buf));
    }
    ([("content-type", "text/plain; version=0.0.4")], out)
}
