//! Hot functions on the write and read paths.

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use forge_model::dotted;
use forge_model::merge::{MergeOptions, merge_versions};
use forge_model::opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use forge_model::opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use forge_model::opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use forge_model::otlp::Normalizer;
use forge_model::row::{SpanRow, rows_to_batch};

fn rows(n: usize, dup_every: usize) -> Vec<SpanRow> {
    (0..n)
        .map(|i| {
            let k = if i % dup_every == 0 { i / 2 } else { i };
            let mut t = [0u8; 16];
            t[..8].copy_from_slice(&((k / 50) as u64).to_be_bytes());
            let mut s = [0u8; 8];
            s.copy_from_slice(&(k as u64).to_be_bytes());
            let mut r = SpanRow::new("p", t, s);
            r.parent_span_id = (k % 50 != 0).then(|| ((k - k % 50) as u64).to_be_bytes());
            r.start_time = Some(1_761_148_800_000_000_000 + k as i64 * 1000);
            r.name = Some("chat gpt-4o".into());
            r.inputs = (i % 3 == 0).then(|| "x".repeat(500));
            r.version = i as i64;
            r
        })
        .collect()
}

fn kv(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.into())),
        }),
        ..Default::default()
    }
}

fn otlp(n: usize) -> ExportTraceServiceRequest {
    let spans = (0..n)
        .map(|i| Span {
            trace_id: vec![1; 16],
            span_id: (i as u64 + 1).to_be_bytes().to_vec(),
            parent_span_id: if i == 0 {
                vec![]
            } else {
                1u64.to_be_bytes().to_vec()
            },
            name: "chat".into(),
            start_time_unix_nano: 1_761_148_800_000_000_000 + i as u64,
            end_time_unix_nano: 1_761_148_800_000_000_100 + i as u64,
            attributes: vec![
                kv("gen_ai.operation.name", "chat"),
                kv("gen_ai.request.model", "gpt-4o"),
                kv(
                    "gen_ai.input.messages",
                    &format!(
                        r#"[{{"role":"user","parts":[{{"type":"text","content":"{}"}}]}}]"#,
                        "hello ".repeat(150)
                    ),
                ),
                kv("http.method", "POST"),
            ],
            ..Default::default()
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

fn bench(c: &mut Criterion) {
    let batch = rows_to_batch(&rows(50_000, 5)).unwrap();
    c.bench_function("merge_versions 50k rows, 20% upserts", |b| {
        b.iter(|| merge_versions(&batch, MergeOptions::default()).unwrap())
    });
    let unique = rows_to_batch(&rows(50_000, usize::MAX)).unwrap();
    c.bench_function("merge_versions 50k unique rows", |b| {
        b.iter(|| merge_versions(&unique, MergeOptions::default()).unwrap())
    });
    let req = otlp(500);
    let n = Normalizer::default();
    c.bench_function("normalize 500 GenAI spans", |b| {
        b.iter(|| n.normalize(&req, None, "default").unwrap())
    });
    let r = rows(5_000, usize::MAX);
    c.bench_function("dotted_order fill 5k spans", |b| {
        b.iter_batched(
            || r.clone(),
            |mut r| dotted::fill(&mut r, &|_, _| None),
            BatchSize::SmallInput,
        )
    });
    c.bench_function("rows_to_batch 5k", |b| {
        b.iter(|| rows_to_batch(&r).unwrap())
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
