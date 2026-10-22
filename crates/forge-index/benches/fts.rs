use criterion::{Criterion, criterion_group, criterion_main};
use forge_index::terms::parse_query;
use forge_model::row::{SpanRow, rows_to_batch};

fn bench(c: &mut Criterion) {
    let words = [
        "refund", "order", "shipping", "invoice", "customer", "agent", "policy", "warranty",
    ];
    let rows: Vec<SpanRow> = (0..20_000usize)
        .map(|i| {
            let mut r = SpanRow::new("p", [1; 16], [0; 8]);
            r.name = Some("chat".into());
            let text: Vec<&str> = (0..300)
                .map(|j| words[(i * 7 + j * 13) % words.len()])
                .collect();
            r.inputs = Some(format!(
                r#"{{"messages":[{{"role":"user","content":"{} #{i}"}}]}}"#,
                text.join(" ")
            ));
            r
        })
        .collect();
    let batch = rows_to_batch(&rows).unwrap();
    let mut g = c.benchmark_group("fts");
    g.sample_size(10);
    g.bench_function("build sidecar 20k rows x 2KB", |b| {
        b.iter(|| forge_index::build(std::slice::from_ref(&batch)).unwrap())
    });
    let sidecar = forge_index::build(std::slice::from_ref(&batch)).unwrap();
    let q = parse_query("refund policy", None).unwrap();
    g.bench_function("search sidecar (2 terms, all columns)", |b| {
        b.iter(|| forge_index::fts::search_bytes(&sidecar, &q).unwrap())
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
