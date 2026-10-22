//! Read-path tests: merge-on-read across segments and memtables, fast paths, SQL, and
//! differential checks that fast paths and plain SQL agree.

use std::collections::BTreeSet;
use std::sync::Arc;

use forge_ingest::Ingester;
use forge_model::config::Config;
use forge_model::row::{SpanRow, span_id_hex, trace_id_hex};
use forge_model::traits::{IndexReaderRef, SegmentStoreRef};
use forge_query::QueryEngine;
use forge_query::fastpath::{self, RunFilter};
use serde_json::Value;

const T0: i64 = 1_761_148_800_000_000_000; // 2025-10-22T16:00Z
const MS: i64 = 1_000_000;

struct Env {
    _dir: tempfile::TempDir,
    ing: Arc<Ingester>,
    q: QueryEngine,
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::default().with_data_dir(dir.path());
    let meta = forge_meta::open(&config.metastore.url).unwrap();
    let store = forge_storage::from_url(&config.storage.url, &config.storage.options).unwrap();
    let segs: SegmentStoreRef = Arc::new(forge_storage::VortexSegmentStore::new(store.clone()));
    let ing = Ingester::open(config.clone(), meta.clone(), segs.clone(), store)
        .await
        .unwrap();
    let fts: IndexReaderRef = Arc::new(forge_index::FtsReader::new(segs.clone(), 1 << 26));
    let q = QueryEngine::new(meta, segs, Some(fts), ing.clone(), config.query.clone());
    Env { _dir: dir, ing, q }
}

fn trace(n: u8) -> [u8; 16] {
    let mut t = [0u8; 16];
    t[0] = n;
    t[15] = 1;
    t
}

/// A trace: root + `children` children, each with one grandchild. Start-only versions.
fn start_rows(n: u8, start: i64, children: u8) -> Vec<SpanRow> {
    let mut rows = Vec::new();
    let mut root = SpanRow::new("p", trace(n), [n, 0, 0, 0, 0, 0, 0, 1]);
    root.name = Some(format!("agent-{n}"));
    root.run_type = Some("agent".into());
    root.start_time = Some(start);
    root.inputs = Some(format!(
        r#"{{"messages":[{{"role":"user","content":"order {n} needs a refund"}}]}}"#
    ));
    root.metadata = Some(r#"{"env":"prod"}"#.into());
    root.tags = Some(vec!["t1".into()]);
    rows.push(root);
    for c in 0..children {
        let mut ch = SpanRow::new("p", trace(n), [n, 1, c, 0, 0, 0, 0, 1]);
        ch.parent_span_id = Some([n, 0, 0, 0, 0, 0, 0, 1]);
        ch.name = Some("llm".into());
        ch.run_type = Some("llm".into());
        ch.start_time = Some(start + (c as i64 + 1) * MS);
        ch.model = Some("gpt-4o".into());
        ch.total_tokens = Some(10 * c as i64);
        rows.push(ch.clone());
        let mut g = SpanRow::new("p", trace(n), [n, 2, c, 0, 0, 0, 0, 1]);
        g.parent_span_id = Some(ch.span_id);
        g.name = Some("tool".into());
        g.run_type = Some("tool".into());
        g.start_time = Some(start + (c as i64 + 1) * MS + 1);
        rows.push(g);
    }
    rows
}

/// End-of-span versions: only end time, status and outputs.
fn end_rows(rows: &[SpanRow], status: &str) -> Vec<SpanRow> {
    rows.iter()
        .map(|r| {
            let mut e = SpanRow::new("p", r.trace_id, r.span_id);
            e.parent_span_id = r.parent_span_id;
            e.start_time = r.start_time;
            e.end_time = Some(r.start_time.unwrap() + 50 * MS);
            e.status = Some(status.into());
            e.outputs = Some(r#"{"answer":"done"}"#.into());
            e
        })
        .collect()
}

async fn sql(q: &QueryEngine, s: &str) -> Vec<Value> {
    let r = q.sql(s, None).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    forge_query::to_json_rows(&r.batches)
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn merge_across_segments_and_memtable() {
    let e = env().await;
    // Trace 1: start versions flushed, end versions in the memtable.
    let t1 = start_rows(1, T0, 3);
    e.ing.ingest(t1.clone()).await.unwrap();
    e.ing.flush_now().await.unwrap();
    e.ing.ingest(end_rows(&t1, "ok")).await.unwrap();
    // Trace 2: entirely in segments, start and end in different segments.
    let t2 = start_rows(2, T0 + 10 * MS, 2);
    e.ing.ingest(t2.clone()).await.unwrap();
    e.ing.flush_now().await.unwrap();
    e.ing.ingest(end_rows(&t2[..1], "error")).await.unwrap();
    e.ing.flush_now().await.unwrap();

    let tree = fastpath::trace_tree(&e.q.reader, "p", &trace(1), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tree["span_count"], 7);
    let root = &tree["spans"][0];
    assert_eq!(root["name"], "agent-1");
    assert_eq!(root["status"], "ok");
    assert_eq!(root["inputs"]["messages"][0]["role"], "user");
    assert_eq!(root["outputs"]["answer"], "done");
    assert_eq!(root["latency_ms"], 50.0);
    assert_eq!(root["children"].as_array().unwrap().len(), 3);
    assert_eq!(root["children"][1]["children"][0]["name"], "tool");

    let tree2 = fastpath::trace_tree(&e.q.reader, "p", &trace(2), false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tree2["span_count"], 5);
    assert_eq!(tree2["spans"][0]["status"], "error");
    assert!(tree2["spans"][0].get("inputs").is_none());
    assert!(
        fastpath::trace_tree(&e.q.reader, "p", &trace(9), false)
            .await
            .unwrap()
            .is_none()
    );

    // SQL sees the merged state too.
    let rows = sql(&e.q, "SELECT count(*) AS n FROM spans").await;
    assert_eq!(rows[0]["n"], 12);
    let rows = sql(
        &e.q,
        "SELECT name, status FROM spans WHERE parent_span_id IS NULL ORDER BY start_time",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["status"], "ok");
    assert_eq!(rows[1]["status"], "error");
    // json_key over the merged payload.
    let rows = sql(&e.q, "SELECT json_key(outputs, '$.answer') AS a, json_key(inputs, 'messages[0].role') AS r FROM spans WHERE parent_span_id IS NULL AND status = 'ok'").await;
    assert_eq!(rows[0]["a"], "done");
    assert_eq!(rows[0]["r"], "user");
    // Filters that would be wrong below the merge are applied above it.
    let rows = sql(
        &e.q,
        "SELECT count(*) AS n FROM spans WHERE status = 'error'",
    )
    .await;
    assert_eq!(rows[0]["n"], 1);
    // trace_tree table function.
    let rows = sql(
        &e.q,
        &format!(
            "SELECT depth, name FROM trace_tree('{}', 'p')",
            trace_id_hex(&trace(1))
        ),
    )
    .await;
    assert_eq!(rows.len(), 7);
    assert_eq!(rows[0]["depth"], 0);
    assert_eq!(rows[2]["depth"], 2);
}

#[tokio::test]
async fn explain_keeps_payloads_out() {
    let e = env().await;
    e.ing.ingest(start_rows(1, T0, 2)).await.unwrap();
    e.ing.flush_now().await.unwrap();
    let plan = sql(&e.q, "EXPLAIN SELECT name, start_time FROM spans WHERE project_id = 'p' AND parent_span_id IS NULL").await;
    let text = plan
        .iter()
        .map(|r| r["plan"].as_str().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("SpansScanExec"), "{text}");
    assert!(
        text.contains("MergeVersionsExec"),
        "L0 segments need a merge:\n{text}"
    );
    assert!(
        text.contains("payload_columns=[]"),
        "payload columns read:\n{text}"
    );
    assert!(
        text.contains("roots_only: true"),
        "parent filter pushed down:\n{text}"
    );
    let plan = sql(&e.q, "EXPLAIN SELECT json_key(inputs, '$.a') FROM spans").await;
    let text = plan
        .iter()
        .map(|r| r["plan"].as_str().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("payload_columns=[inputs]"), "{text}");
}

#[tokio::test]
async fn sql_is_read_only_and_scoped() {
    let e = env().await;
    e.ing.ingest(start_rows(1, T0, 1)).await.unwrap();
    let mut other = start_rows(2, T0, 1);
    for r in other.iter_mut() {
        r.project_id = "q".into();
    }
    e.ing.ingest(other).await.unwrap();
    for bad in [
        "CREATE TABLE x (a INT)",
        "INSERT INTO spans VALUES (1)",
        "DROP TABLE spans",
        "SET datafusion.execution.batch_size = 1",
    ] {
        assert!(
            e.q.sql(bad, None).await.is_err(),
            "{bad} should be rejected"
        );
    }
    let all =
        e.q.sql("SELECT count(*) AS n FROM spans", None)
            .await
            .unwrap();
    assert_eq!(forge_query::to_json_rows(&all.batches).unwrap()[0]["n"], 6);
    let scoped =
        e.q.sql("SELECT count(*) AS n FROM spans", Some("q".into()))
            .await
            .unwrap();
    assert_eq!(
        forge_query::to_json_rows(&scoped.batches).unwrap()[0]["n"],
        3
    );
    let escape =
        e.q.sql(
            "SELECT count(*) AS n FROM spans WHERE project_id = 'p'",
            Some("q".into()),
        )
        .await
        .unwrap();
    assert_eq!(
        forge_query::to_json_rows(&escape.batches).unwrap()[0]["n"],
        0
    );
    assert!(e.q.sql("SELECT * FROM nope", None).await.is_err());
}

/// Differential: runs list fast path vs. the same query in SQL, across pages, partitions,
/// segments and the memtable.
#[tokio::test]
async fn runs_fast_path_matches_sql() {
    let e = env().await;
    let hour = 3_600_000_000_000i64;
    for n in 0..40u8 {
        // Spread over three hours; some flushed, some not.
        let start = T0 + (n as i64 % 3) * hour + n as i64 * 1000 * MS;
        let rows = start_rows(n, start, 1);
        e.ing.ingest(rows.clone()).await.unwrap();
        if n % 2 == 0 {
            e.ing
                .ingest(end_rows(&rows, if n % 4 == 0 { "error" } else { "ok" }))
                .await
                .unwrap();
        }
        if n % 7 == 0 {
            e.ing.flush_now().await.unwrap();
        }
    }
    let filters = [
        RunFilter::default(),
        RunFilter {
            status: Some("error".into()),
            ..Default::default()
        },
        RunFilter {
            start: Some(T0 + hour),
            end: Some(T0 + 2 * hour),
            ..Default::default()
        },
        RunFilter {
            min_latency_ms: Some(10.0),
            ..Default::default()
        },
        RunFilter {
            metadata: [("env".to_string(), "prod".to_string())].into(),
            tag: Some("t1".into()),
            ..Default::default()
        },
    ];
    let sql_where = [
        "",
        " AND status = 'error'",
        &format!(
            " AND start_time >= to_timestamp_nanos({}) AND start_time < to_timestamp_nanos({})",
            T0 + hour,
            T0 + 2 * hour
        ),
        " AND end_time - start_time >= INTERVAL '10 milliseconds'",
        " AND json_key(metadata, '$.env') = 'prod' AND array_has(tags, 't1')",
    ];
    for (f, w) in filters.iter().zip(sql_where.iter()) {
        let mut fast: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = fastpath::list_runs(&e.q.reader, "p", f, 7, cursor.as_deref())
                .await
                .unwrap();
            fast.extend(page.runs.iter().map(|r| span_id_hex(&r.span_id)));
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let q = format!(
            "SELECT span_id FROM spans WHERE project_id = 'p' AND parent_span_id IS NULL{w} ORDER BY start_time DESC, trace_id DESC, span_id DESC"
        );
        let slow: Vec<String> = sql(&e.q, &q)
            .await
            .iter()
            .map(|r| r["span_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(fast, slow, "filter {f:?}");
        assert!(!slow.is_empty(), "filter {f:?} should match something");
    }
}

/// Differential: full-text fast path vs. SQL `search()`.
#[tokio::test]
async fn search_fast_path_matches_sql() {
    let e = env().await;
    for n in 0..30u8 {
        let mut rows = start_rows(n, T0 + n as i64 * MS * 100, 1);
        if n % 3 == 0 {
            rows[1].error = Some(format!("Timeout calling upstream {n}"));
        }
        e.ing.ingest(rows.clone()).await.unwrap();
        if n % 5 == 0 {
            // Later version changes the text: the old one must stop matching.
            let mut upd = SpanRow::new("p", rows[0].trace_id, rows[0].span_id);
            upd.start_time = rows[0].start_time;
            upd.inputs =
                Some(r#"{"messages":[{"role":"user","content":"just saying hello"}]}"#.into());
            e.ing.ingest(vec![upd]).await.unwrap();
        }
        if n % 4 == 0 {
            e.ing.flush_now().await.unwrap();
        }
    }
    for (query, column) in [
        ("refund", "inputs"),
        ("timeout upstream", "error"),
        ("hello", "inputs"),
        ("nothing-matches", "inputs"),
    ] {
        let expr = forge_index::parse_query(query, Some(column)).unwrap();
        let mut fast: BTreeSet<String> = BTreeSet::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = forge_query::search::search(
                &e.q.reader,
                "p",
                &expr,
                &RunFilter::default(),
                false,
                4,
                cursor.as_deref(),
            )
            .await
            .unwrap();
            fast.extend(page.runs.iter().map(|r| span_id_hex(&r.span_id)));
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let slow: BTreeSet<String> = sql(
            &e.q,
            &format!("SELECT span_id FROM spans WHERE search({column}, '{query}')"),
        )
        .await
        .iter()
        .map(|r| r["span_id"].as_str().unwrap().to_string())
        .collect();
        assert_eq!(fast, slow, "query {query:?}");
    }
    // 30 roots mention a refund, minus the 6 rewritten to "hello".
    let n = sql(
        &e.q,
        "SELECT count(*) AS n FROM spans WHERE search(inputs, 'refund')",
    )
    .await;
    assert_eq!(n[0]["n"], 24);
    let n = sql(&e.q, "SELECT count(*) AS n FROM spans WHERE json_key_search(inputs, '$.messages[*].content', 'refund')").await;
    assert_eq!(n[0]["n"], 24);
}
