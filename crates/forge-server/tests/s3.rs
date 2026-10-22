//! S3-compatible storage end to end, including `wal.mode = object_store`.
//!
//! Skipped unless `FORGE_TEST_S3_ENDPOINT` points at an S3-compatible server with an existing
//! bucket `FORGE_TEST_S3_BUCKET` (default `forge-test`), e.g. `moto_server` or MinIO:
//!
//! ```sh
//! moto_server -p 5555 & curl -X PUT http://127.0.0.1:5555/forge-test
//! FORGE_TEST_S3_ENDPOINT=http://127.0.0.1:5555 cargo test -p forge-server --test s3
//! ```

use std::collections::BTreeMap;

use forge_model::config::{Config, WalMode};
use forge_model::row::SpanRow;
use forge_query::fastpath;
use forge_server::Forge;

fn config(dir: &std::path::Path, endpoint: &str, prefix: &str) -> Config {
    let bucket = std::env::var("FORGE_TEST_S3_BUCKET").unwrap_or_else(|_| "forge-test".into());
    let mut c = Config::default().with_data_dir(dir);
    c.storage.url = format!("s3://{bucket}/{prefix}");
    let mut o = BTreeMap::new();
    o.insert("aws_endpoint".to_string(), endpoint.to_string());
    o.insert("aws_allow_http".to_string(), "true".to_string());
    o.insert("aws_access_key_id".to_string(), "test".to_string());
    o.insert("aws_secret_access_key".to_string(), "test".to_string());
    o.insert("aws_region".to_string(), "us-east-1".to_string());
    o.insert(
        "aws_virtual_hosted_style_request".to_string(),
        "false".to_string(),
    );
    c.storage.options = o;
    c.wal.mode = WalMode::ObjectStore;
    c.compaction.enabled = false;
    c.compaction.gc_grace = forge_model::config::HumanDuration(std::time::Duration::ZERO);
    c
}

fn rows(trace: u8) -> Vec<SpanRow> {
    (0..5u8)
        .map(|i| {
            let mut r = SpanRow::new("p", [trace; 16], [trace, i, 0, 0, 0, 0, 0, 1]);
            r.parent_span_id = (i > 0).then_some([trace, 0, 0, 0, 0, 0, 0, 1]);
            r.start_time = Some(1_761_148_800_000_000_000 + i as i64);
            r.inputs = Some(format!(r#"{{"q":"refund {trace}"}}"#));
            r
        })
        .collect()
}

#[tokio::test]
async fn s3_storage_and_object_store_wal() {
    let Ok(endpoint) = std::env::var("FORGE_TEST_S3_ENDPOINT") else {
        eprintln!("skipping: FORGE_TEST_S3_ENDPOINT not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let prefix = format!("run-{}", forge_model::now_ns());
    let c = config(dir.path(), &endpoint, &prefix);

    let forge = Forge::open(c.clone()).await.unwrap();
    forge.ingester.ingest(rows(1)).await.unwrap();
    forge.ingester.flush_now().await.unwrap();
    forge.ingester.ingest(rows(2)).await.unwrap();
    forge.ingester.flush_now().await.unwrap();
    // Unflushed: only in the object-store WAL.
    forge.ingester.ingest(rows(3)).await.unwrap();
    let wal_objects = forge.segments.list_objects("wal").await.unwrap();
    assert!(
        !wal_objects.is_empty(),
        "WAL chunks are objects in the bucket"
    );
    drop(forge);

    // A fresh process (and fresh local disk for cache) replays the WAL from the bucket.
    let dir2 = tempfile::tempdir().unwrap();
    let mut c2 = config(dir2.path(), &endpoint, &prefix);
    c2.metastore = c.metastore.clone();
    let forge = Forge::open(c2).await.unwrap();
    for t in 1..=3u8 {
        let tree = fastpath::trace_tree(&forge.query.reader, "p", &[t; 16], true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tree["span_count"], 5, "trace {t}");
    }
    let expr = forge_index::parse_query("refund", None).unwrap();
    let hits = forge_query::search::search(
        &forge.query.reader,
        "p",
        &expr,
        &Default::default(),
        false,
        50,
        None,
    )
    .await
    .unwrap();
    assert_eq!(hits.runs.len(), 15);

    forge.ingester.flush_now().await.unwrap();
    forge.lifecycle.compact(None, true).await.unwrap();
    forge.lifecycle.gc().await.unwrap();
    let known = forge.metastore.all_object_paths().await.unwrap();
    let stored: std::collections::HashSet<String> = forge
        .segments
        .list_objects("segments")
        .await
        .unwrap()
        .into_iter()
        .map(|(p, _, _)| p)
        .collect();
    assert_eq!(stored, known);
    let n = forge
        .query
        .sql("SELECT count(*) AS n FROM spans", None)
        .await
        .unwrap();
    assert_eq!(forge_query::to_json_rows(&n.batches).unwrap()[0]["n"], 15);
}
