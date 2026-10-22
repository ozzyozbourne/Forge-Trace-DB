//! Object store fault injection: latency, transient errors and partial failures.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use forge_model::config::{ByteSize, Config, HumanDuration};
use forge_model::row::SpanRow;
use forge_model::traits::{SegmentFilter, SegmentState};
use forge_query::fastpath;
use forge_server::Forge;
use forge_storage::fault::{Faults, FaultyStore};

const T0: i64 = 1_761_148_800_000_000_000;

fn rows(trace: u8, n: u8) -> Vec<SpanRow> {
    (0..n)
        .map(|i| {
            let mut r = SpanRow::new("p", [trace; 16], [trace, i, 0, 0, 0, 0, 0, 1]);
            r.parent_span_id = (i > 0).then_some([trace, 0, 0, 0, 0, 0, 0, 1]);
            r.start_time = Some(T0 + trace as i64 * 1_000_000 + i as i64);
            r.name = Some(format!("span {i}"));
            r.inputs = Some(format!(r#"{{"q":"question {trace} {i}"}}"#));
            r
        })
        .collect()
}

async fn open(
    dir: &std::path::Path,
    faults: Faults,
    tweak: impl FnOnce(&mut Config),
) -> (Arc<Forge>, Arc<FaultyStore>) {
    let mut config = Config::default().with_data_dir(dir);
    config.compaction.enabled = false;
    config.compaction.gc_grace = HumanDuration(Duration::ZERO);
    config.compaction.orphan_age = HumanDuration(Duration::ZERO);
    tweak(&mut config);
    let meta = forge_meta::open(&config.metastore.url).unwrap();
    let base = forge_storage::from_url(&config.storage.url, &config.storage.options).unwrap();
    let faulty = Arc::new(FaultyStore::new(base, faults));
    let forge = Forge::open_with(config, meta, faulty.clone())
        .await
        .unwrap();
    (forge, faulty)
}

/// Retry an operation the way a client or the background loop would.
async fn eventually<T, F, Fut>(mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = forge_model::Result<T>>,
{
    for _ in 0..100 {
        if let Ok(v) = f().await {
            return v;
        }
    }
    panic!("operation never succeeded");
}

#[tokio::test]
async fn survives_transient_and_partial_failures() {
    let dir = tempfile::tempdir().unwrap();
    let faults = Faults {
        latency: Duration::from_millis(1),
        get_error_rate: 0.2,
        put_error_rate: 0.3,
        partial_put_rate: 0.2,
    };
    // No cache, so every read goes to the faulty store.
    let (forge, faulty) = open(dir.path(), faults, |c| {
        c.cache.memory = ByteSize(0);
        c.cache.disk = ByteSize(0);
    })
    .await;
    for t in 1..=12u8 {
        forge.ingester.ingest(rows(t, 8)).await.unwrap();
        if t % 3 == 0 {
            eventually(|| forge.ingester.flush_now()).await;
        }
    }
    eventually(|| forge.ingester.flush_now()).await;
    assert!(
        faulty
            .stats
            .injected
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "faults were injected"
    );

    // Every trace reads back whole and correct.
    for t in 1..=12u8 {
        let id = [t; 16];
        let tree = eventually(|| fastpath::trace_tree(&forge.query.reader, "p", &id, true))
            .await
            .unwrap();
        assert_eq!(tree["span_count"], 8, "trace {t}");
        assert_eq!(tree["spans"][0]["children"].as_array().unwrap().len(), 7);
    }
    let n = eventually(|| forge.query.sql("SELECT count(*) AS n FROM spans", None)).await;
    assert_eq!(forge_query::to_json_rows(&n.batches).unwrap()[0]["n"], 96);

    // Compaction under faults also converges, and GC removes the orphans partial puts left.
    eventually(|| forge.lifecycle.compact(None, true)).await;
    *faulty.faults.lock() = Faults::default();
    forge.lifecycle.gc().await.unwrap();
    let known = forge.metastore.all_object_paths().await.unwrap();
    let stored: HashSet<String> = forge
        .segments
        .list_objects("segments")
        .await
        .unwrap()
        .into_iter()
        .map(|(p, _, _)| p)
        .collect();
    assert_eq!(stored, known, "no orphans or missing objects after GC");
    let live = forge
        .metastore
        .list_segments(&SegmentFilter {
            states: Some(vec![SegmentState::Live]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(live.len(), 1);
    let n = forge
        .query
        .sql("SELECT count(*) AS n FROM spans", None)
        .await
        .unwrap();
    assert_eq!(forge_query::to_json_rows(&n.batches).unwrap()[0]["n"], 96);
}

#[tokio::test]
async fn cache_hides_object_store_latency() {
    let dir = tempfile::tempdir().unwrap();
    let faults = Faults {
        latency: Duration::from_millis(40),
        ..Default::default()
    };
    // Memory cache only: flushed objects are not on local disk.
    let (forge, faulty) = open(dir.path(), faults, |c| c.cache.disk = ByteSize(0)).await;
    forge.ingester.ingest(rows(1, 20)).await.unwrap();
    forge.ingester.flush_now().await.unwrap();

    let gets = || faulty.stats.gets.load(std::sync::atomic::Ordering::Relaxed);
    let t = Instant::now();
    fastpath::trace_tree(&forge.query.reader, "p", &[1; 16], true)
        .await
        .unwrap()
        .unwrap();
    let cold = t.elapsed();
    let cold_gets = gets();
    let t = Instant::now();
    fastpath::trace_tree(&forge.query.reader, "p", &[1; 16], true)
        .await
        .unwrap()
        .unwrap();
    let warm = t.elapsed();
    assert_eq!(gets(), cold_gets, "warm load made no object store requests");
    assert!(warm < cold / 2, "warm {warm:?} vs cold {cold:?}");
    let stats = forge.cache_stats.as_ref().unwrap();
    assert!(
        stats.misses.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "cold load went to the store"
    );
}

#[tokio::test]
async fn flushed_segments_are_written_through_to_disk_cache() {
    let dir = tempfile::tempdir().unwrap();
    let faults = Faults {
        latency: Duration::from_millis(40),
        ..Default::default()
    };
    let (forge, faulty) = open(dir.path(), faults, |c| c.cache.memory = ByteSize(1)).await;
    forge.ingester.ingest(rows(1, 20)).await.unwrap();
    forge.ingester.flush_now().await.unwrap();
    let before = faulty.stats.gets.load(std::sync::atomic::Ordering::Relaxed);
    fastpath::trace_tree(&forge.query.reader, "p", &[1; 16], true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        faulty.stats.gets.load(std::sync::atomic::Ordering::Relaxed),
        before,
        "leading-edge reads come from the disk cache"
    );
}
