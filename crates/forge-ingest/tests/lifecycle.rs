use std::sync::Arc;
use std::time::Duration;

use forge_ingest::Ingester;
use forge_ingest::lifecycle::Lifecycle;
use forge_model::config::{ByteSize, Config, HumanDuration};
use forge_model::merge::{MergeOptions, merge_batches};
use forge_model::row::{SpanRow, batch_to_rows};
use forge_model::schema::spans_schema;
use forge_model::traits::{
    Metastore, ProjectRecord, ScanRequest, SegmentFilter, SegmentState, SegmentStoreRef,
};
use forge_storage::VortexSegmentStore;

const T0: i64 = 1_761_148_800_000_000_000;

struct Env {
    _dir: tempfile::TempDir,
    ing: Arc<Ingester>,
    meta: Arc<dyn Metastore>,
    segs: SegmentStoreRef,
    config: Config,
}

async fn env(tweak: impl FnOnce(&mut Config)) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default().with_data_dir(dir.path());
    config.compaction.gc_grace = HumanDuration(Duration::ZERO);
    tweak(&mut config);
    let meta = forge_meta::open(&config.metastore.url).unwrap();
    let store = forge_storage::from_url(&config.storage.url, &config.storage.options).unwrap();
    let segs: SegmentStoreRef = Arc::new(VortexSegmentStore::new(store.clone()));
    let ing = Ingester::open(config.clone(), meta.clone(), segs.clone(), store)
        .await
        .unwrap();
    Env {
        _dir: dir,
        ing,
        meta,
        segs,
        config,
    }
}

fn span(trace: u8, id: u8, parent: Option<u8>, t: i64) -> SpanRow {
    let mut r = SpanRow::new("p", [trace; 16], [id; 8]);
    r.parent_span_id = parent.map(|p| [p; 8]);
    r.start_time = Some(t);
    r.name = Some(format!("s{id}"));
    r
}

async fn live(e: &Env) -> Vec<forge_model::traits::SegmentRecord> {
    e.meta
        .list_segments(&SegmentFilter::default())
        .await
        .unwrap()
}

async fn all_rows(e: &Env) -> Vec<SpanRow> {
    let mut batches = vec![];
    for s in live(e).await {
        let st = e
            .segs
            .scan_segment(&s, ScanRequest::default())
            .await
            .unwrap();
        batches.extend(forge_storage::segment::collect(st).await.unwrap());
    }
    batch_to_rows(&merge_batches(&spans_schema(), &batches, MergeOptions::default()).unwrap())
}

#[tokio::test]
async fn compacts_partition_with_upserts_tombstones_and_backfill() {
    let e = env(|_| {}).await;
    // Child before parent, in different flushes: no dotted order at ingest.
    e.ing
        .ingest(vec![span(1, 2, Some(1), T0 + 10)])
        .await
        .unwrap();
    e.ing.flush_now().await.unwrap();
    e.ing
        .ingest(vec![span(1, 1, None, T0), span(1, 3, None, T0 + 20)])
        .await
        .unwrap();
    e.ing.flush_now().await.unwrap();
    // Update and delete in later flushes.
    let mut upd = SpanRow::new("p", [1; 16], [1; 8]);
    upd.start_time = Some(T0);
    upd.status = Some("ok".into());
    let mut tomb = SpanRow::new("p", [1; 16], [3; 8]);
    tomb.start_time = Some(T0 + 20);
    tomb.deleted = true;
    e.ing.ingest(vec![upd, tomb]).await.unwrap();
    e.ing.flush_now().await.unwrap();
    assert_eq!(live(&e).await.len(), 3);
    let before = all_rows(&e).await;
    assert_eq!(before.len(), 2);
    assert!(before.iter().any(|r| r.dotted_order.is_none()));

    let lc = Lifecycle::new(e.config.clone(), e.meta.clone(), e.segs.clone());
    let report = lc.compact(None, true).await.unwrap();
    assert_eq!(
        (report.partitions, report.inputs, report.outputs),
        (1, 3, 1)
    );
    assert_eq!(report.rows_out, 2);
    assert_eq!(report.dotted_filled, 1);
    let segs = live(&e).await;
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].level, 1);
    assert!(segs[0].fts_path.is_some());
    let after = all_rows(&e).await;
    assert_eq!(after.len(), 2);
    let root = after.iter().find(|r| r.span_id == [1; 8]).unwrap();
    assert_eq!(root.status.as_deref(), Some("ok"));
    let child = after.iter().find(|r| r.span_id == [2; 8]).unwrap();
    assert!(
        child
            .dotted_order
            .as_ref()
            .unwrap()
            .starts_with(root.dotted_order.as_ref().unwrap())
    );
    assert_eq!(
        e.meta
            .segments_for_trace("p", &[1; 16])
            .await
            .unwrap()
            .len(),
        1
    );

    // Nothing left to do; a second run is a no-op.
    assert_eq!(lc.compact(None, true).await.unwrap().partitions, 0);

    // GC removes the inputs' objects (grace is zero in this test).
    let gc = lc.gc().await.unwrap();
    assert_eq!(gc.segments_purged, 3);
    let objects = e.segs.list_objects("segments").await.unwrap();
    assert_eq!(
        objects.len(),
        2,
        "one .vortex and one .fts remain: {objects:?}"
    );

    // A late span lands in L0 and the next compaction folds it into the partition.
    e.ing
        .ingest(vec![span(1, 4, Some(1), T0 + 30)])
        .await
        .unwrap();
    e.ing.flush_now().await.unwrap();
    let r = lc.compact(None, true).await.unwrap();
    assert_eq!((r.inputs, r.outputs, r.rows_out), (2, 1, 3));
}

#[tokio::test]
async fn splits_large_partitions_by_trace_range() {
    let e = env(|c| c.compaction.target_segment = ByteSize(20_000)).await;
    for batch in 0..4u8 {
        let rows: Vec<SpanRow> = (0..50u8)
            .map(|i| {
                let mut r = span(i, batch + 1, None, T0 + i as i64);
                r.inputs = Some(format!("{{\"x\":\"{}\"}}", "y".repeat(200)));
                r
            })
            .collect();
        e.ing.ingest(rows).await.unwrap();
        e.ing.flush_now().await.unwrap();
    }
    let lc = Lifecycle::new(e.config.clone(), e.meta.clone(), e.segs.clone());
    let r = lc.compact(None, true).await.unwrap();
    assert!(r.outputs > 1, "{r:?}");
    assert_eq!(r.rows_out, 200);
    let mut segs = live(&e).await;
    segs.sort_by(|a, b| a.min_trace_id.cmp(&b.min_trace_id));
    for w in segs.windows(2) {
        assert!(w[0].max_trace_id < w[1].min_trace_id, "outputs overlap");
    }
    assert_eq!(all_rows(&e).await.len(), 200);
}

#[tokio::test]
async fn retention_and_orphans() {
    let e = env(|c| c.compaction.orphan_age = HumanDuration(Duration::ZERO)).await;
    e.meta
        .create_project(ProjectRecord {
            id: "p".into(),
            name: "p".into(),
            api_key_hash: None,
            retention_days: Some(7),
            created_at_ms: 0,
        })
        .await
        .unwrap();
    let now = forge_model::now_ns();
    let day = 24 * 3_600_000_000_000i64;
    e.ing
        .ingest(vec![
            span(1, 1, None, now - 30 * day),
            span(2, 1, None, now - day),
        ])
        .await
        .unwrap();
    e.ing.flush_now().await.unwrap();
    assert_eq!(live(&e).await.len(), 2);
    // An object nobody references, e.g. from a crash between upload and commit.
    e.segs
        .put_object(
            "segments/p/0/2020-01-01/00/ORPHAN.vortex",
            bytes::Bytes::from_static(b"x"),
        )
        .await
        .unwrap();

    let lc = Lifecycle::new(e.config.clone(), e.meta.clone(), e.segs.clone());
    assert_eq!(lc.retention().await.unwrap(), 1);
    let left = live(&e).await;
    assert_eq!(left.len(), 1);
    assert!(left[0].min_start_time.unwrap() > now - 2 * day);
    let gc = lc.gc().await.unwrap();
    assert_eq!(gc.segments_purged, 1);
    assert_eq!(gc.orphans_deleted, 1);
    let deleted = e
        .meta
        .list_segments(&SegmentFilter {
            states: Some(vec![SegmentState::Deleted]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(deleted.is_empty());
    assert_eq!(all_rows(&e).await.len(), 1);
}

#[tokio::test]
async fn open_hour_merges_l0_into_runs() {
    let e = env(|c| c.compaction.min_l0_segments = 2).await;
    let now = forge_model::now_ns();
    e.ing
        .ingest(vec![span(1, 1, None, now), span(2, 1, None, now)])
        .await
        .unwrap();
    e.ing.flush_now().await.unwrap();
    e.ing.ingest(vec![span(3, 1, None, now)]).await.unwrap();
    e.ing.flush_now().await.unwrap();
    let lc = Lifecycle::new(e.config.clone(), e.meta.clone(), e.segs.clone());
    let r = lc.compact(None, false).await.unwrap();
    assert_eq!((r.inputs, r.outputs), (2, 1));

    // Delete a span that now lives in the L1 run, plus new data, in two more L0s.
    let mut tomb = SpanRow::new("p", [1; 16], [1; 8]);
    tomb.start_time = Some(now);
    tomb.deleted = true;
    e.ing.ingest(vec![tomb]).await.unwrap();
    e.ing.flush_now().await.unwrap();
    e.ing.ingest(vec![span(4, 1, None, now)]).await.unwrap();
    e.ing.flush_now().await.unwrap();
    let r = lc.compact(None, false).await.unwrap();
    assert_eq!(
        (r.inputs, r.outputs),
        (2, 1),
        "only the L0s are rewritten in an open hour"
    );
    let segs = live(&e).await;
    assert_eq!(segs.iter().filter(|s| s.level == 1).count(), 2);
    let l1: Vec<_> = segs.iter().collect();
    assert!(
        forge_ingest::lifecycle::l1_overlap(&l1),
        "runs overlap, so reads merge them"
    );
    let rows = all_rows(&e).await;
    assert_eq!(
        rows.len(),
        3,
        "tombstone survived the L0 merge and shadows the older run"
    );
    assert!(!rows.iter().any(|r| r.trace_id == [1; 16]));

    // Size-tiered: two more runs make four similar runs, which merge into one.
    for t in 5..9u8 {
        e.ing.ingest(vec![span(t, 1, None, now)]).await.unwrap();
        e.ing.flush_now().await.unwrap();
        if t % 2 == 0 {
            lc.compact(None, false).await.unwrap();
        }
    }
    let r = lc.compact(None, false).await.unwrap();
    assert_eq!(r.outputs, 1, "{r:?}");
    let l1 = live(&e).await.into_iter().filter(|s| s.level == 1).count();
    assert!(l1 <= 2, "runs were tiered together: {l1}");
    assert_eq!(all_rows(&e).await.len(), 7);

    // Forcing rewrites the partition into disjoint L1 without tombstones.
    lc.compact(None, true).await.unwrap();
    let segs = live(&e).await;
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].level, 1);
    assert_eq!(all_rows(&e).await.len(), 7);
}
