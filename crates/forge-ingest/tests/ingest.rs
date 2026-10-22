use std::sync::Arc;

use forge_ingest::Ingester;
use forge_model::config::Config;
use forge_model::row::{SpanRow, batch_to_rows};
use forge_model::traits::{ScanPredicate, ScanRequest, SegmentFilter, SegmentStoreRef};
use forge_storage::VortexSegmentStore;

fn span(trace: u8, span: u8, parent: Option<u8>, t: i64) -> SpanRow {
    let mut r = SpanRow::new("proj", [trace; 16], [span; 8]);
    r.parent_span_id = parent.map(|p| [p; 8]);
    r.start_time = Some(1_700_000_000_000_000_000 + t);
    r.name = Some(format!("span-{span}"));
    r.inputs = Some(format!(r#"{{"q":"question {span}"}}"#));
    r
}

async fn open(
    dir: &std::path::Path,
) -> (
    Arc<Ingester>,
    Arc<dyn forge_model::traits::Metastore>,
    SegmentStoreRef,
) {
    let mut config = Config::default().with_data_dir(dir);
    config.ingest.blob_threshold = forge_model::config::ByteSize(64);
    let meta = forge_meta::open(&config.metastore.url).unwrap();
    let store = forge_storage::from_url(&config.storage.url, &config.storage.options).unwrap();
    let segs: SegmentStoreRef = Arc::new(VortexSegmentStore::new(store.clone()));
    let ing = Ingester::open(config, meta.clone(), segs.clone(), store)
        .await
        .unwrap();
    (ing, meta, segs)
}

#[tokio::test]
async fn ingest_flush_replay() {
    let dir = tempfile::tempdir().unwrap();
    let (ing, meta, segs) = open(dir.path()).await;
    ing.ingest(vec![span(1, 1, None, 0), span(1, 2, Some(1), 10)])
        .await
        .unwrap();
    // Child arriving in a later request still gets a dotted order from the cache.
    ing.ingest(vec![span(1, 3, Some(2), 20)]).await.unwrap();
    let snap = ing.snapshot();
    assert_eq!(snap.rows(), 3);
    let s3 = snap.find_span("proj", &[1; 16], &[3; 8]).unwrap().unwrap();
    assert_eq!(s3.dotted_order.unwrap().matches('.').count(), 2);

    assert_eq!(ing.flush_now().await.unwrap(), 1);
    assert_eq!(ing.snapshot().rows(), 0);
    let listed = meta.list_segments(&SegmentFilter::default()).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].row_count, 3);
    assert!(listed[0].fts_path.is_some());
    assert_eq!(
        meta.segments_for_trace("proj", &[1; 16])
            .await
            .unwrap()
            .len(),
        1
    );
    let rows: Vec<SpanRow> = forge_storage::segment::collect(
        segs.scan_segment(
            &listed[0],
            ScanRequest {
                predicate: ScanPredicate {
                    trace_ids: Some(vec![[1; 16]]),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    )
    .await
    .unwrap()
    .iter()
    .flat_map(batch_to_rows)
    .collect();
    // Sorted in tree order.
    assert_eq!(
        rows.iter().map(|r| r.span_id[0]).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    // A large payload was offloaded to a blob object.
    let r = &rows[0];
    assert!(
        forge_storage::blob::blob_ref(r.inputs.as_ref().unwrap()).is_none(),
        "small stays inline"
    );
    let mut big = span(2, 9, None, 0);
    big.outputs = Some(format!("\"{}\"", "x".repeat(200)));
    ing.ingest(vec![big]).await.unwrap();
    let out = ing
        .snapshot()
        .find_span("proj", &[2; 16], &[9; 8])
        .unwrap()
        .unwrap()
        .outputs
        .unwrap();
    let sha = forge_storage::blob::blob_ref(&out).expect("offloaded");
    let resolved = forge_storage::blob::resolve(segs.as_ref(), "proj", out)
        .await
        .unwrap();
    assert_eq!(resolved.len(), 202);
    assert_eq!(sha.len(), 64);

    // Unflushed rows survive a restart via the WAL.
    ing.ingest(vec![span(3, 1, None, 0)]).await.unwrap();
    drop(ing);
    let (ing, meta, _) = open(dir.path()).await;
    assert_eq!(ing.snapshot().rows(), 2);
    ing.flush_now().await.unwrap();
    drop(ing);
    let (ing, _, _) = open(dir.path()).await;
    assert_eq!(ing.snapshot().rows(), 0, "WAL truncated after flush");
    assert_eq!(
        meta.list_segments(&SegmentFilter::default())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn backpressure() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default().with_data_dir(dir.path());
    config.memtable.memory_budget = forge_model::config::ByteSize(1);
    let meta = forge_meta::open(&config.metastore.url).unwrap();
    let store = forge_storage::from_url(&config.storage.url, &config.storage.options).unwrap();
    let segs: SegmentStoreRef = Arc::new(VortexSegmentStore::new(store.clone()));
    let ing = Ingester::open(config, meta, segs, store).await.unwrap();
    ing.ingest(vec![span(1, 1, None, 0)]).await.unwrap();
    assert!(matches!(
        ing.ingest(vec![span(1, 2, None, 0)]).await,
        Err(forge_model::ForgeError::Backpressure(_))
    ));
    ing.flush_now().await.unwrap();
    ing.ingest(vec![span(1, 2, None, 0)]).await.unwrap();
}
