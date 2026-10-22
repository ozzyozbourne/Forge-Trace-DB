//! Vortex segment files on object storage.

use std::ops::Range;
use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use forge_model::schema::{self, storage_schema};
use forge_model::traits::{
    BatchStream, ScanPredicate, ScanRequest, SegmentRecord, SegmentStore, WrittenSegment,
};
use forge_model::{ForgeError, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use vortex::VortexSessionDefault;
use vortex::array::IntoArray;
use vortex::array::expr::{self, Expression};
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{ByteBuffer, ByteBufferMut};
use vortex::extension::datetime::{TimeUnit, Timestamp, TimestampOptions};
use vortex::file::{OpenOptionsSessionExt, VortexFile, WriteOptionsSessionExt};
use vortex::scalar::Scalar;
use vortex::scan::selection::Selection;
use vortex::session::VortexSession;

use crate::store::ObjectStoreRef;

pub static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::default);

fn verr(e: impl std::fmt::Display) -> ForgeError {
    ForgeError::internal(format!("vortex: {e}"))
}

fn ostore_err(e: object_store::Error) -> ForgeError {
    match e {
        object_store::Error::NotFound { path, .. } => ForgeError::NotFound(path),
        other => ForgeError::Internal(anyhow::Error::new(other)),
    }
}

/// Encode a storage-schema batch as a Vortex file in memory.
pub async fn encode_segment(batch: &RecordBatch) -> Result<Bytes> {
    let schema = batch.schema();
    let array = SESSION
        .arrow()
        .from_arrow_record_batch(batch.clone(), &schema)
        .map_err(verr)?;
    let mut buf = ByteBufferMut::empty();
    SESSION
        .write_options()
        .write(&mut buf, array.into_array().to_array_stream())
        .await
        .map_err(verr)?;
    Ok(buf.freeze().into_bytes())
}

fn ts_lit(ns: i64) -> Expression {
    expr::lit(Scalar::extension::<Timestamp>(
        TimestampOptions {
            unit: TimeUnit::Nanoseconds,
            tz: Some(schema::TZ.into()),
        },
        Scalar::from(Some(ns)),
    ))
}

fn bin_lit(b: &[u8]) -> Expression {
    expr::lit(ByteBuffer::copy_from(b))
}

/// Translate a [`ScanPredicate`] to a Vortex filter expression.
pub fn predicate_expr(p: &ScanPredicate) -> Option<Expression> {
    let mut parts: Vec<Expression> = Vec::new();
    if let Some(project) = &p.project_id {
        parts.push(expr::eq(
            expr::col(schema::PROJECT_ID),
            expr::lit(project.as_str()),
        ));
    }
    if let Some(ids) = &p.trace_ids
        && let Some(e) = expr::or_collect(
            ids.iter()
                .map(|t| expr::eq(expr::col(schema::TRACE_ID), bin_lit(t))),
        )
    {
        parts.push(e);
    }
    if let Some(ids) = &p.span_ids
        && let Some(e) = expr::or_collect(
            ids.iter()
                .map(|s| expr::eq(expr::col(schema::SPAN_ID), bin_lit(s))),
        )
    {
        parts.push(e);
    }
    if let Some(min) = p.start_min {
        parts.push(expr::gt_eq(expr::col(schema::START_TIME), ts_lit(min)));
    }
    if let Some(max) = p.start_max {
        parts.push(expr::lt(expr::col(schema::START_TIME), ts_lit(max)));
    }
    if p.roots_only {
        parts.push(expr::is_null(expr::col(schema::PARENT_SPAN_ID)));
    }
    expr::and_collect(parts)
}

pub struct VortexSegmentStore {
    store: ObjectStoreRef,
    /// Opened files keyed by path; holds parsed footers so repeat scans skip the footer read.
    files: moka::future::Cache<String, VortexFile>,
}

impl VortexSegmentStore {
    pub fn new(store: ObjectStoreRef) -> Self {
        VortexSegmentStore {
            store,
            files: moka::future::Cache::builder().max_capacity(4096).build(),
        }
    }

    pub fn object_store(&self) -> &ObjectStoreRef {
        &self.store
    }

    pub async fn open(&self, path: &str) -> Result<VortexFile> {
        let store = self.store.clone();
        let p = path.to_string();
        self.files
            .try_get_with(p.clone(), async move {
                SESSION
                    .open_options()
                    .open_object_store(&store, Path::from(p))
                    .await
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(|e| {
                if e.contains("not found") || e.contains("NotFound") {
                    ForgeError::NotFound(path.to_string())
                } else {
                    verr(e)
                }
            })
    }

    /// Footer summary for `forge inspect`.
    pub async fn describe(&self, path: &str) -> Result<String> {
        let f = self.open(path).await?;
        let mut out = String::new();
        out.push_str(&format!("rows: {}\n", f.row_count()));
        out.push_str(&format!("dtype: {}\n", f.dtype()));
        if let Some(stats) = f.file_stats() {
            out.push_str(&format!("statistics: {stats:?}\n"));
        }
        if let Ok(splits) = f.splits() {
            out.push_str(&format!("row splits: {}\n", splits.len()));
        }
        Ok(out)
    }
}

#[async_trait]
impl SegmentStore for VortexSegmentStore {
    async fn write_segment(&self, path: &str, batch: &RecordBatch) -> Result<WrittenSegment> {
        let bytes = encode_segment(batch).await?;
        let size = bytes.len() as u64;
        self.store
            .put(&Path::from(path), bytes.into())
            .await
            .map_err(ostore_err)?;
        Ok(WrittenSegment {
            path: path.to_string(),
            byte_size: size,
            row_count: batch.num_rows() as u64,
        })
    }

    async fn scan_segment(&self, segment: &SegmentRecord, req: ScanRequest) -> Result<BatchStream> {
        let file = self.open(&segment.path).await?;
        let full = storage_schema();
        let columns: Vec<String> = match &req.projection {
            Some(p) => p.clone(),
            None => full.fields().iter().map(|f| f.name().clone()).collect(),
        };
        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
        let out_schema = schema::project_schema(&full, &names);
        let dtype = file.dtype().clone();
        let projection = expr::select(names.clone(), expr::root())
            .bind(&dtype)
            .map_err(verr)?;
        let mut scan = file.scan().map_err(verr)?.with_projection(projection);
        if let Some(f) = predicate_expr(&req.predicate) {
            scan = scan.with_filter(f.bind(&dtype).map_err(verr)?);
        }
        if let Some(sel) = req.selection {
            if sel.is_empty() {
                return Ok(futures::stream::empty().boxed());
            }
            scan = scan.with_selection(Selection::IncludeRoaring(sel));
        }
        let stream = scan
            .into_record_batch_stream(out_schema)
            .map_err(verr)?
            .map_err(|e| ForgeError::Internal(anyhow::Error::new(e)))
            .and_then(|b| async move { Ok(schema::from_storage(&b)?) });
        Ok(stream.boxed())
    }

    async fn put_object(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.store
            .put(&Path::from(path), bytes.into())
            .await
            .map_err(ostore_err)?;
        Ok(())
    }

    async fn get_object(&self, path: &str) -> Result<Bytes> {
        let r = self
            .store
            .get(&Path::from(path))
            .await
            .map_err(ostore_err)?;
        r.bytes().await.map_err(ostore_err)
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&Path::from(path), range)
            .await
            .map_err(ostore_err)
    }

    async fn delete_object(&self, path: &str) -> Result<()> {
        self.files.invalidate(path).await;
        match self.store.delete(&Path::from(path)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(ostore_err(e)),
        }
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<(String, i64, u64)>> {
        let prefix = Path::from(prefix);
        let metas: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(ostore_err)?;
        Ok(metas
            .into_iter()
            .map(|m| {
                (
                    m.location.to_string(),
                    m.last_modified.timestamp_millis(),
                    m.size,
                )
            })
            .collect())
    }
}

/// Collect a batch stream.
pub async fn collect(stream: BatchStream) -> Result<Vec<RecordBatch>> {
    stream.try_collect().await
}

pub fn arc_store(s: VortexSegmentStore) -> Arc<VortexSegmentStore> {
    Arc::new(s)
}

#[allow(dead_code)]
fn _assert_store(_: &dyn ObjectStore) {}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::row::{SpanRow, batch_to_rows, rows_to_batch};
    use forge_model::traits::SegmentState;
    use object_store::memory::InMemory;

    fn rows() -> Vec<SpanRow> {
        (0..1000u32)
            .map(|i| {
                let mut t = [0u8; 16];
                t[12..].copy_from_slice(&(i / 10).to_be_bytes());
                let mut s = [0u8; 8];
                s[4..].copy_from_slice(&i.to_be_bytes());
                let mut r = SpanRow::new(if i < 500 { "a" } else { "b" }, t, s);
                r.parent_span_id = (i % 10 != 0).then_some([1; 8]);
                r.start_time = Some(i as i64 * 1_000);
                r.name = Some(format!("span {i}"));
                r.inputs = Some(format!("{{\"q\":\"question {i}\"}}"));
                r.version = i as i64;
                r
            })
            .collect()
    }

    fn record(path: &str) -> SegmentRecord {
        SegmentRecord {
            id: "x".into(),
            project_id: "a".into(),
            level: 0,
            partition_hour: 0,
            path: path.into(),
            fts_path: None,
            row_count: 1000,
            byte_size: 0,
            min_start_time: None,
            max_start_time: None,
            min_trace_id: vec![],
            max_trace_id: vec![],
            max_version: 0,
            created_at_ms: 0,
            state: SegmentState::Live,
            deleted_at_ms: None,
        }
    }

    #[tokio::test]
    async fn write_and_scan() {
        let store = VortexSegmentStore::new(Arc::new(InMemory::new()));
        let rows = rows();
        let batch = schema::to_storage(&rows_to_batch(&rows).unwrap()).unwrap();
        let w = store
            .write_segment("segments/a/0/x.vortex", &batch)
            .await
            .unwrap();
        assert_eq!(w.row_count, 1000);
        let seg = record("segments/a/0/x.vortex");

        // Full read round-trips.
        let all = collect(
            store
                .scan_segment(&seg, ScanRequest::default())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let back: Vec<SpanRow> = all.iter().flat_map(batch_to_rows).collect();
        assert_eq!(back, rows);

        // Predicate + projection.
        let mut t = [0u8; 16];
        t[12..].copy_from_slice(&7u32.to_be_bytes());
        let req = ScanRequest {
            projection: Some(vec!["span_id".into(), "name".into()]),
            predicate: ScanPredicate {
                project_id: Some("a".into()),
                trace_ids: Some(vec![t]),
                ..Default::default()
            },
            selection: None,
        };
        let got = collect(store.scan_segment(&seg, req).await.unwrap())
            .await
            .unwrap();
        let n: usize = got.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 10);
        assert_eq!(got[0].num_columns(), 2);
        assert_eq!(
            got[0].schema().field(0).data_type(),
            &arrow::datatypes::DataType::FixedSizeBinary(8)
        );

        // Time range + roots only.
        let req = ScanRequest {
            projection: Some(vec!["start_time".into()]),
            predicate: ScanPredicate {
                start_min: Some(100_000),
                start_max: Some(200_000),
                roots_only: true,
                ..Default::default()
            },
            selection: None,
        };
        let got = collect(store.scan_segment(&seg, req).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 10);

        // Row selection.
        let mut sel = roaring::RoaringTreemap::new();
        sel.insert(3);
        sel.insert(999);
        let req = ScanRequest {
            projection: Some(vec!["name".into()]),
            selection: Some(sel),
            ..Default::default()
        };
        let got = collect(store.scan_segment(&seg, req).await.unwrap())
            .await
            .unwrap();
        let names: Vec<_> = got
            .iter()
            .flat_map(batch_to_rows)
            .map(|r| r.name.unwrap())
            .collect();
        assert_eq!(names, vec!["span 3", "span 999"]);

        assert!(matches!(
            store
                .scan_segment(&record("missing.vortex"), ScanRequest::default())
                .await,
            Err(ForgeError::NotFound(_)) | Err(ForgeError::Internal(_))
        ));
    }
}
