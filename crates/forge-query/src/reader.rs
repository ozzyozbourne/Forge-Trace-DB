//! Shared read path: which rows a request needs, from which segments and memtables, merged.
//!
//! Every read takes the memtable snapshot *before* listing segments. A flush commits its
//! segments before dropping its memtable, so this order never misses a row; at worst a row is
//! seen twice, which the merge collapses.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use forge_ingest::MemSnapshot;
use forge_model::merge::{MergeOptions, merge_versions};
use forge_model::schema::{self, DELETED, KEY_COLUMNS, VERSION, spans_schema};
use forge_model::traits::{
    IndexReaderRef, MetastoreRef, ScanPredicate, ScanRequest, SegmentFilter, SegmentRecord,
    SegmentStoreRef,
};
use forge_model::{Result, TraceId};
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringTreemap;

/// Source of unflushed rows (the ingester).
pub trait MemtableSource: Send + Sync + 'static {
    fn snapshot(&self) -> MemSnapshot;
}

impl MemtableSource for forge_ingest::Ingester {
    fn snapshot(&self) -> MemSnapshot {
        forge_ingest::Ingester::snapshot(self)
    }
}

/// An empty memtable source, for offline tools reading only flushed data.
pub struct NoMemtables;

impl MemtableSource for NoMemtables {
    fn snapshot(&self) -> MemSnapshot {
        MemSnapshot::default()
    }
}

#[derive(Clone)]
pub struct Reader {
    pub metastore: MetastoreRef,
    pub segments: SegmentStoreRef,
    pub index: Option<IndexReaderRef>,
    pub mem: Arc<dyn MemtableSource>,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Reader")
    }
}

/// Rows a read resolved to, before merging.
pub struct ReadPlan {
    pub mem: Vec<RecordBatch>,
    pub segments: Vec<SegmentRecord>,
    pub predicate: ScanPredicate,
    /// Columns read from storage: requested columns plus the merge key.
    pub scan_columns: Vec<String>,
    /// Columns the caller asked for, in order.
    pub output_columns: Vec<String>,
    /// Row selections from the inverted index, by segment id.
    pub selections: HashMap<String, RoaringTreemap>,
}

impl ReadPlan {
    /// Whether rows from different inputs may share a key. Compacted (L1) segments partition
    /// keys disjointly and hold no tombstones, so they need no merge.
    pub fn needs_merge(&self) -> bool {
        self.mem.iter().any(|b| b.num_rows() > 0) || self.segments.iter().any(|s| s.level == 0)
    }
}

pub fn all_columns() -> Vec<String> {
    spans_schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Requested columns plus the ones the merge needs, in schema order.
pub fn with_merge_columns(columns: &[String]) -> Vec<String> {
    let mut want: Vec<String> = columns.to_vec();
    for c in KEY_COLUMNS.iter().chain([VERSION, DELETED].iter()) {
        if !want.iter().any(|w| w == c) {
            want.push(c.to_string());
        }
    }
    let schema = spans_schema();
    let mut ordered: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|n| want.contains(n))
        .collect();
    ordered.dedup();
    ordered
}

/// Select columns of a logical batch by name.
pub fn project(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let idx: Vec<usize> = columns
        .iter()
        .map(|c| schema.index_of(c))
        .collect::<std::result::Result<_, _>>()?;
    Ok(batch.project(&idx)?)
}

impl Reader {
    /// Resolve a read. `segments` overrides the metastore listing (fast paths that already
    /// know which segments to read).
    pub async fn plan(
        &self,
        predicate: ScanPredicate,
        columns: Option<Vec<String>>,
        segments: Option<Vec<SegmentRecord>>,
    ) -> Result<ReadPlan> {
        let snapshot = self.mem.snapshot();
        let output_columns = columns.unwrap_or_else(all_columns);
        let scan_columns = with_merge_columns(&output_columns);
        let segments = match segments {
            Some(s) => s,
            None => {
                let filter = SegmentFilter {
                    project_id: predicate.project_id.clone(),
                    start_min: predicate.start_min,
                    start_max: predicate.start_max,
                    trace_id: match &predicate.trace_ids {
                        Some(t) if t.len() == 1 => Some(t[0]),
                        _ => None,
                    },
                    ..Default::default()
                };
                let mut segs = self.metastore.list_segments(&filter).await?;
                if let Some(traces) = &predicate.trace_ids {
                    segs.retain(|s| {
                        traces.iter().any(|t| {
                            s.min_trace_id.as_slice() <= t.as_slice()
                                && t.as_slice() <= s.max_trace_id.as_slice()
                        })
                    });
                }
                segs
            }
        };
        let mem = snapshot
            .scan(&predicate)?
            .into_iter()
            .map(|b| project(&b, &scan_columns))
            .collect::<Result<Vec<_>>>()?;
        Ok(ReadPlan {
            mem,
            segments,
            predicate,
            scan_columns,
            output_columns,
            selections: HashMap::new(),
        })
    }

    /// Scan one segment of a plan.
    pub async fn scan_segment(
        &self,
        plan: &ReadPlan,
        seg: &SegmentRecord,
    ) -> Result<Vec<RecordBatch>> {
        let req = ScanRequest {
            projection: Some(plan.scan_columns.clone()),
            predicate: plan.predicate.clone(),
            selection: plan.selections.get(&seg.id).cloned(),
        };
        let stream = self.segments.scan_segment(seg, req).await?;
        stream.try_collect().await
    }

    /// Read every input of a plan concurrently.
    pub async fn read_inputs(&self, plan: &ReadPlan) -> Result<Vec<RecordBatch>> {
        let mut batches = plan.mem.clone();
        let reads: Vec<_> = plan
            .segments
            .iter()
            .map(|seg| {
                let store = self.segments.clone();
                let seg = seg.clone();
                let req = ScanRequest {
                    projection: Some(plan.scan_columns.clone()),
                    predicate: plan.predicate.clone(),
                    selection: plan.selections.get(&seg.id).cloned(),
                };
                async move {
                    let stream = store.scan_segment(&seg, req).await?;
                    stream.try_collect::<Vec<RecordBatch>>().await
                }
            })
            .collect();
        let results: Vec<Vec<RecordBatch>> = futures::stream::iter(reads)
            .buffer_unordered(16)
            .try_collect()
            .await?;
        batches.extend(results.into_iter().flatten());
        Ok(batches)
    }

    /// Read and merge, returning the requested columns.
    pub async fn read_merged(&self, plan: &ReadPlan) -> Result<RecordBatch> {
        let inputs = self.read_inputs(plan).await?;
        merge_and_project(plan, inputs)
    }

    /// The merged rows of one trace, via `trace_index`.
    pub async fn trace_rows(
        &self,
        project: &str,
        trace: &TraceId,
        columns: Option<Vec<String>>,
    ) -> Result<RecordBatch> {
        let predicate = ScanPredicate {
            project_id: Some(project.to_string()),
            trace_ids: Some(vec![*trace]),
            ..Default::default()
        };
        // Snapshot before the trace_index lookup (see module docs).
        let snapshot = self.mem.snapshot();
        let segments = self.metastore.segments_for_trace(project, trace).await?;
        let output_columns = columns.unwrap_or_else(all_columns);
        let scan_columns = with_merge_columns(&output_columns);
        let mem = snapshot
            .scan(&predicate)?
            .into_iter()
            .map(|b| project_batch(&b, &scan_columns))
            .collect::<Result<Vec<_>>>()?;
        let plan = ReadPlan {
            mem,
            segments,
            predicate,
            scan_columns,
            output_columns,
            selections: HashMap::new(),
        };
        self.read_merged(&plan).await
    }
}

fn project_batch(b: &RecordBatch, cols: &[String]) -> Result<RecordBatch> {
    project(b, cols)
}

/// Merge (when needed) and project to the output columns.
pub fn merge_and_project(plan: &ReadPlan, inputs: Vec<RecordBatch>) -> Result<RecordBatch> {
    let scan_schema = schema::project_schema(
        &spans_schema(),
        &plan
            .scan_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    let all = concat_batches(&scan_schema, &inputs)?;
    let merged = if plan.needs_merge() {
        merge_versions(&all, MergeOptions::default())?
    } else {
        all
    };
    project(&merged, &plan.output_columns)
}
