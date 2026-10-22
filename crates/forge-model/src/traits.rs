//! Interfaces between crates. `forge-meta` implements [`Metastore`], `forge-storage` implements
//! [`SegmentStore`], `forge-index` implements [`IndexReader`]. Changes here go through the lead.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::row::{SpanId, TraceId};

pub const HOUR_NS: i64 = 3_600_000_000_000;

/// Start of the hour containing `ns`, in nanoseconds.
pub fn hour_of(ns: i64) -> i64 {
    ns.div_euclid(HOUR_NS) * HOUR_NS
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing)]
    pub api_key_hash: Option<String>,
    pub retention_days: Option<u32>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentState {
    Live,
    Compacting,
    Deleted,
}

impl SegmentState {
    pub fn as_str(self) -> &'static str {
        match self {
            SegmentState::Live => "live",
            SegmentState::Compacting => "compacting",
            SegmentState::Deleted => "deleted",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "live" => Some(SegmentState::Live),
            "compacting" => Some(SegmentState::Compacting),
            "deleted" => Some(SegmentState::Deleted),
            _ => None,
        }
    }
    /// Whether queries read segments in this state.
    pub fn readable(self) -> bool {
        !matches!(self, SegmentState::Deleted)
    }
}

/// One immutable segment object, as the metastore lists it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentRecord {
    pub id: String,
    pub project_id: String,
    /// 0 = flushed from a memtable, 1 = compacted.
    pub level: u8,
    /// Start of the partition hour, ns since epoch.
    pub partition_hour: i64,
    pub path: String,
    /// Inverted-index sidecar, when one was built.
    pub fts_path: Option<String>,
    pub row_count: u64,
    pub byte_size: u64,
    pub min_start_time: Option<i64>,
    pub max_start_time: Option<i64>,
    #[serde(with = "hex_vec")]
    pub min_trace_id: Vec<u8>,
    #[serde(with = "hex_vec")]
    pub max_trace_id: Vec<u8>,
    pub max_version: i64,
    pub created_at_ms: i64,
    pub state: SegmentState,
    pub deleted_at_ms: Option<i64>,
}

mod hex_vec {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        hex::decode(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// Which segments a query needs. Every field narrows the result; `None` means unconstrained.
#[derive(Debug, Clone, Default)]
pub struct SegmentFilter {
    pub project_id: Option<String>,
    /// Inclusive lower bound on `start_time`.
    pub start_min: Option<i64>,
    /// Exclusive upper bound on `start_time`.
    pub start_max: Option<i64>,
    pub trace_id: Option<TraceId>,
    pub level: Option<u8>,
    pub partition_hour: Option<i64>,
    /// Defaults to readable (live + compacting) segments.
    pub states: Option<Vec<SegmentState>>,
}

/// A new segment and the traces it contains, committed atomically.
#[derive(Debug, Clone)]
pub struct NewSegment {
    pub record: SegmentRecord,
    pub trace_ids: Vec<TraceId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionJob {
    pub id: String,
    pub project_id: String,
    pub partition_hour: i64,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub status: String,
    pub lease_expiry_ms: i64,
}

#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    async fn create_project(&self, project: ProjectRecord) -> Result<()>;
    async fn get_project(&self, id: &str) -> Result<Option<ProjectRecord>>;
    async fn project_by_key_hash(&self, hash: &str) -> Result<Option<ProjectRecord>>;
    async fn list_projects(&self) -> Result<Vec<ProjectRecord>>;
    async fn set_project_key(&self, id: &str, hash: &str) -> Result<()>;

    /// In one transaction: insert `add` (and their trace_index rows) and mark `remove` deleted.
    async fn commit_segments(&self, add: Vec<NewSegment>, remove: Vec<String>) -> Result<()>;
    async fn list_segments(&self, filter: &SegmentFilter) -> Result<Vec<SegmentRecord>>;
    /// Readable segments holding a trace, from `trace_index`.
    async fn segments_for_trace(
        &self,
        project_id: &str,
        trace_id: &TraceId,
    ) -> Result<Vec<SegmentRecord>>;
    /// Every object path the metastore knows about, in any state.
    async fn all_object_paths(&self) -> Result<HashSet<String>>;
    /// Remove rows of segments whose objects are gone.
    async fn purge_segments(&self, ids: &[String]) -> Result<()>;
    /// Mark every segment of a project with partition before `before_hour` deleted. Returns count.
    async fn expire_partitions(&self, project_id: &str, before_hour: i64) -> Result<usize>;

    /// Take a lease on compacting one partition. `None` when someone else holds it.
    async fn lease_compaction(
        &self,
        project_id: &str,
        partition_hour: i64,
        inputs: &[String],
        lease_ms: i64,
    ) -> Result<Option<CompactionJob>>;
    async fn finish_compaction(&self, job_id: &str, outputs: &[String], status: &str)
    -> Result<()>;
    async fn list_compaction_jobs(&self, limit: usize) -> Result<Vec<CompactionJob>>;

    /// Highest version committed to any segment; versions after restart start above it.
    async fn max_version(&self) -> Result<i64>;
}

pub type MetastoreRef = Arc<dyn Metastore>;

/// Row filter a segment scan can evaluate inside the file reader. Only stable columns
/// appear here, so applying it before the version merge is safe.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScanPredicate {
    pub project_id: Option<String>,
    pub trace_ids: Option<Vec<TraceId>>,
    pub span_ids: Option<Vec<SpanId>>,
    /// Inclusive.
    pub start_min: Option<i64>,
    /// Exclusive.
    pub start_max: Option<i64>,
    pub roots_only: bool,
}

impl ScanPredicate {
    pub fn is_empty(&self) -> bool {
        *self == ScanPredicate::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    /// Column names to read; `None` reads all.
    pub projection: Option<Vec<String>>,
    pub predicate: ScanPredicate,
    /// Only these row ids (positions in the segment).
    pub selection: Option<RoaringTreemap>,
}

pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Result of writing a segment.
#[derive(Debug, Clone)]
pub struct WrittenSegment {
    pub path: String,
    pub byte_size: u64,
    pub row_count: u64,
}

#[async_trait]
pub trait SegmentStore: Send + Sync + 'static {
    /// Write a sorted storage-schema batch as a segment at `path`.
    async fn write_segment(&self, path: &str, batch: &RecordBatch) -> Result<WrittenSegment>;
    /// Scan a segment. Returned batches use the logical schema, projected as requested.
    async fn scan_segment(
        &self,
        segment: &SegmentRecord,
        request: ScanRequest,
    ) -> Result<BatchStream>;
    async fn put_object(&self, path: &str, bytes: bytes::Bytes) -> Result<()>;
    async fn get_object(&self, path: &str) -> Result<bytes::Bytes>;
    async fn get_range(&self, path: &str, range: std::ops::Range<u64>) -> Result<bytes::Bytes>;
    async fn delete_object(&self, path: &str) -> Result<()>;
    /// `(path, last_modified_ms, size)` of every object under a prefix.
    async fn list_objects(&self, prefix: &str) -> Result<Vec<(String, i64, u64)>>;
}

pub type SegmentStoreRef = Arc<dyn SegmentStore>;

/// A parsed full-text query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SearchExpr {
    /// A token in a column (or any indexed column), optionally under one JSON path.
    Term {
        column: Option<String>,
        path: Option<String>,
        token: String,
    },
    And(Vec<SearchExpr>),
    Or(Vec<SearchExpr>),
}

#[async_trait]
pub trait IndexReader: Send + Sync + 'static {
    /// Row ids in the segment that may match. `None` when the segment has no index, in which
    /// case the caller falls back to scanning.
    async fn lookup(
        &self,
        segment: &SegmentRecord,
        query: &SearchExpr,
    ) -> Result<Option<RoaringTreemap>>;
}

pub type IndexReaderRef = Arc<dyn IndexReader>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hours() {
        assert_eq!(hour_of(HOUR_NS + 5), HOUR_NS);
        assert_eq!(hour_of(-1), -HOUR_NS);
    }
}
