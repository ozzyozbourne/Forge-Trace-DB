//! In-memory tables of recently ingested rows. Each ingest request appends one Arrow batch;
//! a per-trace index makes trace loads and span lookups cheap without a scan.
//!
//! [`Memtables`] holds the active table plus frozen ones waiting for flush. Queries take a
//! [`MemSnapshot`], which is cheap: batches are immutable and shared.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::TimestampNanosecondType;
use forge_model::Result;
use forge_model::merge::{MergeOptions, merge_versions};
use forge_model::row::{SpanId, SpanRow, TraceId, batch_to_rows};
use forge_model::schema::{self, spans_schema};
use forge_model::traits::ScanPredicate;
use parking_lot::RwLock;

type TraceKey = (String, TraceId);

#[derive(Default)]
struct Inner {
    batches: Vec<RecordBatch>,
    /// (project, trace) -> (batch, row) positions.
    traces: HashMap<TraceKey, Vec<(u32, u32)>>,
    bytes: usize,
    rows: usize,
    min_version: Option<i64>,
}

pub struct Memtable {
    pub id: u64,
    created: Instant,
    inner: RwLock<Inner>,
    /// WAL boundary covering every record in this table; set when frozen.
    wal_boundary: RwLock<Option<u64>>,
}

impl Memtable {
    pub fn new(id: u64) -> Self {
        Memtable {
            id,
            created: Instant::now(),
            inner: RwLock::new(Inner::default()),
            wal_boundary: RwLock::new(None),
        }
    }

    pub fn insert(&self, batch: RecordBatch) {
        let n = batch.num_rows();
        if n == 0 {
            return;
        }
        let projects = batch
            .column(schema::col_idx(schema::PROJECT_ID))
            .as_string::<i32>()
            .clone();
        let traces = batch
            .column(schema::col_idx(schema::TRACE_ID))
            .as_fixed_size_binary()
            .clone();
        let mut g = self.inner.write();
        let bi = g.batches.len() as u32;
        for r in 0..n {
            let t: TraceId = traces.value(r).try_into().expect("16-byte trace id");
            g.traces
                .entry((projects.value(r).to_string(), t))
                .or_default()
                .push((bi, r as u32));
        }
        g.bytes += batch.get_array_memory_size();
        g.rows += n;
        if let Some(v) = forge_model::merge::max_version(&batch) {
            g.min_version = Some(g.min_version.map_or(v, |m| m.min(v)));
        }
        g.batches.push(batch);
    }

    pub fn bytes(&self) -> usize {
        self.inner.read().bytes
    }

    pub fn rows(&self) -> usize {
        self.inner.read().rows
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().rows == 0
    }

    pub fn age(&self) -> std::time::Duration {
        self.created.elapsed()
    }

    pub fn batches(&self) -> Vec<RecordBatch> {
        self.inner.read().batches.clone()
    }

    pub fn set_wal_boundary(&self, b: u64) {
        *self.wal_boundary.write() = Some(b);
    }

    pub fn wal_boundary(&self) -> Option<u64> {
        *self.wal_boundary.read()
    }

    /// Unmerged rows of one trace.
    pub fn trace_rows(&self, project: &str, trace: &TraceId) -> Option<RecordBatch> {
        let g = self.inner.read();
        let pos = g.traces.get(&(project.to_string(), *trace))?;
        let mut by_batch: HashMap<u32, Vec<u32>> = HashMap::new();
        for (b, r) in pos {
            by_batch.entry(*b).or_default().push(*r);
        }
        let mut parts = Vec::with_capacity(by_batch.len());
        for (b, rows) in by_batch {
            let idx = arrow::array::UInt32Array::from(rows);
            let batch = &g.batches[b as usize];
            let cols = batch
                .columns()
                .iter()
                .map(|c| arrow::compute::take(c.as_ref(), &idx, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .ok()?;
            parts.push(RecordBatch::try_new(batch.schema(), cols).ok()?);
        }
        concat_batches(&spans_schema(), &parts).ok()
    }
}

/// Active + frozen memtables.
pub struct Memtables {
    pub active: Arc<Memtable>,
    /// Oldest first.
    pub frozen: Vec<Arc<Memtable>>,
    next_id: u64,
}

impl Default for Memtables {
    fn default() -> Self {
        Memtables {
            active: Arc::new(Memtable::new(0)),
            frozen: Vec::new(),
            next_id: 1,
        }
    }
}

impl Memtables {
    /// Swap in a new active table; returns the frozen one.
    pub fn freeze(&mut self) -> Arc<Memtable> {
        let fresh = Arc::new(Memtable::new(self.next_id));
        self.next_id += 1;
        let old = std::mem::replace(&mut self.active, fresh);
        self.frozen.push(old.clone());
        old
    }

    pub fn remove_frozen(&mut self, id: u64) {
        self.frozen.retain(|m| m.id != id);
    }

    pub fn total_bytes(&self) -> usize {
        self.active.bytes() + self.frozen.iter().map(|m| m.bytes()).sum::<usize>()
    }

    pub fn snapshot(&self) -> MemSnapshot {
        let mut tables: Vec<Arc<Memtable>> = self.frozen.clone();
        tables.push(self.active.clone());
        MemSnapshot { tables }
    }
}

/// A consistent view of all memtables at one instant.
#[derive(Clone, Default)]
pub struct MemSnapshot {
    pub tables: Vec<Arc<Memtable>>,
}

fn filter_mask(batch: &RecordBatch, p: &ScanPredicate) -> Option<arrow::array::BooleanArray> {
    if p.is_empty() {
        return None;
    }
    let n = batch.num_rows();
    let project = batch
        .column(schema::col_idx(schema::PROJECT_ID))
        .as_string::<i32>();
    let trace = batch
        .column(schema::col_idx(schema::TRACE_ID))
        .as_fixed_size_binary();
    let span = batch
        .column(schema::col_idx(schema::SPAN_ID))
        .as_fixed_size_binary();
    let parent = batch.column(schema::col_idx(schema::PARENT_SPAN_ID));
    let start = batch
        .column(schema::col_idx(schema::START_TIME))
        .as_primitive::<TimestampNanosecondType>();
    Some(
        (0..n)
            .map(|i| {
                if let Some(pr) = &p.project_id
                    && project.value(i) != pr
                {
                    return Some(false);
                }
                if let Some(ts) = &p.trace_ids
                    && !ts.iter().any(|t| t.as_slice() == trace.value(i))
                {
                    return Some(false);
                }
                if let Some(ss) = &p.span_ids
                    && !ss.iter().any(|s| s.as_slice() == span.value(i))
                {
                    return Some(false);
                }
                if p.start_min.is_some() || p.start_max.is_some() {
                    if start.is_null(i) {
                        return Some(false);
                    }
                    let v = start.value(i);
                    if p.start_min.is_some_and(|m| v < m) || p.start_max.is_some_and(|m| v >= m) {
                        return Some(false);
                    }
                }
                if p.roots_only && parent.is_valid(i) {
                    return Some(false);
                }
                if p.trace_min.is_some_and(|t| trace.value(i) < t.as_slice())
                    || p.trace_max.is_some_and(|t| trace.value(i) > t.as_slice())
                {
                    return Some(false);
                }
                Some(true)
            })
            .collect(),
    )
}

impl MemSnapshot {
    pub fn is_empty(&self) -> bool {
        self.tables.iter().all(|t| t.is_empty())
    }

    pub fn rows(&self) -> usize {
        self.tables.iter().map(|t| t.rows()).sum()
    }

    /// All unmerged rows matching a predicate.
    pub fn scan(&self, p: &ScanPredicate) -> Result<Vec<RecordBatch>> {
        // Trace lookups use the index.
        if let (Some(project), Some(traces)) = (&p.project_id, &p.trace_ids) {
            let mut out = Vec::new();
            for t in &self.tables {
                for tr in traces {
                    if let Some(b) = t.trace_rows(project, tr) {
                        match filter_mask(&b, p) {
                            Some(m) => out.push(filter_record_batch(&b, &m)?),
                            None => out.push(b),
                        }
                    }
                }
            }
            return Ok(out);
        }
        let mut out = Vec::new();
        for t in &self.tables {
            for b in t.batches() {
                match filter_mask(&b, p) {
                    Some(m) => {
                        let f = filter_record_batch(&b, &m)?;
                        if f.num_rows() > 0 {
                            out.push(f);
                        }
                    }
                    None => out.push(b),
                }
            }
        }
        Ok(out)
    }

    /// The merged current state of one span, if the memtables hold any version of it.
    pub fn find_span(
        &self,
        project: &str,
        trace: &TraceId,
        span: &SpanId,
    ) -> Result<Option<SpanRow>> {
        let p = ScanPredicate {
            project_id: Some(project.to_string()),
            trace_ids: Some(vec![*trace]),
            span_ids: Some(vec![*span]),
            ..Default::default()
        };
        let parts = self.scan(&p)?;
        if parts.is_empty() {
            return Ok(None);
        }
        let b = concat_batches(&spans_schema(), &parts)?;
        let merged = merge_versions(
            &b,
            MergeOptions {
                keep_tombstones: true,
            },
        )?;
        Ok(batch_to_rows(&merged).into_iter().next())
    }

    /// Find a span by id alone (PATCH without trace id). Linear scan of the memtables.
    pub fn find_span_any_trace(&self, project: &str, span: &SpanId) -> Result<Option<SpanRow>> {
        let p = ScanPredicate {
            project_id: Some(project.to_string()),
            span_ids: Some(vec![*span]),
            ..Default::default()
        };
        let parts = self.scan(&p)?;
        if parts.is_empty() {
            return Ok(None);
        }
        let b = concat_batches(&spans_schema(), &parts)?;
        let merged = merge_versions(
            &b,
            MergeOptions {
                keep_tombstones: true,
            },
        )?;
        Ok(batch_to_rows(&merged).into_iter().next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::row::rows_to_batch;

    fn row(t: u8, s: u8, v: i64) -> SpanRow {
        let mut r = SpanRow::new("p", [t; 16], [s; 8]);
        r.version = v;
        r.start_time = Some(v * 10);
        r
    }

    #[test]
    fn index_and_snapshot() {
        let mut m = Memtables::default();
        m.active
            .insert(rows_to_batch(&[row(1, 1, 1), row(2, 1, 2)]).unwrap());
        let frozen = m.freeze();
        m.active.insert(
            rows_to_batch(&[{
                let mut r = row(1, 1, 3);
                r.name = Some("late".into());
                r.start_time = None;
                r
            }])
            .unwrap(),
        );
        assert_eq!(frozen.rows(), 2);
        let snap = m.snapshot();
        assert_eq!(snap.rows(), 3);
        let s = snap.find_span("p", &[1; 16], &[1; 8]).unwrap().unwrap();
        assert_eq!(s.name.as_deref(), Some("late"));
        assert_eq!(s.start_time, Some(10));
        assert!(snap.find_span("p", &[3; 16], &[1; 8]).unwrap().is_none());
        assert_eq!(
            snap.find_span_any_trace("p", &[1; 8])
                .unwrap()
                .unwrap()
                .trace_id,
            [1; 16]
        );
        let p = ScanPredicate {
            start_min: Some(15),
            ..Default::default()
        };
        assert_eq!(
            snap.scan(&p)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            1
        );
        m.remove_frozen(frozen.id);
        assert_eq!(m.snapshot().rows(), 1);
    }
}
