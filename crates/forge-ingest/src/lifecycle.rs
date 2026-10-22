//! Background lifecycle: compaction (L0 → L1 per project-hour), `dotted_order` backfill,
//! retention and garbage collection.
//!
//! Compaction rewrites a whole partition: every live segment of `(project, hour)` is merged
//! (upserts applied, tombstones dropped, missing `dotted_order` filled) into L1 segments of
//! roughly `target_segment` bytes. Work proceeds one trace-id range at a time, so memory is
//! bounded by the output size rather than the partition. Outputs are committed and inputs
//! marked deleted in one metastore transaction; deleted objects are removed by GC after a grace
//! period so in-flight queries can finish.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{AsArray, RecordBatch};
use arrow::compute::concat_batches;
use forge_model::config::Config;
use forge_model::merge::{MergeOptions, merge_versions};
use forge_model::row::{SpanId, TraceId, batch_to_rows, rows_to_batch};
use forge_model::schema::{self, spans_schema};
use forge_model::traits::{
    HOUR_NS, MetastoreRef, NewSegment, ScanPredicate, ScanRequest, SegmentFilter, SegmentRecord,
    SegmentState, SegmentStoreRef, hour_of,
};
use forge_model::{Result, dotted, now_ms, now_ns};
use futures::{StreamExt, TryStreamExt};

use crate::writer;

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct CompactionReport {
    pub partitions: usize,
    pub inputs: usize,
    pub outputs: usize,
    pub rows_in: u64,
    pub rows_out: u64,
    pub dotted_filled: u64,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct GcReport {
    pub segments_purged: usize,
    pub orphans_deleted: usize,
    pub blobs_deleted: usize,
    pub partitions_expired: usize,
}

pub struct Lifecycle {
    config: Config,
    metastore: MetastoreRef,
    segments: SegmentStoreRef,
}

pub type LifecycleRef = Arc<Lifecycle>;

/// What a compaction of one partition merges.
#[derive(Debug, Clone, PartialEq)]
pub enum MergePlan {
    /// Every live segment, into disjoint L1 segments.
    Full,
    /// Only these segments, into one new L1 run.
    Subset(Vec<String>),
}

/// Size tiers of L1 runs: runs within a factor of four of each other share a tier. Returns
/// the ids of the smallest tier holding at least four runs.
fn tier_to_merge(l1: &[&SegmentRecord]) -> Option<Vec<String>> {
    let mut tiers: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for s in l1 {
        let tier = (s.byte_size.max(1 << 20) as f64 / (1 << 20) as f64)
            .log(4.0)
            .floor() as u32;
        tiers.entry(tier).or_default().push(s.id.clone());
    }
    tiers.into_values().find(|ids| ids.len() >= 4)
}

/// Whether any two L1 runs' trace ranges overlap (so reads must merge them).
pub fn l1_overlap(l1: &[&SegmentRecord]) -> bool {
    let mut ranges: Vec<(&[u8], &[u8])> = l1
        .iter()
        .map(|s| (s.min_trace_id.as_slice(), s.max_trace_id.as_slice()))
        .collect();
    ranges.sort();
    ranges.windows(2).any(|w| w[1].0 <= w[0].1)
}

fn trace_of(batch: &RecordBatch, i: usize) -> TraceId {
    let a = batch
        .column(batch.schema().index_of(schema::TRACE_ID).unwrap())
        .as_fixed_size_binary();
    a.value(i).try_into().unwrap()
}

impl Lifecycle {
    pub fn new(config: Config, metastore: MetastoreRef, segments: SegmentStoreRef) -> LifecycleRef {
        Arc::new(Lifecycle {
            config,
            metastore,
            segments,
        })
    }

    async fn scan_all(
        &self,
        segs: &[SegmentRecord],
        pred: &ScanPredicate,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        for s in segs {
            let req = ScanRequest {
                projection: columns.clone(),
                predicate: pred.clone(),
                selection: None,
            };
            let mut b: Vec<RecordBatch> = self
                .segments
                .scan_segment(s, req)
                .await?
                .try_collect()
                .await?;
            out.append(&mut b);
        }
        Ok(out)
    }

    /// Partitions that are due, with what to merge.
    ///
    /// - A closed hour with any L0 or overlapping L1 runs is rewritten whole into disjoint L1
    ///   segments (as is every partition when `force`).
    /// - An open hour merges its L0s into a new L1 run once it has `min_l0_segments` of them,
    ///   and merges L1 runs size-tiered: four or more runs of a similar size become one. Write
    ///   amplification stays logarithmic in the hour's size.
    pub async fn due_partitions(
        &self,
        project: Option<&str>,
        force: bool,
    ) -> Result<Vec<(String, i64, MergePlan)>> {
        let live = self
            .metastore
            .list_segments(&SegmentFilter {
                project_id: project.map(str::to_string),
                states: Some(vec![SegmentState::Live]),
                ..Default::default()
            })
            .await?;
        let mut by_part: BTreeMap<(String, i64), Vec<&SegmentRecord>> = BTreeMap::new();
        for s in &live {
            by_part
                .entry((s.project_id.clone(), s.partition_hour))
                .or_default()
                .push(s);
        }
        // An hour is closed once it ended more than five minutes ago (allowing stragglers).
        let now = now_ns();
        let closed = |hour: i64| hour + HOUR_NS + 5 * 60 * 1_000_000_000 <= now;
        let mut out = Vec::new();
        for ((p, hour), segs) in by_part {
            let l0: Vec<&SegmentRecord> = segs.iter().copied().filter(|s| s.level == 0).collect();
            let l1: Vec<&SegmentRecord> = segs.iter().copied().filter(|s| s.level == 1).collect();
            if force || closed(hour) {
                if !l0.is_empty() || l1_overlap(&l1) {
                    out.push((p, hour, MergePlan::Full));
                }
                continue;
            }
            let mut subset: Vec<String> = Vec::new();
            if l0.len() >= self.config.compaction.min_l0_segments {
                subset.extend(l0.iter().map(|s| s.id.clone()));
            }
            if let Some(tier) = tier_to_merge(&l1) {
                subset.extend(tier);
            }
            if !subset.is_empty() {
                out.push((p, hour, MergePlan::Subset(subset)));
            }
        }
        Ok(out)
    }

    /// Compact every due partition (all of a project's when `force`).
    pub async fn compact(&self, project: Option<&str>, force: bool) -> Result<CompactionReport> {
        let parts = self.due_partitions(project, force).await?;
        let mut total = CompactionReport::default();
        let concurrency = self.config.compaction.concurrency.max(1);
        let results: Vec<Option<CompactionReport>> = futures::stream::iter(
            parts
                .into_iter()
                .map(|(p, h, plan)| async move { self.compact_partition(&p, h, &plan).await }),
        )
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
        for r in results.into_iter().flatten() {
            total.partitions += r.partitions;
            total.inputs += r.inputs;
            total.outputs += r.outputs;
            total.rows_in += r.rows_in;
            total.rows_out += r.rows_out;
            total.dotted_filled += r.dotted_filled;
        }
        Ok(total)
    }

    /// Compact one partition according to `plan`. `None` when there is nothing to do or another
    /// compactor holds the lease.
    pub async fn compact_partition(
        &self,
        project: &str,
        hour: i64,
        plan: &MergePlan,
    ) -> Result<Option<CompactionReport>> {
        let mut inputs = self
            .metastore
            .list_segments(&SegmentFilter {
                project_id: Some(project.to_string()),
                partition_hour: Some(hour),
                states: Some(vec![SegmentState::Live]),
                ..Default::default()
            })
            .await?;
        let all = inputs.len();
        if let MergePlan::Subset(ids) = plan {
            inputs.retain(|s| ids.contains(&s.id));
        }
        if inputs.is_empty() || (inputs.len() == 1 && inputs[0].level == 1) {
            return Ok(None);
        }
        // A partial merge must keep tombstones when older versions may live in segments that
        // are not part of it.
        let keep_tombstones = inputs.len() < all;
        let ids: Vec<String> = inputs.iter().map(|s| s.id.clone()).collect();
        let Some(job) = self
            .metastore
            .lease_compaction(project, hour, &ids, 30 * 60 * 1000)
            .await?
        else {
            return Ok(None);
        };
        match self.rewrite(project, hour, &inputs, keep_tombstones).await {
            Ok((outputs, report)) => {
                let out_ids: Vec<String> = outputs.iter().map(|s| s.record.id.clone()).collect();
                self.metastore.commit_segments(outputs, ids).await?;
                self.metastore
                    .finish_compaction(&job.id, &out_ids, "done")
                    .await?;
                tracing::info!(
                    project,
                    hour,
                    inputs = report.inputs,
                    outputs = report.outputs,
                    rows = report.rows_out,
                    "compacted partition"
                );
                Ok(Some(report))
            }
            Err(e) => {
                tracing::error!(project, hour, error = %e, "compaction failed");
                self.metastore
                    .finish_compaction(&job.id, &[], "failed")
                    .await?;
                Err(e)
            }
        }
    }

    /// Split the partition's traces into ranges of roughly `target_segment` bytes.
    async fn trace_ranges(&self, inputs: &[SegmentRecord]) -> Result<Vec<(TraceId, TraceId)>> {
        let total_bytes: u64 = inputs.iter().map(|s| s.byte_size).sum();
        let total_rows: u64 = inputs.iter().map(|s| s.row_count).sum();
        let target = self.config.compaction.target_segment.bytes().max(1);
        let max_rows = self.config.compaction.max_rows_per_merge.max(1) as u64;
        let n_ranges = total_bytes
            .div_ceil(target)
            .max(total_rows.div_ceil(max_rows))
            .max(1) as usize;
        if n_ranges == 1 {
            return Ok(vec![([0; 16], [0xff; 16])]);
        }
        let batches = self
            .scan_all(
                inputs,
                &ScanPredicate::default(),
                Some(vec![schema::TRACE_ID.to_string()]),
            )
            .await?;
        let mut traces: Vec<TraceId> = Vec::new();
        for b in &batches {
            for i in 0..b.num_rows() {
                traces.push(trace_of(b, i));
            }
        }
        traces.sort_unstable();
        traces.dedup();
        let per = traces.len().div_ceil(n_ranges).max(1);
        let mut ranges = Vec::new();
        for (k, chunk) in traces.chunks(per).enumerate() {
            let lo = if k == 0 { [0; 16] } else { chunk[0] };
            ranges.push((lo, *chunk.last().unwrap()));
        }
        if let Some(last) = ranges.last_mut() {
            last.1 = [0xff; 16];
        }
        Ok(ranges)
    }

    async fn rewrite(
        &self,
        project: &str,
        hour: i64,
        inputs: &[SegmentRecord],
        keep_tombstones: bool,
    ) -> Result<(Vec<NewSegment>, CompactionReport)> {
        let mut report = CompactionReport {
            partitions: 1,
            inputs: inputs.len(),
            ..Default::default()
        };
        let mut outputs = Vec::new();
        for (lo, hi) in self.trace_ranges(inputs).await? {
            let pred = ScanPredicate {
                trace_min: Some(lo),
                trace_max: Some(hi),
                ..Default::default()
            };
            let batches = self.scan_all(inputs, &pred, None).await?;
            report.rows_in += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            if batches.is_empty() {
                continue;
            }
            let all = concat_batches(&spans_schema(), &batches)?;
            let mut merged = merge_versions(&all, MergeOptions { keep_tombstones })?;
            let (filled, b) = self.backfill_dotted(project, hour, merged).await?;
            merged = b;
            report.dotted_filled += filled;
            if merged.num_rows() == 0 {
                continue;
            }
            report.rows_out += merged.num_rows() as u64;
            outputs.push(
                writer::write_partition(self.segments.as_ref(), project, 1, hour, &merged, true)
                    .await?,
            );
        }
        report.outputs = outputs.len();
        Ok((outputs, report))
    }

    /// Fill missing `dotted_order`, looking up ancestors in other partitions when needed.
    async fn backfill_dotted(
        &self,
        project: &str,
        hour: i64,
        batch: RecordBatch,
    ) -> Result<(u64, RecordBatch)> {
        let d = batch.column(batch.schema().index_of(schema::DOTTED_ORDER)?);
        let missing_before = d.null_count();
        if missing_before == 0 {
            return Ok((0, batch));
        }
        let mut rows = batch_to_rows(&batch);
        dotted::fill(&mut rows, &|_, _| None);
        // Parents outside this partition: fetch their dotted orders.
        let present: HashSet<(TraceId, SpanId)> =
            rows.iter().map(|r| (r.trace_id, r.span_id)).collect();
        let mut wanted: HashMap<TraceId, HashSet<SpanId>> = HashMap::new();
        for r in &rows {
            if r.dotted_order.is_none()
                && let Some(p) = r.parent_span_id
                && !present.contains(&(r.trace_id, p))
            {
                wanted.entry(r.trace_id).or_default().insert(p);
            }
        }
        let mut external: HashMap<(TraceId, SpanId), String> = HashMap::new();
        for (trace, spans) in wanted {
            let segs: Vec<SegmentRecord> = self
                .metastore
                .segments_for_trace(project, &trace)
                .await?
                .into_iter()
                .filter(|s| s.partition_hour != hour)
                .collect();
            if segs.is_empty() {
                continue;
            }
            let pred = ScanPredicate {
                project_id: Some(project.to_string()),
                trace_ids: Some(vec![trace]),
                ..Default::default()
            };
            let cols = Some(
                [
                    schema::PROJECT_ID,
                    schema::TRACE_ID,
                    schema::SPAN_ID,
                    schema::PARENT_SPAN_ID,
                    schema::DOTTED_ORDER,
                    schema::START_TIME,
                    schema::VERSION,
                    schema::DELETED,
                ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            );
            let b = self.scan_all(&segs, &pred, cols).await?;
            for r in b.iter().flat_map(batch_to_rows) {
                if spans.contains(&r.span_id)
                    && let Some(d) = r.dotted_order
                {
                    external.insert((r.trace_id, r.span_id), d);
                }
            }
        }
        if !external.is_empty() {
            dotted::fill(&mut rows, &|t, s| external.get(&(*t, *s)).cloned());
        }
        let missing_after = rows.iter().filter(|r| r.dotted_order.is_none()).count();
        let out = rows_to_batch(&rows)?;
        Ok(((missing_before - missing_after) as u64, out))
    }

    /// Expire partitions past each project's retention.
    pub async fn retention(&self) -> Result<usize> {
        let mut n = 0;
        for p in self.metastore.list_projects().await? {
            if let Some(days) = p.retention_days {
                let cutoff = hour_of(now_ns() - days as i64 * 24 * HOUR_NS);
                let expired = self.metastore.expire_partitions(&p.id, cutoff).await?;
                if expired > 0 {
                    tracing::info!(project = %p.id, segments = expired, "expired partitions past retention");
                }
                n += expired;
            }
        }
        Ok(n)
    }

    /// Remove objects of deleted segments after the grace period, orphans (objects no
    /// metastore row names) after `orphan_age`, and blobs past their project's retention.
    pub async fn gc(&self) -> Result<GcReport> {
        let mut report = GcReport::default();
        let now = now_ms();
        let grace = self.config.compaction.gc_grace.get().as_millis() as i64;
        let deleted = self
            .metastore
            .list_segments(&SegmentFilter {
                states: Some(vec![SegmentState::Deleted]),
                ..Default::default()
            })
            .await?;
        let mut purge = Vec::new();
        for s in deleted {
            if s.deleted_at_ms.is_some_and(|d| d + grace <= now) {
                self.segments.delete_object(&s.path).await?;
                if let Some(f) = &s.fts_path {
                    self.segments.delete_object(f).await?;
                }
                purge.push(s.id);
            }
        }
        if !purge.is_empty() {
            self.metastore.purge_segments(&purge).await?;
            report.segments_purged = purge.len();
        }

        let known = self.metastore.all_object_paths().await?;
        let orphan_age = self.config.compaction.orphan_age.get().as_millis() as i64;
        for (path, modified, _) in self
            .segments
            .list_objects(forge_storage::paths::SEGMENTS_PREFIX)
            .await?
        {
            if !known.contains(&path) && modified + orphan_age <= now {
                self.segments.delete_object(&path).await?;
                report.orphans_deleted += 1;
            }
        }

        let retention: HashMap<String, u32> = self
            .metastore
            .list_projects()
            .await?
            .into_iter()
            .filter_map(|p| p.retention_days.map(|d| (p.id, d)))
            .collect();
        if !retention.is_empty() {
            for (path, modified, _) in self
                .segments
                .list_objects(forge_storage::paths::BLOBS_PREFIX)
                .await?
            {
                let project = path.split('/').nth(1).unwrap_or_default();
                if let Some(days) = retention.get(project) {
                    // Blobs are re-written on every ingest that references them, so their
                    // modification time tracks their newest reference.
                    if modified + *days as i64 * 86_400_000 <= now {
                        self.segments.delete_object(&path).await?;
                        report.blobs_deleted += 1;
                    }
                }
            }
        }
        Ok(report)
    }

    /// Run compaction, retention and GC every `compaction.interval` until shutdown.
    pub fn spawn(
        self: &Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        let mut shutdown = shutdown;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(
                me.config
                    .compaction
                    .interval
                    .get()
                    .max(Duration::from_secs(1)),
            );
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = shutdown.changed() => break,
                }
                if let Err(e) = me.compact(None, false).await {
                    tracing::error!(error = %e, "compaction round failed");
                }
                match me.retention().await {
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "retention failed"),
                }
                if let Err(e) = me.gc().await {
                    tracing::error!(error = %e, "gc failed");
                }
            }
        })
    }
}
