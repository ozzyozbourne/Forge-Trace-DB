//! The write path: normalize → WAL → memtable → flush → metastore commit → WAL truncate.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use forge_model::config::{Config, WalMode};
use forge_model::merge::{MergeOptions, merge_versions};
use forge_model::row::{SpanId, SpanRow, TraceId, rows_to_batch};
use forge_model::schema::spans_schema;
use forge_model::traits::{MetastoreRef, SegmentStoreRef};
use forge_model::{ForgeError, Result, dotted, now_ns};
use forge_storage::{ObjectStoreRef, blob};
use parking_lot::RwLock;
use tokio::sync::Mutex;

use crate::memtable::{MemSnapshot, Memtable, Memtables};
use crate::wal::{Wal, WalOptions, decode_batches, encode_batch};
use crate::writer;

/// Counters exported as metrics.
#[derive(Default)]
pub struct IngestStats {
    pub spans: AtomicU64,
    pub requests: AtomicU64,
    pub rejected: AtomicU64,
    pub flushes: AtomicU64,
    pub flush_errors: AtomicU64,
    pub segments_written: AtomicU64,
    pub bytes_flushed: AtomicU64,
}

pub struct Ingester {
    config: Config,
    metastore: MetastoreRef,
    segments: SegmentStoreRef,
    wal: Wal,
    memtables: RwLock<Memtables>,
    /// Held shared across WAL append + memtable insert, exclusive across freeze, so every WAL
    /// record lands in the memtable whose boundary covers it.
    rotation: tokio::sync::RwLock<()>,
    flush_lock: Mutex<()>,
    version: AtomicI64,
    dotted_cache: moka::sync::Cache<(String, TraceId, SpanId), String>,
    pub stats: IngestStats,
}

pub type IngesterRef = Arc<Ingester>;

impl Ingester {
    /// Open the WAL, replay it into the memtable, and pick a version above everything seen.
    pub async fn open(
        config: Config,
        metastore: MetastoreRef,
        segments: SegmentStoreRef,
        object_store: ObjectStoreRef,
    ) -> Result<IngesterRef> {
        let opts = WalOptions {
            commit_interval: config.wal.commit_interval.get(),
            commit_bytes: config.wal.commit_bytes.bytes() as usize,
        };
        let (wal, replay, _) = match config.wal.mode {
            WalMode::Local => Wal::open_local(std::path::Path::new(&config.wal.dir), opts).await?,
            WalMode::ObjectStore => {
                Wal::open_object_store(object_store, &config.server.node_id, opts).await?
            }
        };
        let memtables = Memtables::default();
        let mut max_version = metastore.max_version().await?;
        let mut replayed = 0usize;
        for payload in &replay {
            for b in decode_batches(payload)? {
                if let Some(v) = forge_model::merge::max_version(&b) {
                    max_version = max_version.max(v);
                }
                replayed += b.num_rows();
                memtables.active.insert(b);
            }
        }
        if replayed > 0 {
            tracing::info!(
                rows = replayed,
                records = replay.len(),
                "replayed WAL into memtable"
            );
        }
        Ok(Arc::new(Ingester {
            config,
            metastore,
            segments,
            wal,
            memtables: RwLock::new(memtables),
            rotation: tokio::sync::RwLock::new(()),
            flush_lock: Mutex::new(()),
            version: AtomicI64::new(max_version),
            dotted_cache: moka::sync::Cache::builder()
                .weigher(|k: &(String, TraceId, SpanId), v: &String| {
                    (k.0.len() + v.len() + 64) as u32
                })
                .max_capacity(64 << 20)
                .build(),
            stats: IngestStats::default(),
        }))
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn segments(&self) -> &SegmentStoreRef {
        &self.segments
    }

    pub fn metastore(&self) -> &MetastoreRef {
        &self.metastore
    }

    /// Reserve `n` versions. Versions are nanosecond timestamps made strictly increasing, so
    /// they order writes across restarts and double as ingest time.
    fn next_versions(&self, n: usize) -> i64 {
        let n = n.max(1) as i64;
        loop {
            let cur = self.version.load(Ordering::Acquire);
            let base = (cur + 1).max(now_ns());
            if self
                .version
                .compare_exchange(cur, base + n - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return base;
            }
        }
    }

    pub fn buffered_bytes(&self) -> usize {
        self.memtables.read().total_bytes()
    }

    /// Error if unflushed memtables exceed the memory budget.
    pub fn check_backpressure(&self) -> Result<()> {
        let used = self.buffered_bytes() as u64;
        if used > self.config.memtable.memory_budget.bytes() {
            self.stats.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(ForgeError::Backpressure(format!(
                "{used} bytes buffered exceeds the memtable budget; retry later"
            )));
        }
        Ok(())
    }

    /// Ingest normalized rows. Returns once they are durable and queryable.
    pub async fn ingest(&self, mut rows: Vec<SpanRow>) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.check_backpressure()?;
        for r in &rows {
            forge_storage::paths::validate_project_id(&r.project_id)?;
        }

        // Large payloads go to blob objects.
        let threshold = self.config.ingest.blob_threshold.bytes() as usize;
        for r in rows.iter_mut() {
            for field in [
                &mut r.inputs,
                &mut r.outputs,
                &mut r.events,
                &mut r.attributes,
            ] {
                if let Some(v) = field.take() {
                    *field = Some(
                        blob::maybe_offload(self.segments.as_ref(), &r.project_id, v, threshold)
                            .await?,
                    );
                }
            }
        }

        // dotted_order from parents in this batch or recently seen.
        {
            let cache = &self.dotted_cache;
            let project = rows[0].project_id.clone();
            let single_project = rows.iter().all(|r| r.project_id == project);
            if single_project {
                dotted::fill(&mut rows, &|t, s| cache.get(&(project.clone(), *t, *s)));
            } else {
                // Rare: group by project so lookups use the right key.
                let mut by_project: std::collections::BTreeMap<String, Vec<SpanRow>> =
                    Default::default();
                for r in rows.drain(..) {
                    by_project.entry(r.project_id.clone()).or_default().push(r);
                }
                for (p, mut group) in by_project {
                    dotted::fill(&mut group, &|t, s| cache.get(&(p.clone(), *t, *s)));
                    rows.extend(group);
                }
            }
            for r in &rows {
                if let Some(d) = &r.dotted_order {
                    cache.insert((r.project_id.clone(), r.trace_id, r.span_id), d.clone());
                }
            }
        }

        let base = self.next_versions(rows.len());
        for (i, r) in rows.iter_mut().enumerate() {
            r.version = base + i as i64;
        }
        let n = rows.len();
        let batch = rows_to_batch(&rows)?;
        let payload = encode_batch(&batch)?;

        let _guard = self.rotation.read().await;
        self.wal.append(payload).await?;
        self.memtables.read().active.insert(batch);
        self.stats.spans.fetch_add(n as u64, Ordering::Relaxed);
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        Ok(n)
    }

    /// What queries see of unflushed data. Take this *before* listing segments.
    pub fn snapshot(&self) -> MemSnapshot {
        self.memtables.read().snapshot()
    }

    /// Freeze the active memtable if it is non-empty. Returns the frozen table.
    async fn freeze(&self) -> Result<Option<Arc<Memtable>>> {
        let _guard = self.rotation.write().await;
        if self.memtables.read().active.is_empty() {
            return Ok(None);
        }
        let boundary = self.wal.rotate().await?;
        let frozen = self.memtables.write().freeze();
        frozen.set_wal_boundary(boundary);
        Ok(Some(frozen))
    }

    fn should_freeze(&self) -> bool {
        let g = self.memtables.read();
        let a = &g.active;
        !a.is_empty()
            && (a.bytes() as u64 >= self.config.memtable.flush_bytes.bytes()
                || a.age() >= self.config.memtable.flush_interval.get())
    }

    /// Write one frozen memtable to L0 segments and commit them.
    async fn flush_memtable(&self, m: &Arc<Memtable>) -> Result<usize> {
        let batches = m.batches();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut new_segments = Vec::new();
        if rows > 0 {
            let all = concat_batches(&spans_schema(), &batches)?;
            // Collapse versions within the memtable; tombstones must survive to shadow older
            // segments.
            let merged = merge_versions(
                &all,
                MergeOptions {
                    keep_tombstones: true,
                },
            )?;
            for ((project, hour), part) in writer::partition(&merged)? {
                let seg =
                    writer::write_partition(self.segments.as_ref(), &project, 0, hour, &part, true)
                        .await?;
                self.stats
                    .bytes_flushed
                    .fetch_add(seg.record.byte_size, Ordering::Relaxed);
                new_segments.push(seg);
            }
        }
        let count = new_segments.len();
        self.metastore.commit_segments(new_segments, vec![]).await?;
        if let Some(b) = m.wal_boundary() {
            self.wal.truncate_through(b).await?;
        }
        self.memtables.write().remove_frozen(m.id);
        self.stats.flushes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .segments_written
            .fetch_add(count as u64, Ordering::Relaxed);
        tracing::debug!(memtable = m.id, rows, segments = count, "flushed memtable");
        Ok(count)
    }

    /// Flush every frozen memtable, oldest first. Stops at the first failure; the table stays
    /// frozen and queryable and is retried later.
    async fn flush_frozen(&self) -> Result<usize> {
        let mut total = 0;
        loop {
            let next = self.memtables.read().frozen.first().cloned();
            let Some(m) = next else { break };
            match self.flush_memtable(&m).await {
                Ok(n) => total += n,
                Err(e) => {
                    self.stats.flush_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
        Ok(total)
    }

    /// Freeze and flush everything now. Returns the number of segments written.
    pub async fn flush_now(&self) -> Result<usize> {
        let _g = self.flush_lock.lock().await;
        self.freeze().await?;
        self.flush_frozen().await
    }

    /// Background flusher: freezes on size or age, then flushes.
    pub fn spawn_flusher(
        self: &Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        let mut shutdown = shutdown;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(200));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut backoff = Duration::from_millis(200);
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = shutdown.changed() => break,
                }
                let _g = me.flush_lock.lock().await;
                if me.should_freeze()
                    && let Err(e) = me.freeze().await
                {
                    tracing::error!(error = %e, "memtable freeze failed");
                }
                if me.memtables.read().frozen.is_empty() {
                    continue;
                }
                match me.flush_frozen().await {
                    Ok(_) => backoff = Duration::from_millis(200),
                    Err(e) => {
                        tracing::error!(error = %e, "flush failed; retrying");
                        drop(_g);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        })
    }

    /// Recent `dotted_order` for a span, if this process saw it.
    pub fn cached_dotted(&self, project: &str, trace: &TraceId, span: &SpanId) -> Option<String> {
        self.dotted_cache.get(&(project.to_string(), *trace, *span))
    }

    /// Unmerged memtable rows of one trace.
    pub fn memtable_trace(&self, project: &str, trace: &TraceId) -> Vec<RecordBatch> {
        let snap = self.snapshot();
        snap.tables
            .iter()
            .filter_map(|t| t.trace_rows(project, trace))
            .collect()
    }
}
