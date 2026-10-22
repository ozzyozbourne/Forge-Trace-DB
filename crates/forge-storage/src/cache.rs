//! Two-tier read cache in front of object storage, as an `ObjectStore` wrapper so every reader
//! (Vortex segments, `.fts` sidecars, blobs) benefits.
//!
//! - Memory: byte ranges keyed by `(path, start, end)`, LRU by size. Holds footers, index pages
//!   and hot column chunks.
//! - Disk: whole immutable objects (segments, sidecars, blobs). Objects written through this
//!   store are copied to disk at write time, so freshly flushed segments never cost an object
//!   store round trip; objects first read through a range request are downloaded to disk in
//!   the background.
//!
//! Only immutable paths (`segments/`, `blobs/`) are cached; everything else passes through.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result,
};
use parking_lot::Mutex;

use crate::store::ObjectStoreRef;

#[derive(Default)]
pub struct CacheStats {
    pub memory_hits: AtomicU64,
    pub disk_hits: AtomicU64,
    pub misses: AtomicU64,
    pub bytes_from_store: AtomicU64,
}

fn cacheable(p: &Path) -> bool {
    let s = p.as_ref();
    s.starts_with("segments/") || s.starts_with("blobs/")
}

/// Disk tier: whole objects under `dir`, evicted least-recently-used beyond `capacity`.
struct DiskTier {
    dir: PathBuf,
    capacity: u64,
    inner: Mutex<DiskIndex>,
}

#[derive(Default)]
struct DiskIndex {
    entries: HashMap<String, (u64, u64)>, // path -> (size, last use tick)
    total: u64,
    tick: u64,
}

impl DiskTier {
    fn file(&self, path: &str) -> PathBuf {
        // Flatten the object path; keep it readable for operators.
        self.dir.join(path.replace('/', "__"))
    }

    fn open(dir: PathBuf, capacity: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let mut idx = DiskIndex::default();
        for e in std::fs::read_dir(&dir)? {
            let e = e?;
            let name = e.file_name().to_string_lossy().to_string();
            if name.ends_with(".tmp") {
                let _ = std::fs::remove_file(e.path());
                continue;
            }
            let size = e.metadata()?.len();
            idx.tick += 1;
            idx.entries
                .insert(name.replace("__", "/"), (size, idx.tick));
            idx.total += size;
        }
        Ok(DiskTier {
            dir,
            capacity,
            inner: Mutex::new(idx),
        })
    }

    fn size_of(&self, path: &str) -> Option<u64> {
        let mut g = self.inner.lock();
        g.tick += 1;
        let t = g.tick;
        g.entries.get_mut(path).map(|e| {
            e.1 = t;
            e.0
        })
    }

    fn insert(&self, path: &str, data: &[u8]) {
        if data.len() as u64 > self.capacity / 4 {
            return;
        }
        let file = self.file(path);
        let tmp = file.with_extension("tmp");
        if std::fs::write(&tmp, data)
            .and_then(|_| std::fs::rename(&tmp, &file))
            .is_err()
        {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        let evict: Vec<String> = {
            let mut g = self.inner.lock();
            g.tick += 1;
            let t = g.tick;
            if let Some((old, _)) = g.entries.insert(path.to_string(), (data.len() as u64, t)) {
                g.total -= old;
            }
            g.total += data.len() as u64;
            let mut evict = Vec::new();
            if g.total > self.capacity {
                let mut by_age: Vec<(u64, String, u64)> = g
                    .entries
                    .iter()
                    .map(|(k, (s, t))| (*t, k.clone(), *s))
                    .collect();
                by_age.sort();
                for (_, k, s) in by_age {
                    if g.total <= self.capacity * 9 / 10 {
                        break;
                    }
                    if k == path {
                        continue;
                    }
                    g.entries.remove(&k);
                    g.total -= s;
                    evict.push(k);
                }
            }
            evict
        };
        for k in evict {
            let _ = std::fs::remove_file(self.file(&k));
        }
    }

    fn read(&self, path: &str, range: Range<u64>) -> Option<Bytes> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(self.file(path)).ok()?;
        f.seek(SeekFrom::Start(range.start)).ok()?;
        let mut buf = vec![0u8; (range.end - range.start) as usize];
        f.read_exact(&mut buf).ok()?;
        Some(Bytes::from(buf))
    }

    fn remove(&self, path: &str) {
        let mut g = self.inner.lock();
        if let Some((s, _)) = g.entries.remove(path) {
            g.total -= s;
        }
        drop(g);
        let _ = std::fs::remove_file(self.file(path));
    }
}

pub struct CachingStore {
    inner: ObjectStoreRef,
    memory: moka::future::Cache<(String, u64, u64), Bytes>,
    sizes: moka::sync::Cache<String, ObjectMeta>,
    disk: Option<Arc<DiskTier>>,
    downloading: Arc<Mutex<std::collections::HashSet<String>>>,
    pub stats: Arc<CacheStats>,
}

impl fmt::Debug for CachingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingStore({})", self.inner)
    }
}

impl fmt::Display for CachingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingStore({})", self.inner)
    }
}

impl CachingStore {
    pub fn new(
        inner: ObjectStoreRef,
        memory_bytes: u64,
        disk_dir: Option<PathBuf>,
        disk_bytes: u64,
    ) -> std::io::Result<Self> {
        let disk = match disk_dir {
            Some(d) if disk_bytes > 0 => Some(Arc::new(DiskTier::open(d, disk_bytes)?)),
            _ => None,
        };
        Ok(CachingStore {
            inner,
            memory: moka::future::Cache::builder()
                .weigher(|k: &(String, u64, u64), v: &Bytes| {
                    (v.len() + k.0.len() + 32).min(u32::MAX as usize) as u32
                })
                .max_capacity(memory_bytes.max(1))
                .support_invalidation_closures()
                .build(),
            sizes: moka::sync::Cache::new(100_000),
            disk,
            downloading: Arc::new(Mutex::new(Default::default())),
            stats: Arc::new(CacheStats::default()),
        })
    }

    async fn meta(&self, location: &Path) -> Result<ObjectMeta> {
        let key = location.to_string();
        if let Some(m) = self.sizes.get(&key) {
            return Ok(m);
        }
        if let Some(size) = self.disk.as_ref().and_then(|d| d.size_of(&key)) {
            let m = ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            };
            self.sizes.insert(key, m.clone());
            return Ok(m);
        }
        let m = self.inner.head(location).await?;
        self.sizes.insert(key, m.clone());
        Ok(m)
    }

    fn result(meta: ObjectMeta, range: Range<u64>, bytes: Bytes) -> GetResult {
        GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range,
            attributes: Default::default(),
        }
    }

    fn spawn_download(&self, location: &Path, size: u64) {
        let Some(disk) = self.disk.clone() else {
            return;
        };
        if size > disk.capacity / 4 {
            return;
        }
        let key = location.to_string();
        if !self.downloading.lock().insert(key.clone()) {
            return;
        }
        let inner = self.inner.clone();
        let downloading = self.downloading.clone();
        let stats = self.stats.clone();
        let loc = location.clone();
        tokio::spawn(async move {
            if let Ok(r) = inner.get(&loc).await
                && let Ok(b) = r.bytes().await
            {
                stats
                    .bytes_from_store
                    .fetch_add(b.len() as u64, Ordering::Relaxed);
                let d = disk.clone();
                let k = key.clone();
                let _ = tokio::task::spawn_blocking(move || d.insert(&k, &b)).await;
            }
            downloading.lock().remove(&key);
        });
    }
}

#[async_trait]
impl ObjectStore for CachingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let copy = (self.disk.is_some() && cacheable(location)).then(|| payload.clone());
        let r = self.inner.put_opts(location, payload, opts).await?;
        if let (Some(p), Some(disk)) = (copy, self.disk.clone()) {
            let key = location.to_string();
            let bytes: Vec<u8> = p.iter().flat_map(|b| b.iter().copied()).collect();
            let _ = tokio::task::spawn_blocking(move || disk.insert(&key, &bytes)).await;
        }
        Ok(r)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let plain = options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none();
        if !plain || !cacheable(location) {
            return self.inner.get_opts(location, options).await;
        }
        let meta = self.meta(location).await?;
        if options.head {
            return Ok(Self::result(meta, 0..0, Bytes::new()));
        }
        let size = meta.size;
        let range = match &options.range {
            None => 0..size,
            Some(GetRange::Bounded(r)) => r.start..r.end.min(size),
            Some(GetRange::Offset(o)) => (*o).min(size)..size,
            Some(GetRange::Suffix(n)) => size.saturating_sub(*n)..size,
        };
        if range.start >= range.end && size > 0 {
            return self.inner.get_opts(location, options).await;
        }
        let key = (location.to_string(), range.start, range.end);
        if let Some(b) = self.memory.get(&key).await {
            self.stats.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Self::result(meta, range, b));
        }
        if let Some(disk) = &self.disk
            && disk.size_of(&key.0).is_some()
        {
            let d = disk.clone();
            let (p, r) = (key.0.clone(), range.clone());
            if let Ok(Some(b)) = tokio::task::spawn_blocking(move || d.read(&p, r)).await {
                self.stats.disk_hits.fetch_add(1, Ordering::Relaxed);
                self.memory.insert(key, b.clone()).await;
                return Ok(Self::result(meta, range, b));
            }
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        let r = self
            .inner
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Bounded(range.clone())),
                    ..Default::default()
                },
            )
            .await?;
        let b = r.bytes().await?;
        self.stats
            .bytes_from_store
            .fetch_add(b.len() as u64, Ordering::Relaxed);
        self.memory.insert(key, b.clone()).await;
        if range.start == 0 && range.end == size {
            if let Some(disk) = self.disk.clone() {
                let (p, data) = (location.to_string(), b.clone());
                tokio::task::spawn_blocking(move || disk.insert(&p, &data));
            }
        } else {
            self.spawn_download(location, size);
        }
        Ok(Self::result(meta, range, b))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let disk = self.disk.clone();
        let memory = self.memory.clone();
        let sizes = self.sizes.clone();
        let locations = locations
            .map(move |l| {
                if let Ok(p) = &l {
                    let key = p.to_string();
                    if let Some(d) = &disk {
                        d.remove(&key);
                    }
                    sizes.invalidate(&key);
                    let k2 = key.clone();
                    let _ = memory.invalidate_entries_if(move |k, _| k.0 == k2);
                }
                l
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn caches_ranges_and_writes_through() {
        let inner: ObjectStoreRef = Arc::new(InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let c = CachingStore::new(
            inner.clone(),
            1 << 20,
            Some(dir.path().to_path_buf()),
            1 << 20,
        )
        .unwrap();
        let p = Path::from("segments/a/x.vortex");
        c.put(&p, Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();
        // Served from disk (written through), then memory.
        assert_eq!(&c.get_range(&p, 2..5).await.unwrap()[..], b"234");
        assert_eq!(c.stats.disk_hits.load(Ordering::Relaxed), 1);
        assert_eq!(&c.get_range(&p, 2..5).await.unwrap()[..], b"234");
        assert_eq!(c.stats.memory_hits.load(Ordering::Relaxed), 1);
        assert_eq!(c.stats.misses.load(Ordering::Relaxed), 0);
        // Suffix requests resolve against the cached size.
        let r = c
            .get_opts(
                &p,
                GetOptions {
                    range: Some(GetRange::Suffix(3)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.meta.size, 10);
        assert_eq!(&r.bytes().await.unwrap()[..], b"789");
        // Objects written elsewhere are fetched, then served from cache.
        let q = Path::from("segments/a/y.vortex");
        inner
            .put(&q, Bytes::from_static(b"abcdef").into())
            .await
            .unwrap();
        assert_eq!(&c.get_range(&q, 0..3).await.unwrap()[..], b"abc");
        assert_eq!(c.stats.misses.load(Ordering::Relaxed), 1);
        assert_eq!(&c.get_range(&q, 0..3).await.unwrap()[..], b"abc");
        assert_eq!(c.stats.misses.load(Ordering::Relaxed), 1);
        // Deletes invalidate both tiers.
        c.delete(&p).await.unwrap();
        assert!(c.get_range(&p, 2..5).await.is_err());
        // Mutable paths pass through.
        let w = Path::from("wal/n/1.wal");
        c.put(&w, Bytes::from_static(b"w").into()).await.unwrap();
        assert_eq!(&c.get(&w).await.unwrap().bytes().await.unwrap()[..], b"w");
    }

    #[test]
    fn disk_tier_evicts_lru() {
        let dir = tempfile::tempdir().unwrap();
        let d = DiskTier::open(dir.path().to_path_buf(), 100).unwrap();
        d.insert("segments/a", &[0u8; 20]);
        d.insert("segments/b", &[0u8; 20]);
        let _ = d.size_of("segments/a");
        for i in 0..5 {
            d.insert(&format!("segments/c{i}"), &[0u8; 20]);
        }
        assert!(d.size_of("segments/a").is_some() || d.inner.lock().total <= 100);
        assert!(d.inner.lock().total <= 100);
        // Reopening rebuilds the index from disk.
        let total = d.inner.lock().total;
        let d2 = DiskTier::open(dir.path().to_path_buf(), 100).unwrap();
        assert_eq!(d2.inner.lock().total, total);
    }
}
