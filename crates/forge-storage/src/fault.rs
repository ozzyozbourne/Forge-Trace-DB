//! Resilience wrappers around `ObjectStore`:
//!
//! - [`RetryStore`] retries transient failures (anything but not-found, precondition and
//!   already-exists errors) with exponential backoff. Cloud backends retry HTTP-level errors
//!   themselves; this also covers the local backend and errors surfacing mid-stream.
//! - [`FaultyStore`] injects latency, transient errors ("503s") and partial failures (a put
//!   that lands but reports an error) for tests.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use parking_lot::Mutex;
use rand::Rng;

use crate::store::ObjectStoreRef;

fn transient(e: &Error) -> bool {
    !matches!(
        e,
        Error::NotFound { .. }
            | Error::Precondition { .. }
            | Error::AlreadyExists { .. }
            | Error::NotModified { .. }
            | Error::NotImplemented { .. }
    )
}

pub struct RetryStore {
    inner: ObjectStoreRef,
    attempts: u32,
    base: Duration,
}

impl RetryStore {
    pub fn new(inner: ObjectStoreRef) -> Self {
        RetryStore {
            inner,
            attempts: 4,
            base: Duration::from_millis(50),
        }
    }

    pub fn with_policy(inner: ObjectStoreRef, attempts: u32, base: Duration) -> Self {
        RetryStore {
            inner,
            attempts: attempts.max(1),
            base,
        }
    }

    async fn retry<T, F, Fut>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut delay = self.base;
        let mut attempt = 1;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) if attempt < self.attempts && transient(&e) => {
                    tracing::debug!(attempt, error = %e, "retrying object store call");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl fmt::Debug for RetryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RetryStore({})", self.inner)
    }
}

impl fmt::Display for RetryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RetryStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RetryStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.retry(|| self.inner.put_opts(location, payload.clone(), opts.clone()))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // Buffer the body so errors mid-stream are retried too.
        let r = self
            .retry(|| async {
                let r = self.inner.get_opts(location, options.clone()).await?;
                let (meta, range, attributes) =
                    (r.meta.clone(), r.range.clone(), r.attributes.clone());
                let bytes = r.bytes().await?;
                Ok((meta, range, attributes, bytes))
            })
            .await?;
        let (meta, range, attributes, bytes) = r;
        Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range,
            attributes,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.retry(|| self.inner.list_with_delimiter(prefix)).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.retry(|| self.inner.copy_opts(from, to, options.clone()))
            .await
    }
}

/// Fault knobs, changeable while a test runs.
#[derive(Debug, Clone, Default)]
pub struct Faults {
    pub latency: Duration,
    /// Probability that a get fails before reading.
    pub get_error_rate: f64,
    /// Probability that a put fails without writing.
    pub put_error_rate: f64,
    /// Probability that a put writes the object and then reports failure.
    pub partial_put_rate: f64,
}

#[derive(Default)]
pub struct FaultStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub injected: AtomicU64,
}

pub struct FaultyStore {
    inner: ObjectStoreRef,
    pub faults: Arc<Mutex<Faults>>,
    pub stats: Arc<FaultStats>,
}

impl FaultyStore {
    pub fn new(inner: ObjectStoreRef, faults: Faults) -> Self {
        FaultyStore {
            inner,
            faults: Arc::new(Mutex::new(faults)),
            stats: Arc::new(FaultStats::default()),
        }
    }

    fn injected(&self, what: &str) -> Error {
        self.stats.injected.fetch_add(1, Ordering::Relaxed);
        Error::Generic {
            store: "faulty",
            source: format!("injected {what} failure (503 Slow Down)").into(),
        }
    }

    fn roll(p: f64) -> bool {
        p > 0.0 && rand::rng().random_bool(p.min(1.0))
    }
}

impl fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        let f = self.faults.lock().clone();
        if !f.latency.is_zero() {
            tokio::time::sleep(f.latency).await;
        }
        if Self::roll(f.put_error_rate) {
            return Err(self.injected("put"));
        }
        let r = self.inner.put_opts(location, payload, opts).await?;
        if Self::roll(f.partial_put_rate) {
            return Err(self.injected("partial put"));
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
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        let f = self.faults.lock().clone();
        if !f.latency.is_zero() {
            tokio::time::sleep(f.latency).await;
        }
        if Self::roll(f.get_error_rate) {
            return Err(self.injected("get"));
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
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
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn retries_transient_errors() {
        let inner: ObjectStoreRef = Arc::new(InMemory::new());
        let faulty = Arc::new(FaultyStore::new(
            inner,
            Faults {
                get_error_rate: 0.5,
                put_error_rate: 0.5,
                ..Default::default()
            },
        ));
        let retry = RetryStore::with_policy(faulty.clone(), 30, Duration::from_millis(1));
        let p = Path::from("segments/x");
        for _ in 0..20 {
            retry
                .put(&p, bytes::Bytes::from_static(b"hello").into())
                .await
                .unwrap();
            assert_eq!(
                &retry.get(&p).await.unwrap().bytes().await.unwrap()[..],
                b"hello"
            );
        }
        assert!(faulty.stats.injected.load(Ordering::Relaxed) > 0);
        // Not-found is not retried.
        assert!(matches!(
            retry.get(&Path::from("nope")).await,
            Err(Error::NotFound { .. })
        ));
    }
}
