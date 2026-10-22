//! Storage for Forge: object-store construction, Vortex segment files, oversized payload blobs
//! and the read cache.

pub mod blob;
pub mod cache;
pub mod fault;
pub mod paths;
pub mod segment;
pub mod store;

use std::sync::Arc;

use forge_model::Result;
use forge_model::config::Config;

pub use cache::{CacheStats, CachingStore};
pub use segment::{SESSION, VortexSegmentStore};
pub use store::{ObjectStoreRef, from_url};

/// The stores a process uses, built on a base store.
pub struct Layers {
    /// Retries transient errors; used for the WAL and anything that must not be cached.
    pub direct: ObjectStoreRef,
    /// `direct` behind the memory + disk cache; used for segments, sidecars and blobs.
    pub cached: ObjectStoreRef,
    pub cache_stats: Option<Arc<CacheStats>>,
}

/// Wrap a base store with retries and (when configured) the read cache.
pub fn layer(base: ObjectStoreRef, config: &Config) -> Result<Layers> {
    let direct: ObjectStoreRef = Arc::new(fault::RetryStore::new(base));
    let mem = config.cache.memory.bytes();
    let disk = config.cache.disk.bytes();
    if mem == 0 && disk == 0 {
        return Ok(Layers {
            cached: direct.clone(),
            direct,
            cache_stats: None,
        });
    }
    let dir = (disk > 0).then(|| std::path::PathBuf::from(&config.cache.disk_dir));
    let c = CachingStore::new(direct.clone(), mem, dir, disk)?;
    let stats = c.stats.clone();
    Ok(Layers {
        direct,
        cached: Arc::new(c),
        cache_stats: Some(stats),
    })
}
