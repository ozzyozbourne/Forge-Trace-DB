//! Build an `ObjectStore` from a storage URL. All Forge paths are relative to the URL's prefix.

use std::sync::Arc;

use anyhow::Context;
use forge_model::{ForgeError, Result};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::prefix::PrefixStore;
use url::Url;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

/// `file:///abs/dir`, `memory://`, `s3://bucket/prefix`, `gs://bucket/prefix`,
/// `az://container/prefix` (and the other schemes `object_store` parses). `options` are passed
/// to the cloud builders, e.g. `aws_endpoint` and `aws_allow_http` for MinIO.
pub fn from_url<'a>(
    url: &str,
    options: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> Result<ObjectStoreRef> {
    if url.starts_with("memory://") {
        return Ok(Arc::new(InMemory::new()));
    }
    let parsed =
        Url::parse(url).map_err(|e| ForgeError::invalid(format!("storage url {url}: {e}")))?;
    if parsed.scheme() == "file" {
        let path = parsed.to_file_path().map_err(|_| {
            ForgeError::invalid(format!("storage url {url} is not an absolute file path"))
        })?;
        std::fs::create_dir_all(&path).with_context(|| format!("creating {}", path.display()))?;
        let fs = LocalFileSystem::new_with_prefix(&path)
            .map_err(|e| ForgeError::internal(format!("local store {}: {e}", path.display())))?
            .with_automatic_cleanup(true);
        return Ok(Arc::new(fs));
    }
    let opts: Vec<(String, String)> = options
        .into_iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let (store, prefix) = object_store::parse_url_opts(&parsed, opts)
        .map_err(|e| ForgeError::invalid(format!("storage url {url}: {e}")))?;
    let store: ObjectStoreRef = Arc::from(store);
    if prefix.as_ref().is_empty() {
        Ok(store)
    } else {
        Ok(Arc::new(PrefixStore::new(store, prefix)))
    }
}
