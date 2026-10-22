//! Payloads over the blob threshold live in content-addressed objects; the column keeps a small
//! JSON reference `{"$forge_blob": "<sha256>", "size": N}`.

use forge_model::Result;
use forge_model::traits::SegmentStore;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::paths::blob_path;

pub const BLOB_KEY: &str = "$forge_blob";

/// If `value` is larger than `threshold`, upload it and return the reference JSON instead.
pub async fn maybe_offload(
    store: &dyn SegmentStore,
    project_id: &str,
    value: String,
    threshold: usize,
) -> Result<String> {
    if value.len() <= threshold {
        return Ok(value);
    }
    let sha = hex::encode(Sha256::digest(value.as_bytes()));
    let size = value.len();
    store
        .put_object(&blob_path(project_id, &sha), bytes::Bytes::from(value))
        .await?;
    Ok(json!({ BLOB_KEY: sha, "size": size }).to_string())
}

/// The blob hash if `value` is a blob reference.
pub fn blob_ref(value: &str) -> Option<String> {
    if !value.starts_with("{\"$forge_blob\"") {
        return None;
    }
    let v: Value = serde_json::from_str(value).ok()?;
    v.get(BLOB_KEY)?.as_str().map(str::to_string)
}

/// Resolve a possibly-offloaded value back to the payload.
pub async fn resolve(store: &dyn SegmentStore, project_id: &str, value: String) -> Result<String> {
    match blob_ref(&value) {
        None => Ok(value),
        Some(sha) => {
            let b = store.get_object(&blob_path(project_id, &sha)).await?;
            Ok(String::from_utf8_lossy(&b).into_owned())
        }
    }
}
