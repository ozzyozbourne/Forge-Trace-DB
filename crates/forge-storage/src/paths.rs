//! Object layout:
//!
//! ```text
//! segments/<project_id>/<level>/<yyyy-mm-dd>/<hh>/<segment_ulid>.vortex
//! segments/<project_id>/<level>/<yyyy-mm-dd>/<hh>/<segment_ulid>.fts
//! blobs/<project_id>/<sha256>
//! wal/<node_id>/<seq>.wal
//! ```

use chrono::{DateTime, Utc};
use forge_model::{ForgeError, Result};

pub const SEGMENTS_PREFIX: &str = "segments";
pub const BLOBS_PREFIX: &str = "blobs";
pub const WAL_PREFIX: &str = "wal";

/// Project ids become path components, so they are restricted.
pub fn validate_project_id(id: &str) -> Result<()> {
    let ok = !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && !id.starts_with('.');
    if ok {
        Ok(())
    } else {
        Err(ForgeError::invalid(format!(
            "project id {id:?} must be 1-128 chars of [A-Za-z0-9._-] and not start with '.'"
        )))
    }
}

fn hour_dir(partition_hour_ns: i64) -> String {
    let dt: DateTime<Utc> = DateTime::from_timestamp_nanos(partition_hour_ns);
    dt.format("%Y-%m-%d/%H").to_string()
}

/// Path of a new segment's `.vortex` object. Returns `(id, path)`.
pub fn new_segment_path(project_id: &str, level: u8, partition_hour_ns: i64) -> (String, String) {
    let id = ulid::Ulid::new().to_string();
    let path = format!(
        "{SEGMENTS_PREFIX}/{project_id}/{level}/{}/{id}.vortex",
        hour_dir(partition_hour_ns)
    );
    (id, path)
}

/// The `.fts` sidecar next to a segment.
pub fn fts_path(segment_path: &str) -> String {
    match segment_path.strip_suffix(".vortex") {
        Some(base) => format!("{base}.fts"),
        None => format!("{segment_path}.fts"),
    }
}

pub fn blob_path(project_id: &str, sha256_hex: &str) -> String {
    format!("{BLOBS_PREFIX}/{project_id}/{sha256_hex}")
}

pub fn wal_path(node_id: &str, seq: u64) -> String {
    format!("{WAL_PREFIX}/{node_id}/{seq:020}.wal")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        let (id, p) = new_segment_path(
            "proj",
            0,
            1_700_000_000_000_000_000 / 3_600_000_000_000 * 3_600_000_000_000,
        );
        assert_eq!(p, format!("segments/proj/0/2023-11-14/22/{id}.vortex"));
        assert_eq!(
            fts_path(&p),
            format!("segments/proj/0/2023-11-14/22/{id}.fts")
        );
        assert!(validate_project_id("my-proj_1.x").is_ok());
        assert!(validate_project_id("../etc").is_err());
        assert!(validate_project_id("").is_err());
        assert_eq!(wal_path("n", 7), "wal/n/00000000000000000007.wal");
    }
}
