//! Storage for Forge: object-store construction, Vortex segment files, oversized payload blobs
//! and the read cache.

pub mod blob;
pub mod paths;
pub mod segment;
pub mod store;

pub use segment::{SESSION, VortexSegmentStore};
pub use store::{ObjectStoreRef, from_url};
