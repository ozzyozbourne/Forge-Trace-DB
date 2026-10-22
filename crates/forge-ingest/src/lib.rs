//! Forge's write path: WAL, memtables, flush to immutable L0 segments, and the background
//! lifecycle tasks (compaction, retention, garbage collection).

pub mod ingester;
pub mod memtable;
pub mod wal;
pub mod writer;

pub use ingester::{Ingester, IngesterRef};
pub use memtable::MemSnapshot;
