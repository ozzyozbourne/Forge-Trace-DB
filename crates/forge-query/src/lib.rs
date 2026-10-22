//! Forge's query engine: an embedded DataFusion session over a `spans` table that unions
//! memtables with Vortex segments and merges row versions on read, Forge's SQL functions,
//! and hand-built fast paths for trace trees, run lists and full-text search.

pub mod engine;
pub mod fastpath;
pub mod provider;
pub mod pushdown;
pub mod reader;
pub mod search;
pub mod tree;
pub mod udf;

pub use engine::{QueryEngine, SqlResult, to_json_rows, to_pretty};
pub use reader::{MemtableSource, NoMemtables, Reader};
