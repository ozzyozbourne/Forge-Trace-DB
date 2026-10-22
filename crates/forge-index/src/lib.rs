//! Forge's inverted index: a per-segment `.fts` sidecar of an FST term dictionary and roaring
//! posting lists, built at flush and compaction, plus the tokenizer and query parser shared with
//! the brute-force memtable search.

pub mod fts;
pub mod terms;
pub mod tokenize;

pub use fts::{FtsBuilder, FtsReader, build};
pub use terms::{matches, parse_query, path_query};
