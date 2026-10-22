//! Shared model for Forge: the `spans` schema, row conversion, OTLP + GenAI normalization,
//! `dotted_order`, version merging, configuration and the traits between crates.

pub mod config;
pub mod dotted;
pub mod error;
pub mod genai;
pub mod merge;
pub mod otlp;
pub mod pricing;
pub mod row;
pub mod schema;
pub mod traits;

pub use error::{ForgeError, Result};
pub use opentelemetry_proto;
pub use row::{SpanId, SpanRow, TraceId};

/// Current wall-clock time in nanoseconds since the Unix epoch.
pub fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub fn now_ms() -> i64 {
    now_ns() / 1_000_000
}
