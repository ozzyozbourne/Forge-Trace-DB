//! The one logical table, `spans`, and its physical (segment) variant.
//!
//! The logical schema is what DataFusion and the API see. Trace and span ids are
//! `FixedSizeBinary(16)` / `FixedSizeBinary(8)` as in OTel. Vortex has no fixed-size
//! binary type, so segments store ids as variable `Binary`; [`to_storage`] and
//! [`from_storage`] convert between the two.

use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;

pub const PROJECT_ID: &str = "project_id";
pub const TRACE_ID: &str = "trace_id";
pub const SPAN_ID: &str = "span_id";
pub const PARENT_SPAN_ID: &str = "parent_span_id";
pub const DOTTED_ORDER: &str = "dotted_order";
pub const NAME: &str = "name";
pub const RUN_TYPE: &str = "run_type";
pub const START_TIME: &str = "start_time";
pub const END_TIME: &str = "end_time";
pub const STATUS: &str = "status";
pub const ERROR: &str = "error";
pub const MODEL: &str = "model";
pub const PROVIDER: &str = "provider";
pub const PROMPT_TOKENS: &str = "prompt_tokens";
pub const COMPLETION_TOKENS: &str = "completion_tokens";
pub const TOTAL_TOKENS: &str = "total_tokens";
pub const COST_USD: &str = "cost_usd";
pub const SESSION_ID: &str = "session_id";
pub const TAGS: &str = "tags";
pub const METADATA: &str = "metadata";
pub const ATTRIBUTES: &str = "attributes";
pub const EVENTS: &str = "events";
pub const INPUTS: &str = "inputs";
pub const OUTPUTS: &str = "outputs";
pub const VERSION: &str = "version";
pub const DELETED: &str = "deleted";

/// Columns holding large JSON payloads. List and filter plans never read these.
pub const PAYLOAD_COLUMNS: [&str; 4] = [INPUTS, OUTPUTS, EVENTS, ATTRIBUTES];

/// Columns that identify a row version: the merge key plus `version`.
pub const KEY_COLUMNS: [&str; 3] = [PROJECT_ID, TRACE_ID, SPAN_ID];

/// Columns whose value never changes across versions of one span once ingest has filled
/// them in. Filters on these may be pushed below the version merge.
pub const STABLE_COLUMNS: [&str; 5] = [PROJECT_ID, TRACE_ID, SPAN_ID, PARENT_SPAN_ID, START_TIME];

/// Columns the full-text index covers.
pub const INDEXED_COLUMNS: [&str; 5] = [NAME, ERROR, INPUTS, OUTPUTS, METADATA];

/// Timezone of every timestamp column. An offset rather than "UTC" so arrow needs no tz database.
pub const TZ: &str = "+00:00";

pub const TRACE_ID_LEN: i32 = 16;
pub const SPAN_ID_LEN: i32 = 8;

pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some(TZ.into()))
}

fn tags_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
}

fn build(trace: DataType, span: DataType) -> Schema {
    Schema::new(vec![
        Field::new(PROJECT_ID, DataType::Utf8, false),
        Field::new(TRACE_ID, trace, false),
        Field::new(SPAN_ID, span.clone(), false),
        Field::new(PARENT_SPAN_ID, span, true),
        Field::new(DOTTED_ORDER, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(RUN_TYPE, DataType::Utf8, true),
        Field::new(START_TIME, timestamp_type(), true),
        Field::new(END_TIME, timestamp_type(), true),
        Field::new(STATUS, DataType::Utf8, true),
        Field::new(ERROR, DataType::Utf8, true),
        Field::new(MODEL, DataType::Utf8, true),
        Field::new(PROVIDER, DataType::Utf8, true),
        Field::new(PROMPT_TOKENS, DataType::Int64, true),
        Field::new(COMPLETION_TOKENS, DataType::Int64, true),
        Field::new(TOTAL_TOKENS, DataType::Int64, true),
        Field::new(COST_USD, DataType::Float64, true),
        Field::new(SESSION_ID, DataType::Utf8, true),
        Field::new(TAGS, tags_type(), true),
        Field::new(METADATA, DataType::Utf8, true),
        Field::new(ATTRIBUTES, DataType::Utf8, true),
        Field::new(EVENTS, DataType::Utf8, true),
        Field::new(INPUTS, DataType::Utf8, true),
        Field::new(OUTPUTS, DataType::Utf8, true),
        Field::new(VERSION, DataType::Int64, false),
        Field::new(DELETED, DataType::Boolean, false),
    ])
}

static SPANS: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(build(
        DataType::FixedSizeBinary(TRACE_ID_LEN),
        DataType::FixedSizeBinary(SPAN_ID_LEN),
    ))
});

static STORAGE: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(build(DataType::Binary, DataType::Binary)));

/// The logical `spans` schema.
pub fn spans_schema() -> SchemaRef {
    SPANS.clone()
}

/// The schema rows have inside a segment file.
pub fn storage_schema() -> SchemaRef {
    STORAGE.clone()
}

/// Index of a column in both schemas.
pub fn col_idx(name: &str) -> usize {
    SPANS
        .index_of(name)
        .unwrap_or_else(|_| panic!("unknown spans column {name}"))
}

/// Project a schema to a subset of columns, by name, preserving the given order.
pub fn project_schema(schema: &SchemaRef, columns: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| schema.field_with_name(c).expect("known column").clone())
            .collect::<Vec<_>>(),
    ))
}

/// Every column except the payload columns, in schema order.
pub fn non_payload_columns() -> Vec<&'static str> {
    SPANS
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|c| !PAYLOAD_COLUMNS.contains(c))
        .map(|c| -> &'static str {
            // Safe: names come from the static schema.
            SPANS.field_with_name(c).unwrap().name().as_str()
        })
        .collect()
}

fn convert(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, col) in schema.fields().iter().zip(batch.columns()) {
        let want = target
            .field_with_name(field.name())
            .map(|f| f.data_type().clone());
        match want {
            Ok(dt) if &dt != col.data_type() => {
                cols.push(cast(col, &dt)?);
                fields.push(field.as_ref().clone().with_data_type(dt));
            }
            _ => {
                cols.push(col.clone());
                fields.push(field.as_ref().clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
}

/// Convert a logical batch (any column subset) to the storage representation.
pub fn to_storage(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    convert(batch, &STORAGE)
}

/// Convert a storage batch (any column subset) back to the logical representation.
pub fn from_storage(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    convert(batch, &SPANS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_line_up() {
        assert_eq!(
            spans_schema().fields().len(),
            storage_schema().fields().len()
        );
        assert_eq!(col_idx(PROJECT_ID), 0);
        assert!(!non_payload_columns().contains(&INPUTS));
        assert!(non_payload_columns().contains(&START_TIME));
    }
}
