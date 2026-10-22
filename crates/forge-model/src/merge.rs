//! Merge-on-read for upserts: rows sharing `(project_id, trace_id, span_id)` collapse into one,
//! and for every column the value from the highest `version` that has it non-null wins.
//! Tombstoned keys (newest non-null `deleted` is true) are dropped unless asked to keep them.
//!
//! Works on any batch that contains the key columns and `version`, in either the logical or
//! the storage representation, so the same code serves queries and compaction.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array};
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, filter_record_batch, lexsort_to_indices, take,
};
use arrow::datatypes::{Int64Type, SchemaRef};
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};

use crate::schema::{DELETED, KEY_COLUMNS, VERSION};

#[derive(Debug, Clone, Copy, Default)]
pub struct MergeOptions {
    /// Keep rows whose merged `deleted` flag is true (compaction of partial inputs needs this).
    pub keep_tombstones: bool,
}

/// Merge several batches with the same schema.
pub fn merge_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    opts: MergeOptions,
) -> Result<RecordBatch, ArrowError> {
    let batch = concat_batches(schema, batches)?;
    merge_versions(&batch, opts)
}

/// Merge all versions in one batch. Output is ordered by key.
pub fn merge_versions(batch: &RecordBatch, opts: MergeOptions) -> Result<RecordBatch, ArrowError> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let schema = batch.schema();
    let key_idx: Vec<usize> = KEY_COLUMNS
        .iter()
        .map(|c| schema.index_of(c))
        .collect::<Result<_, _>>()?;
    let version_idx = schema.index_of(VERSION)?;

    // Sort by key asc, version desc.
    let mut sort_cols: Vec<SortColumn> = key_idx
        .iter()
        .map(|&i| SortColumn {
            values: batch.column(i).clone(),
            options: Some(SortOptions::default()),
        })
        .collect();
    sort_cols.push(SortColumn {
        values: batch.column(version_idx).clone(),
        options: Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    });
    let order = lexsort_to_indices(&sort_cols, None)?;
    let order = order.values();

    // Group boundaries via row-encoded keys.
    let converter = RowConverter::new(
        key_idx
            .iter()
            .map(|&i| SortField::new(schema.field(i).data_type().clone()))
            .collect(),
    )?;
    let keys: Vec<ArrayRef> = key_idx.iter().map(|&i| batch.column(i).clone()).collect();
    let rows = converter.convert_columns(&keys)?;
    let mut group_starts: Vec<usize> = Vec::new();
    for (pos, &row) in order.iter().enumerate() {
        if pos == 0 || rows.row(row as usize) != rows.row(order[pos - 1] as usize) {
            group_starts.push(pos);
        }
    }
    let n_groups = group_starts.len();
    let group_end = |g: usize| {
        if g + 1 < n_groups {
            group_starts[g + 1]
        } else {
            order.len()
        }
    };
    let all_unique = n_groups == order.len();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let indices: UInt32Array = if all_unique || col.null_count() == 0 {
            group_starts.iter().map(|&s| order[s]).collect()
        } else {
            (0..n_groups)
                .map(|g| {
                    let (s, e) = (group_starts[g], group_end(g));
                    order[s..e]
                        .iter()
                        .copied()
                        .find(|&r| col.is_valid(r as usize))
                        .unwrap_or(order[s])
                })
                .collect()
        };
        columns.push(take(col.as_ref(), &indices, None)?);
    }
    let merged = RecordBatch::try_new(schema.clone(), columns)?;

    if opts.keep_tombstones {
        return Ok(merged);
    }
    match schema.index_of(DELETED) {
        Ok(i) => {
            let deleted = merged.column(i).as_boolean();
            if deleted.true_count() == 0 {
                return Ok(merged);
            }
            let keep: BooleanArray = deleted.iter().map(|d| Some(!d.unwrap_or(false))).collect();
            filter_record_batch(&merged, &keep)
        }
        Err(_) => Ok(merged),
    }
}

/// True when some key appears more than once, i.e. a merge would change anything.
pub fn has_duplicate_keys(batch: &RecordBatch) -> Result<bool, ArrowError> {
    let schema = batch.schema();
    let key_idx: Vec<usize> = KEY_COLUMNS
        .iter()
        .map(|c| schema.index_of(c))
        .collect::<Result<_, _>>()?;
    let converter = RowConverter::new(
        key_idx
            .iter()
            .map(|&i| SortField::new(schema.field(i).data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(
        &key_idx
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect::<Vec<_>>(),
    )?;
    let mut seen = std::collections::HashSet::with_capacity(rows.num_rows());
    Ok(rows.iter().any(|r| !seen.insert(r.as_ref().to_vec())))
}

/// Highest `version` in a batch.
pub fn max_version(batch: &RecordBatch) -> Option<i64> {
    let i = batch.schema().index_of(VERSION).ok()?;
    arrow::compute::max(batch.column(i).as_primitive::<Int64Type>())
}

/// Ensure a projection includes the columns the merge needs. Returns the extended projection
/// and whether it had to be extended.
pub fn with_merge_columns(projection: &[usize], schema: &SchemaRef) -> (Vec<usize>, bool) {
    let mut out = projection.to_vec();
    let mut extended = false;
    for c in KEY_COLUMNS.iter().chain([VERSION, DELETED].iter()) {
        let i = schema.index_of(c).expect("merge column in schema");
        if !out.contains(&i) {
            out.push(i);
            extended = true;
        }
    }
    (out, extended)
}

pub fn empty_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::new_empty(schema.clone())
}

/// Wrap single array into Arc for convenience in tests.
#[doc(hidden)]
pub fn arc(a: impl Array + 'static) -> ArrayRef {
    Arc::new(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{SpanRow, batch_to_rows, rows_to_batch};
    use proptest::prelude::*;

    fn row(span: u8, version: i64) -> SpanRow {
        let mut r = SpanRow::new("p", [1; 16], [span; 8]);
        r.version = version;
        r
    }

    #[test]
    fn latest_non_null_wins() {
        let mut start = row(1, 1);
        start.name = Some("agent".into());
        start.start_time = Some(10);
        start.inputs = Some("{\"q\":1}".into());
        let mut end = row(1, 2);
        end.end_time = Some(20);
        end.status = Some("ok".into());
        end.outputs = Some("{\"a\":2}".into());
        end.name = Some("agent-renamed".into());
        // Out of order delivery: end first in the batch.
        let batch = rows_to_batch(&[end, start, row(2, 5)]).unwrap();
        let merged = batch_to_rows(&merge_versions(&batch, MergeOptions::default()).unwrap());
        assert_eq!(merged.len(), 2);
        let m = &merged[0];
        assert_eq!(m.name.as_deref(), Some("agent-renamed"));
        assert_eq!(m.start_time, Some(10));
        assert_eq!(m.end_time, Some(20));
        assert_eq!(m.status.as_deref(), Some("ok"));
        assert_eq!(m.inputs.as_deref(), Some("{\"q\":1}"));
        assert_eq!(m.version, 2);
    }

    #[test]
    fn tombstones_drop() {
        let mut d = row(1, 3);
        d.deleted = true;
        let batch = rows_to_batch(&[row(1, 1), d, row(2, 1)]).unwrap();
        let merged = merge_versions(&batch, MergeOptions::default()).unwrap();
        assert_eq!(merged.num_rows(), 1);
        let kept = merge_versions(
            &batch,
            MergeOptions {
                keep_tombstones: true,
            },
        )
        .unwrap();
        assert_eq!(kept.num_rows(), 2);
        // A newer upsert resurrects.
        let batch = rows_to_batch(&[
            row(1, 1),
            {
                let mut d = row(1, 2);
                d.deleted = true;
                d
            },
            row(1, 3),
        ])
        .unwrap();
        assert_eq!(
            merge_versions(&batch, MergeOptions::default())
                .unwrap()
                .num_rows(),
            1
        );
    }

    proptest! {
        // Merging is idempotent and order-independent.
        #[test]
        fn merge_is_order_independent(
            entries in proptest::collection::vec((0u8..6, 0i64..1000, proptest::option::of(0i64..50), proptest::option::of("[a-c]{1,3}")), 1..40),
            seed in any::<u64>(),
        ) {
            // Unique versions per key, as ingest guarantees.
            let mut rows: Vec<SpanRow> = entries.iter().enumerate().map(|(i, (s, _, t, n))| {
                let mut r = row(*s, i as i64);
                r.start_time = *t;
                r.name = n.clone();
                r
            }).collect();
            let a = batch_to_rows(&merge_versions(&rows_to_batch(&rows).unwrap(), MergeOptions::default()).unwrap());
            // Deterministic shuffle.
            let mut x = seed;
            for i in (1..rows.len()).rev() {
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                rows.swap(i, (x % (i as u64 + 1)) as usize);
            }
            let b = batch_to_rows(&merge_versions(&rows_to_batch(&rows).unwrap(), MergeOptions::default()).unwrap());
            prop_assert_eq!(&a, &b);
            let again = batch_to_rows(&merge_versions(&rows_to_batch(&a).unwrap(), MergeOptions::default()).unwrap());
            prop_assert_eq!(&a, &again);
            // Reference model.
            for m in &a {
                let versions: Vec<&SpanRow> = rows.iter().filter(|r| r.span_id == m.span_id).collect();
                let best_t = versions.iter().filter(|r| r.start_time.is_some()).max_by_key(|r| r.version).and_then(|r| r.start_time);
                prop_assert_eq!(m.start_time, best_t);
                prop_assert_eq!(m.version, versions.iter().map(|r| r.version).max().unwrap());
            }
        }
    }
}
