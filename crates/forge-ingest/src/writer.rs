//! Turn merged rows into segment objects: split by `(project_id, hour)`, sort, write the
//! `.vortex` file and its `.fts` sidecar, and describe the result for the metastore.

use std::collections::BTreeMap;

use arrow::array::{Array, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{Int64Type, TimestampNanosecondType};
use forge_model::Result;
use forge_model::now_ms;
use forge_model::row::TraceId;
use forge_model::schema::{self, to_storage};
use forge_model::traits::{NewSegment, SegmentRecord, SegmentState, SegmentStore, hour_of};
use forge_storage::paths;

/// Partition hour of each row: its start time, or its ingest time (≈ version) when unknown.
fn row_hours(batch: &RecordBatch) -> Vec<i64> {
    let col = |n: &str| {
        batch
            .column(batch.schema().index_of(n).expect("column present"))
            .clone()
    };
    let start = col(schema::START_TIME);
    let start = start.as_primitive::<TimestampNanosecondType>();
    let version = col(schema::VERSION);
    let version = version.as_primitive::<Int64Type>();
    (0..batch.num_rows())
        .map(|i| {
            if start.is_valid(i) {
                hour_of(start.value(i))
            } else {
                hour_of(version.value(i))
            }
        })
        .collect()
}

/// Split a logical batch (any projection containing `project_id`, `start_time` and
/// `version`) into `(project, hour) -> rows`.
pub fn partition(batch: &RecordBatch) -> Result<BTreeMap<(String, i64), RecordBatch>> {
    let projects = batch
        .column(batch.schema().index_of(schema::PROJECT_ID)?)
        .clone();
    let projects = projects.as_string::<i32>();
    let hours = row_hours(batch);
    let mut groups: BTreeMap<(String, i64), Vec<u32>> = BTreeMap::new();
    for (i, h) in hours.iter().enumerate() {
        groups
            .entry((projects.value(i).to_string(), *h))
            .or_default()
            .push(i as u32);
    }
    let mut out = BTreeMap::new();
    if groups.len() == 1 {
        let (k, _) = groups.into_iter().next().unwrap();
        out.insert(k, batch.clone());
        return Ok(out);
    }
    for (k, idx) in groups {
        out.insert(k, take_record_batch(batch, &UInt32Array::from(idx))?);
    }
    Ok(out)
}

/// Physical order inside a segment: `(trace_id, dotted_order, start_time, span_id)`, so one
/// trace's rows are contiguous and in tree order.
pub fn sort_for_segment(batch: &RecordBatch) -> Result<RecordBatch> {
    let col = |n: &str| batch.column(batch.schema().index_of(n).unwrap()).clone();
    let opts = |nulls_first| {
        Some(SortOptions {
            descending: false,
            nulls_first,
        })
    };
    let cols = vec![
        SortColumn {
            values: col(schema::PROJECT_ID),
            options: opts(false),
        },
        SortColumn {
            values: col(schema::TRACE_ID),
            options: opts(false),
        },
        SortColumn {
            values: col(schema::DOTTED_ORDER),
            options: opts(false),
        },
        SortColumn {
            values: col(schema::START_TIME),
            options: opts(false),
        },
        SortColumn {
            values: col(schema::SPAN_ID),
            options: opts(false),
        },
    ];
    let idx = lexsort_to_indices(&cols, None)?;
    Ok(take_record_batch(batch, &idx)?)
}

/// Write one partition's rows (logical schema, already merged) as a segment.
pub async fn write_partition(
    store: &dyn SegmentStore,
    project_id: &str,
    level: u8,
    partition_hour: i64,
    batch: &RecordBatch,
    build_index: bool,
) -> Result<NewSegment> {
    let t = std::time::Instant::now();
    let sorted = sort_for_segment(batch)?;
    let stored = to_storage(&sorted)?;
    let sort_time = t.elapsed();
    let (id, path) = paths::new_segment_path(project_id, level, partition_hour);
    // Encode the segment and build its index concurrently.
    let index_input = stored.clone();
    let index = build_index.then(|| {
        tokio::task::spawn_blocking(move || forge_index::build(std::slice::from_ref(&index_input)))
    });
    let t = std::time::Instant::now();
    let written = store.write_segment(&path, &stored).await?;
    let write_time = t.elapsed();
    let t = std::time::Instant::now();
    let fts_path = match index {
        Some(task) => {
            let fts = task
                .await
                .map_err(|e| forge_model::ForgeError::internal(e.to_string()))??;
            let p = paths::fts_path(&path);
            store.put_object(&p, fts).await?;
            Some(p)
        }
        None => None,
    };
    tracing::debug!(
        rows = batch.num_rows(),
        bytes = written.byte_size,
        sort_ms = sort_time.as_millis() as u64,
        write_ms = write_time.as_millis() as u64,
        index_wait_ms = t.elapsed().as_millis() as u64,
        "wrote segment"
    );

    let start = sorted
        .column(schema::col_idx(schema::START_TIME))
        .as_primitive::<TimestampNanosecondType>();
    let version = sorted
        .column(schema::col_idx(schema::VERSION))
        .as_primitive::<Int64Type>();
    let traces = sorted
        .column(schema::col_idx(schema::TRACE_ID))
        .as_fixed_size_binary();
    let mut trace_ids: Vec<TraceId> = Vec::new();
    for i in 0..traces.len() {
        let t: TraceId = traces.value(i).try_into().unwrap();
        if trace_ids.last() != Some(&t) {
            trace_ids.push(t);
        }
    }
    trace_ids.dedup();
    let record = SegmentRecord {
        id,
        project_id: project_id.to_string(),
        level,
        partition_hour,
        path,
        fts_path,
        row_count: written.row_count,
        byte_size: written.byte_size,
        min_start_time: arrow::compute::min(start),
        max_start_time: arrow::compute::max(start),
        min_trace_id: trace_ids.first().map(|t| t.to_vec()).unwrap_or_default(),
        max_trace_id: trace_ids.last().map(|t| t.to_vec()).unwrap_or_default(),
        max_version: arrow::compute::max(version).unwrap_or(0),
        created_at_ms: now_ms(),
        state: SegmentState::Live,
        deleted_at_ms: None,
    };
    Ok(NewSegment { record, trace_ids })
}
