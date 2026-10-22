//! Full-text search fast path.
//!
//! Per partition hour, newest first:
//! 1. each segment's `.fts` sidecar turns the query into candidate row ids (segments whose
//!    dictionary lacks a term are skipped without reading data);
//! 2. only those rows are read, and their keys collected (plus brute-force matches from the
//!    memtables);
//! 3. candidates are taken newest first in small chunks: every version of a chunk's keys is
//!    read and merged, and the merged rows re-checked, so a match on a superseded version never
//!    leaks through;
//! 4. stop once `limit` hits are collected.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::array::RecordBatch;
use forge_index::terms::matches;
use forge_model::Result;
use forge_model::row::{SpanId, SpanRow, TraceId, batch_to_rows};
use forge_model::schema::{INDEXED_COLUMNS, KEY_COLUMNS, START_TIME, VERSION, non_payload_columns};
use forge_model::traits::{HOUR_NS, ScanPredicate, SearchExpr, SegmentFilter, SegmentRecord};

use crate::fastpath::{Cursor, RunFilter, RunPage};
use crate::reader::{ReadPlan, Reader, merge_and_project, with_merge_columns};

/// Largest key list verified in one read.
const MAX_KEY_PREDICATE: usize = 512;

fn text_of(r: &SpanRow) -> impl Fn(&str) -> Option<String> + '_ {
    move |c| r.indexed_text(c).map(str::to_string)
}

fn candidate_columns() -> Vec<String> {
    let mut c: Vec<String> = KEY_COLUMNS.iter().map(|s| s.to_string()).collect();
    c.push(START_TIME.into());
    c.push(VERSION.into());
    c.extend(INDEXED_COLUMNS.iter().map(|s| s.to_string()));
    c
}

/// Columns returned for hits: everything except payloads, plus the indexed text so the merged
/// row can be re-verified.
fn result_columns() -> Vec<String> {
    let mut c: Vec<String> = non_payload_columns()
        .into_iter()
        .map(String::from)
        .collect();
    for col in INDEXED_COLUMNS {
        if !c.iter().any(|x| x == col) {
            c.push(col.to_string());
        }
    }
    c
}

pub async fn search(
    reader: &Reader,
    project: &str,
    expr: &SearchExpr,
    filter: &RunFilter,
    roots_only: bool,
    limit: usize,
    cursor: Option<&str>,
) -> Result<RunPage> {
    let limit = limit.clamp(1, 1000);
    let cursor = cursor.map(Cursor::decode).transpose()?;
    let lower = filter.start;
    let upper = match (filter.end, cursor) {
        (Some(e), Some(c)) => Some(e.min(c.start.saturating_add(1))),
        (Some(e), None) => Some(e),
        (None, Some(c)) => Some(c.start.saturating_add(1)),
        (None, None) => None,
    };
    let base = ScanPredicate {
        project_id: Some(project.to_string()),
        start_min: lower,
        start_max: upper,
        roots_only,
        ..Default::default()
    };
    let out_cols = result_columns();
    let scan_cols = with_merge_columns(&out_cols);

    // Memtable rows (all columns we need), by hour.
    let snapshot = reader.mem.snapshot();
    let mut mem_by_hour: BTreeMap<i64, Vec<RecordBatch>> = BTreeMap::new();
    for b in snapshot.scan(&base)? {
        let b = crate::reader::project(&b, &scan_cols)?;
        for ((_, hour), part) in forge_ingest::writer::partition(&b)? {
            mem_by_hour.entry(hour).or_default().push(part);
        }
    }
    let segments = reader
        .metastore
        .list_segments(&SegmentFilter {
            project_id: Some(project.to_string()),
            start_min: lower,
            start_max: upper,
            ..Default::default()
        })
        .await?;
    let mut seg_by_hour: HashMap<i64, Vec<SegmentRecord>> = HashMap::new();
    for s in segments {
        seg_by_hour.entry(s.partition_hour).or_default().push(s);
    }
    let mut hours: Vec<i64> = seg_by_hour
        .keys()
        .chain(mem_by_hour.keys())
        .copied()
        .collect();
    hours.sort_unstable_by(|a, b| b.cmp(a));
    hours.dedup();

    let mut out: Vec<SpanRow> = Vec::new();
    let mut more = false;
    for hour in hours.iter() {
        let mut pred = base.clone();
        pred.start_min = Some(lower.map_or(*hour, |l| l.max(*hour)));
        pred.start_max = Some(upper.map_or(hour + HOUR_NS, |u| u.min(hour + HOUR_NS)));
        let segs = seg_by_hour.remove(hour).unwrap_or_default();
        let mem = mem_by_hour.remove(hour).unwrap_or_default();

        // 1-2: candidate keys with their start times, from the index (segments) and brute
        // force (memtables). Superseded versions may produce false candidates; step 3 drops them.
        let mut cands: HashMap<(TraceId, SpanId), i64> = HashMap::new();
        for b in &mem {
            for r in batch_to_rows(b) {
                if let Some(st) = r.start_time
                    && matches(expr, &text_of(&r))
                {
                    cands.insert((r.trace_id, r.span_id), st);
                }
            }
        }
        let key_cols: Vec<String> = KEY_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .chain([START_TIME.to_string()])
            .collect();
        let mut indexed = ReadPlan {
            mem: vec![],
            segments: vec![],
            predicate: pred.clone(),
            scan_columns: with_merge_columns(&key_cols),
            output_columns: key_cols,
            selections: HashMap::new(),
        };
        let cand_cols = candidate_columns();
        let mut unindexed = ReadPlan {
            mem: vec![],
            segments: vec![],
            predicate: pred.clone(),
            scan_columns: with_merge_columns(&cand_cols),
            output_columns: cand_cols,
            selections: HashMap::new(),
        };
        for seg in &segs {
            let selection = match &reader.index {
                Some(idx) => idx.lookup(seg, expr).await?,
                None => None,
            };
            match selection {
                Some(rows) if rows.is_empty() => {}
                Some(rows) => {
                    indexed.selections.insert(seg.id.clone(), rows);
                    indexed.segments.push(seg.clone());
                }
                None => unindexed.segments.push(seg.clone()),
            }
        }
        if !indexed.segments.is_empty() {
            for b in reader.read_inputs(&indexed).await? {
                for r in batch_to_rows(&b) {
                    if let Some(st) = r.start_time {
                        cands.insert((r.trace_id, r.span_id), st);
                    }
                }
            }
        }
        if !unindexed.segments.is_empty() {
            for b in reader.read_inputs(&unindexed).await? {
                for r in batch_to_rows(&b) {
                    if let Some(st) = r.start_time
                        && matches(expr, &text_of(&r))
                    {
                        cands.insert((r.trace_id, r.span_id), st);
                    }
                }
            }
        }
        let mut ordered: Vec<Cursor> = cands
            .into_iter()
            .map(|((trace, span), start)| Cursor { start, trace, span })
            .filter(|c| cursor.is_none_or(|cur| *c < cur))
            .collect();
        ordered.sort_unstable_by(|a, b| b.cmp(a));

        // 3: newest candidates first, in chunks: read every version of the chunk's keys, merge,
        // re-verify, until enough hits.
        let chunk_size = (limit * 2).clamp(32, MAX_KEY_PREDICATE);
        let mut hour_hits: Vec<SpanRow> = Vec::new();
        for chunk in ordered.chunks(chunk_size) {
            let keys: HashSet<(TraceId, SpanId)> =
                chunk.iter().map(|c| (c.trace, c.span)).collect();
            let mut full_pred = pred.clone();
            full_pred.trace_ids = Some(
                chunk
                    .iter()
                    .map(|c| c.trace)
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect(),
            );
            full_pred.span_ids = Some(
                chunk
                    .iter()
                    .map(|c| c.span)
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect(),
            );
            let traces = full_pred.trace_ids.clone().unwrap();
            let full_plan = ReadPlan {
                mem: mem
                    .iter()
                    .map(|b| crate::reader::project(b, &scan_cols))
                    .collect::<Result<Vec<_>>>()?,
                segments: segs
                    .iter()
                    .filter(|s| {
                        traces.iter().any(|t| {
                            s.min_trace_id.as_slice() <= t.as_slice()
                                && t.as_slice() <= s.max_trace_id.as_slice()
                        })
                    })
                    .cloned()
                    .collect(),
                predicate: full_pred,
                scan_columns: scan_cols.clone(),
                output_columns: out_cols.clone(),
                selections: HashMap::new(),
            };
            let inputs = reader.read_inputs(&full_plan).await?;
            let merged = merge_and_project(&full_plan, inputs)?;
            hour_hits.extend(
                batch_to_rows(&merged)
                    .into_iter()
                    .filter(|r| keys.contains(&(r.trace_id, r.span_id)))
                    .filter(|r| r.start_time.is_some())
                    .filter(|r| !roots_only || r.parent_span_id.is_none())
                    .filter(|r| matches(expr, &text_of(r)) && filter.matches(r)),
            );
            if out.len() + hour_hits.len() >= limit {
                break;
            }
        }
        hour_hits.sort_by(|a, b| Cursor::of(b).cmp(&Cursor::of(a)));
        out.extend(hour_hits);
        if out.len() >= limit {
            more = true;
            break;
        }
    }
    // A full final page may or may not have more behind it; a cursor that turns out to be
    // the end yields an empty page.
    let next_cursor = if more {
        out.truncate(limit);
        out.last().and_then(Cursor::of).map(|c| c.encode())
    } else {
        None
    };
    Ok(RunPage {
        runs: out,
        next_cursor,
    })
}
