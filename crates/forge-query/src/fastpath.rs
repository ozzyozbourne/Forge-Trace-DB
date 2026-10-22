//! Hand-built plans for the workloads that dominate: loading a trace tree and listing runs.

use std::collections::{BTreeMap, HashMap};

use arrow::array::RecordBatch;
use base64::Engine as _;
use forge_model::row::{SpanId, SpanRow, TraceId, batch_to_rows, span_id_hex, trace_id_hex};
use forge_model::schema::non_payload_columns;
use forge_model::traits::{HOUR_NS, ScanPredicate, SegmentFilter, SegmentRecord, hour_of};
use forge_model::{ForgeError, Result};
use forge_storage::blob;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::reader::{ReadPlan, Reader, merge_and_project, with_merge_columns};
use crate::tree::{nest, span_json};

/// Replace blob references in payload columns with the payloads.
pub async fn resolve_blobs(reader: &Reader, rows: &mut [SpanRow]) -> Result<()> {
    for r in rows.iter_mut() {
        let project = r.project_id.clone();
        for field in [
            &mut r.inputs,
            &mut r.outputs,
            &mut r.events,
            &mut r.attributes,
        ] {
            if let Some(v) = field.take() {
                *field = Some(blob::resolve(reader.segments.as_ref(), &project, v).await?);
            }
        }
    }
    Ok(())
}

/// Load a trace as nested JSON. `None` when no span of it exists.
pub async fn trace_tree(
    reader: &Reader,
    project: &str,
    trace: &TraceId,
    include_payloads: bool,
) -> Result<Option<Value>> {
    let columns = if include_payloads {
        None
    } else {
        Some(
            non_payload_columns()
                .into_iter()
                .map(String::from)
                .collect(),
        )
    };
    let batch = reader.trace_rows(project, trace, columns).await?;
    let mut rows = batch_to_rows(&batch);
    if rows.is_empty() {
        return Ok(None);
    }
    if include_payloads {
        resolve_blobs(reader, &mut rows).await?;
    }
    let roots = nest(&rows, include_payloads);
    Ok(Some(json!({
        "project_id": project,
        "trace_id": trace_id_hex(trace),
        "span_count": rows.len(),
        "spans": roots,
    })))
}

/// One span with payloads.
pub async fn get_span(
    reader: &Reader,
    project: &str,
    trace: &TraceId,
    span: &SpanId,
) -> Result<Option<Value>> {
    let batch = reader.trace_rows(project, trace, None).await?;
    let mut rows: Vec<SpanRow> = batch_to_rows(&batch)
        .into_iter()
        .filter(|r| &r.span_id == span)
        .collect();
    resolve_blobs(reader, &mut rows).await?;
    Ok(rows.first().map(|r| Value::Object(span_json(r, true))))
}

/// Position in a newest-first listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor {
    pub start: i64,
    pub trace: TraceId,
    pub span: SpanId,
}

impl Cursor {
    pub fn of(r: &SpanRow) -> Option<Cursor> {
        Some(Cursor {
            start: r.start_time?,
            trace: r.trace_id,
            span: r.span_id,
        })
    }

    pub fn encode(&self) -> String {
        let raw = format!(
            "{}:{}:{}",
            self.start,
            trace_id_hex(&self.trace),
            span_id_hex(&self.span)
        );
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
    }

    pub fn decode(s: &str) -> Result<Cursor> {
        let bad = || ForgeError::invalid("invalid cursor");
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| bad())?;
        let raw = String::from_utf8(raw).map_err(|_| bad())?;
        let mut it = raw.split(':');
        let start = it.next().and_then(|x| x.parse().ok()).ok_or_else(bad)?;
        let trace = it
            .next()
            .and_then(forge_model::row::parse_trace_id)
            .ok_or_else(bad)?;
        let span = it
            .next()
            .and_then(forge_model::row::parse_span_id)
            .ok_or_else(bad)?;
        Ok(Cursor { start, trace, span })
    }
}

/// Filters for listing runs. Ranges are inclusive start, exclusive end.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RunFilter {
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub run_type: Option<String>,
    pub status: Option<String>,
    pub name: Option<String>,
    pub session_id: Option<String>,
    pub model: Option<String>,
    pub tag: Option<String>,
    pub min_latency_ms: Option<f64>,
    pub max_latency_ms: Option<f64>,
    pub min_tokens: Option<i64>,
    pub max_tokens: Option<i64>,
    pub min_cost: Option<f64>,
    pub max_cost: Option<f64>,
    /// `key=value` pairs that must all match top-level metadata keys.
    pub metadata: BTreeMap<String, String>,
    pub trace_id: Option<String>,
    /// List every span, not just roots.
    pub all_spans: bool,
}

impl RunFilter {
    pub fn matches(&self, r: &SpanRow) -> bool {
        let eq = |want: &Option<String>, got: &Option<String>| match want {
            None => true,
            Some(w) => got.as_deref() == Some(w.as_str()),
        };
        if !eq(&self.run_type, &r.run_type)
            || !eq(&self.name, &r.name)
            || !eq(&self.session_id, &r.session_id)
            || !eq(&self.model, &r.model)
        {
            return false;
        }
        if let Some(s) = &self.status
            && r.status.as_deref().unwrap_or("unset") != s
        {
            return false;
        }
        if let Some(t) = &self.tag
            && !r
                .tags
                .as_ref()
                .is_some_and(|tags| tags.iter().any(|x| x == t))
        {
            return false;
        }
        if self.min_latency_ms.is_some() || self.max_latency_ms.is_some() {
            let Some(lat) = r
                .start_time
                .zip(r.end_time)
                .map(|(s, e)| (e - s) as f64 / 1e6)
            else {
                return false;
            };
            if self.min_latency_ms.is_some_and(|m| lat < m)
                || self.max_latency_ms.is_some_and(|m| lat > m)
            {
                return false;
            }
        }
        if self.min_tokens.is_some() || self.max_tokens.is_some() {
            let t = r.total_tokens.unwrap_or(0);
            if self.min_tokens.is_some_and(|m| t < m) || self.max_tokens.is_some_and(|m| t > m) {
                return false;
            }
        }
        if self.min_cost.is_some() || self.max_cost.is_some() {
            let c = r.cost_usd.unwrap_or(0.0);
            if self.min_cost.is_some_and(|m| c < m) || self.max_cost.is_some_and(|m| c > m) {
                return false;
            }
        }
        if !self.metadata.is_empty() {
            let Some(meta) = r
                .metadata
                .as_deref()
                .and_then(|m| serde_json::from_str::<Value>(m).ok())
            else {
                return false;
            };
            for (k, want) in &self.metadata {
                let ok = match meta.get(k) {
                    Some(Value::String(s)) => s == want,
                    Some(v) => &v.to_string() == want,
                    None => false,
                };
                if !ok {
                    return false;
                }
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct RunPage {
    pub runs: Vec<SpanRow>,
    pub next_cursor: Option<String>,
}

/// Rows in newest-first order for a partition-at-a-time scan.
fn sort_desc(rows: &mut [SpanRow]) {
    rows.sort_by(|a, b| Cursor::of(b).cmp(&Cursor::of(a)));
}

/// List runs newest first. Reads one partition hour at a time, newest first, and stops once
/// `limit` rows are found. Payload columns are never read.
pub async fn list_runs(
    reader: &Reader,
    project: &str,
    filter: &RunFilter,
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
    let trace_ids = match &filter.trace_id {
        Some(t) => Some(vec![
            forge_model::row::parse_trace_id(t)
                .ok_or_else(|| ForgeError::invalid("invalid trace_id"))?,
        ]),
        None => None,
    };
    let base = ScanPredicate {
        project_id: Some(project.to_string()),
        trace_ids: trace_ids.clone(),
        start_min: lower,
        start_max: upper,
        roots_only: !filter.all_spans,
        ..Default::default()
    };
    let output: Vec<String> = non_payload_columns()
        .into_iter()
        .map(String::from)
        .collect();
    let scan_columns = with_merge_columns(&output);

    // Memtables first (see reader docs), grouped by hour.
    let snapshot = reader.mem.snapshot();
    let mut mem_by_hour: BTreeMap<i64, Vec<RecordBatch>> = BTreeMap::new();
    for b in snapshot.scan(&base)? {
        let b = crate::reader::project(&b, &scan_columns)?;
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
            trace_id: trace_ids.as_ref().map(|t| t[0]),
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
    let mut exhausted = true;
    for (n, hour) in hours.iter().enumerate() {
        let mut pred = base.clone();
        pred.start_min = Some(lower.map_or(*hour, |l| l.max(*hour)));
        pred.start_max = Some(upper.map_or(hour + HOUR_NS, |u| u.min(hour + HOUR_NS)));
        let plan = ReadPlan {
            mem: mem_by_hour.remove(hour).unwrap_or_default(),
            segments: seg_by_hour.remove(hour).unwrap_or_default(),
            predicate: pred,
            scan_columns: scan_columns.clone(),
            output_columns: output.clone(),
            selections: HashMap::new(),
        };
        let inputs = reader.read_inputs(&plan).await?;
        let merged = merge_and_project(&plan, inputs)?;
        let mut rows: Vec<SpanRow> = batch_to_rows(&merged)
            .into_iter()
            .filter(|r| r.start_time.is_some() && filter.matches(r))
            .filter(|r| cursor.is_none_or(|c| Cursor::of(r).is_some_and(|rc| rc < c)))
            .collect();
        sort_desc(&mut rows);
        out.extend(rows);
        if out.len() >= limit {
            exhausted = n + 1 == hours.len() && out.len() == limit;
            break;
        }
    }
    let next_cursor = if out.len() > limit || (!exhausted && out.len() >= limit) {
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

/// Partition hour of a row, used when grouping memtable rows.
pub fn row_hour(r: &SpanRow) -> i64 {
    hour_of(r.start_time.unwrap_or(r.version))
}
