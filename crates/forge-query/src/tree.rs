//! Trace trees: order a trace's spans depth-first and nest them for the API.

use std::collections::HashMap;

use forge_model::row::{SpanId, SpanRow, span_id_hex, trace_id_hex};
use serde_json::{Map, Value, json};

fn sort_key(r: &SpanRow) -> (Option<i64>, &Option<String>, SpanId) {
    (r.start_time, &r.dotted_order, r.span_id)
}

/// Depth-first pre-order with siblings by start time. Spans whose parent is missing (not yet
/// arrived, or outside retention) are treated as roots. Returns `(row index, depth)`.
pub fn tree_order(rows: &[SpanRow]) -> Vec<(usize, usize)> {
    let by_id: HashMap<SpanId, usize> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| (r.span_id, i))
        .collect();
    let mut children: HashMap<Option<SpanId>, Vec<usize>> = HashMap::new();
    for (i, r) in rows.iter().enumerate() {
        let parent = r
            .parent_span_id
            .filter(|p| by_id.contains_key(p) && *p != r.span_id);
        children.entry(parent).or_default().push(i);
    }
    for v in children.values_mut() {
        v.sort_by(|a, b| sort_key(&rows[*a]).cmp(&sort_key(&rows[*b])));
    }
    let mut out = Vec::with_capacity(rows.len());
    let mut seen = vec![false; rows.len()];
    let mut stack: Vec<(usize, usize)> = children
        .get(&None)
        .map(|v| v.iter().rev().map(|i| (*i, 0)).collect())
        .unwrap_or_default();
    loop {
        while let Some((i, d)) = stack.pop() {
            if seen[i] {
                continue;
            }
            seen[i] = true;
            out.push((i, d));
            if let Some(c) = children.get(&Some(rows[i].span_id)) {
                for j in c.iter().rev() {
                    stack.push((*j, d + 1));
                }
            }
        }
        // Cycles (malformed input) leave spans unvisited; emit them as roots.
        match seen.iter().position(|s| !s) {
            Some(i) => stack.push((i, 0)),
            None => break,
        }
    }
    out
}

fn parse_json(s: &Option<String>) -> Value {
    match s {
        None => Value::Null,
        Some(s) => serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone())),
    }
}

fn ts(ns: Option<i64>) -> Value {
    match ns {
        None => Value::Null,
        Some(ns) => {
            let dt = chrono_like(ns);
            Value::String(dt)
        }
    }
}

/// RFC 3339 with nanoseconds, without pulling chrono into this crate's API.
fn chrono_like(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{nanos:09}Z",
        tod / 3600,
        (tod / 60) % 60,
        tod % 60
    )
}

// Howard Hinnant's days-to-civil algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn format_ts(ns: i64) -> String {
    chrono_like(ns)
}

/// A span as API JSON. Payload columns are parsed; `include_payloads` false leaves them out.
pub fn span_json(r: &SpanRow, include_payloads: bool) -> Map<String, Value> {
    let latency_ms = match (r.start_time, r.end_time) {
        (Some(s), Some(e)) => json!((e - s) as f64 / 1e6),
        _ => Value::Null,
    };
    let mut m = Map::new();
    m.insert("project_id".into(), json!(r.project_id));
    m.insert("trace_id".into(), json!(trace_id_hex(&r.trace_id)));
    m.insert("span_id".into(), json!(span_id_hex(&r.span_id)));
    m.insert(
        "parent_span_id".into(),
        json!(r.parent_span_id.as_ref().map(span_id_hex)),
    );
    m.insert("dotted_order".into(), json!(r.dotted_order));
    m.insert("name".into(), json!(r.name));
    m.insert("run_type".into(), json!(r.run_type));
    m.insert("start_time".into(), ts(r.start_time));
    m.insert("end_time".into(), ts(r.end_time));
    m.insert("latency_ms".into(), latency_ms);
    m.insert(
        "status".into(),
        json!(r.status.as_deref().unwrap_or("unset")),
    );
    m.insert("error".into(), json!(r.error));
    m.insert("model".into(), json!(r.model));
    m.insert("provider".into(), json!(r.provider));
    m.insert("prompt_tokens".into(), json!(r.prompt_tokens));
    m.insert("completion_tokens".into(), json!(r.completion_tokens));
    m.insert("total_tokens".into(), json!(r.total_tokens));
    m.insert("cost_usd".into(), json!(r.cost_usd));
    m.insert("session_id".into(), json!(r.session_id));
    m.insert("tags".into(), json!(r.tags.clone().unwrap_or_default()));
    m.insert("metadata".into(), parse_json(&r.metadata));
    if include_payloads {
        m.insert("inputs".into(), parse_json(&r.inputs));
        m.insert("outputs".into(), parse_json(&r.outputs));
        m.insert("events".into(), parse_json(&r.events));
        m.insert("attributes".into(), parse_json(&r.attributes));
    }
    m
}

/// Nest rows into `{..span, children: [...]}` trees. Returns the roots.
pub fn nest(rows: &[SpanRow], include_payloads: bool) -> Vec<Value> {
    let order = tree_order(rows);
    // Build bottom-up: process in reverse pre-order so children are complete first.
    let mut nodes: Vec<Option<Map<String, Value>>> = rows
        .iter()
        .map(|r| Some(span_json(r, include_payloads)))
        .collect();
    let mut kids: Vec<Vec<usize>> = vec![Vec::new(); rows.len()];
    let mut parent_of: Vec<Option<usize>> = vec![None; rows.len()];
    let mut stack: Vec<(usize, usize)> = Vec::new();
    for &(i, depth) in &order {
        while stack.last().is_some_and(|(_, d)| *d >= depth) {
            stack.pop();
        }
        if let Some((p, _)) = stack.last() {
            parent_of[i] = Some(*p);
            kids[*p].push(i);
        }
        stack.push((i, depth));
    }
    for &(i, _) in order.iter().rev() {
        let children: Vec<Value> = kids[i]
            .iter()
            .map(|c| Value::Object(nodes[*c].take().unwrap_or_default()))
            .collect();
        if let Some(n) = nodes[i].as_mut() {
            n.insert("children".into(), Value::Array(children));
        }
    }
    order
        .iter()
        .filter(|(i, _)| parent_of[*i].is_none())
        .map(|(i, _)| Value::Object(nodes[*i].take().unwrap_or_default()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(id: u8, parent: Option<u8>, t: i64) -> SpanRow {
        let mut r = SpanRow::new("p", [1; 16], [id; 8]);
        r.parent_span_id = parent.map(|p| [p; 8]);
        r.start_time = Some(t);
        r
    }

    #[test]
    fn orders_and_nests() {
        let rows = vec![
            span(3, Some(1), 30),
            span(1, None, 0),
            span(2, Some(1), 20),
            span(4, Some(2), 25),
            span(9, Some(8), 5),
        ];
        let order: Vec<(u8, usize)> = tree_order(&rows)
            .iter()
            .map(|(i, d)| (rows[*i].span_id[0], *d))
            .collect();
        assert_eq!(order, vec![(1, 0), (2, 1), (4, 2), (3, 1), (9, 0)]);
        let roots = nest(&rows, false);
        assert_eq!(roots.len(), 2);
        assert_eq!(
            roots[0]["children"][0]["children"][0]["span_id"],
            "0404040404040404"
        );
        assert_eq!(
            format_ts(1_761_148_800_000_000_123),
            "2025-10-22T16:00:00.000000123Z"
        );
    }

    #[test]
    fn survives_cycles() {
        let rows = vec![span(1, Some(2), 0), span(2, Some(1), 1)];
        assert_eq!(tree_order(&rows).len(), 2);
    }
}
