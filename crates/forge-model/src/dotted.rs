//! `dotted_order`: for each ancestor from the root down, `<start time>Z<span id>`, joined by `.`.
//! Same idea as LangSmith runs, so sorting a trace's rows by this string yields a depth-first
//! tree order with siblings ordered by start time.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::row::{SpanId, SpanRow, TraceId, span_id_hex};

/// One segment of a dotted order: `20240101T120000123456Z` + 16 hex chars.
pub fn part(start_ns: i64, span_id: &SpanId) -> String {
    let dt: DateTime<Utc> = DateTime::from_timestamp_nanos(start_ns);
    format!("{}{}", dt.format("%Y%m%dT%H%M%S%6fZ"), span_id_hex(span_id))
}

/// Fill `dotted_order` for rows whose ancestors are known, either in `rows` itself or via
/// `lookup` (e.g. the memtable). Rows with an unknown ancestor or no start time keep `None`
/// and are filled by compaction.
pub fn fill(rows: &mut [SpanRow], lookup: &dyn Fn(&TraceId, &SpanId) -> Option<String>) {
    let mut index: HashMap<(TraceId, SpanId), usize> = HashMap::with_capacity(rows.len());
    for (i, r) in rows.iter().enumerate() {
        // Prefer a row that carries a start time when a span appears twice.
        match index.get(&(r.trace_id, r.span_id)) {
            Some(&j) if rows[j].start_time.is_some() => {}
            _ => {
                index.insert((r.trace_id, r.span_id), i);
            }
        }
    }
    let mut memo: HashMap<(TraceId, SpanId), Option<String>> = HashMap::new();
    for i in 0..rows.len() {
        if rows[i].dotted_order.is_some() {
            continue;
        }
        let key = (rows[i].trace_id, rows[i].span_id);
        let d = resolve(key, rows, &index, lookup, &mut memo, 0);
        rows[i].dotted_order = d;
    }
}

fn resolve(
    key: (TraceId, SpanId),
    rows: &[SpanRow],
    index: &HashMap<(TraceId, SpanId), usize>,
    lookup: &dyn Fn(&TraceId, &SpanId) -> Option<String>,
    memo: &mut HashMap<(TraceId, SpanId), Option<String>>,
    depth: usize,
) -> Option<String> {
    if let Some(m) = memo.get(&key) {
        return m.clone();
    }
    // Guard against cycles in malformed input.
    if depth > 10_000 {
        return None;
    }
    let result = match index.get(&key) {
        None => lookup(&key.0, &key.1),
        Some(&i) => {
            let r = &rows[i];
            if let Some(d) = &r.dotted_order {
                Some(d.clone())
            } else {
                match (r.start_time, r.parent_span_id) {
                    (None, _) => None,
                    (Some(st), None) => Some(part(st, &r.span_id)),
                    (Some(st), Some(p)) => {
                        resolve((key.0, p), rows, index, lookup, memo, depth + 1)
                            .map(|pd| format!("{pd}.{}", part(st, &r.span_id)))
                    }
                }
            }
        }
    };
    memo.insert(key, result.clone());
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn span(id: u8, parent: Option<u8>, t: i64) -> SpanRow {
        let mut r = SpanRow::new("p", [9; 16], [id; 8]);
        r.parent_span_id = parent.map(|p| [p; 8]);
        r.start_time = Some(t);
        r
    }

    #[test]
    fn formats() {
        let p = part(1_700_000_000_123_456_789, &[0xab; 8]);
        assert_eq!(p, "20231114T221320123456Zabababababababab");
    }

    #[test]
    fn fills_out_of_order_and_external() {
        let mut rows = vec![
            span(3, Some(2), 30),
            span(2, Some(1), 20),
            span(5, Some(4), 50),
        ];
        let root = part(10, &[1; 8]);
        let root_c = root.clone();
        fill(&mut rows, &move |_, s| {
            (s == &[1; 8]).then(|| root_c.clone())
        });
        let d2 = format!("{root}.{}", part(20, &[2; 8]));
        assert_eq!(rows[1].dotted_order.as_deref(), Some(d2.as_str()));
        assert_eq!(
            rows[0].dotted_order,
            Some(format!("{d2}.{}", part(30, &[3; 8])))
        );
        assert_eq!(rows[2].dotted_order, None);
    }

    proptest! {
        // Sorting by dotted_order puts every parent before its children.
        #[test]
        fn parents_sort_first(parents in proptest::collection::vec(any::<prop::sample::Index>(), 1..60), times in proptest::collection::vec(0i64..1_000_000_000, 61)) {
            let mut rows = vec![span(0, None, times[0])];
            for (i, p) in parents.iter().enumerate() {
                let id = (i + 1) as u8;
                let parent = p.index(i + 1) as u8;
                rows.push(span(id, Some(parent), times[i + 1]));
            }
            fill(&mut rows, &|_, _| None);
            let mut sorted = rows.clone();
            sorted.sort_by(|a, b| a.dotted_order.cmp(&b.dotted_order));
            let pos: HashMap<SpanId, usize> = sorted.iter().enumerate().map(|(i, r)| (r.span_id, i)).collect();
            for r in &rows {
                prop_assert!(r.dotted_order.is_some());
                if let Some(p) = r.parent_span_id {
                    prop_assert!(pos[&p] < pos[&r.span_id]);
                }
            }
        }
    }
}
