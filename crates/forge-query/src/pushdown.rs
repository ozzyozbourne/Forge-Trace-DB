//! Translate DataFusion filters into a [`ScanPredicate`]. Only filters on stable columns are
//! taken, because the predicate is applied before the version merge. Everything is reported
//! to DataFusion as `Inexact`, so it re-applies every filter after the scan anyway.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use forge_model::row::{SpanId, TraceId, parse_span_id, parse_trace_id};
use forge_model::schema::{PARENT_SPAN_ID, PROJECT_ID, SPAN_ID, START_TIME, TRACE_ID};
use forge_model::traits::ScanPredicate;

fn strip_cast(e: &Expr) -> &Expr {
    match e {
        Expr::Cast(c) => strip_cast(&c.expr),
        Expr::TryCast(c) => strip_cast(&c.expr),
        Expr::Alias(a) => strip_cast(&a.expr),
        other => other,
    }
}

fn column_name(e: &Expr) -> Option<&str> {
    match strip_cast(e) {
        Expr::Column(c) => Some(c.name.as_str()),
        _ => None,
    }
}

fn literal(e: &Expr) -> Option<&ScalarValue> {
    match strip_cast(e) {
        Expr::Literal(v, _) => Some(v),
        _ => None,
    }
}

fn bytes_of(v: &ScalarValue) -> Option<Vec<u8>> {
    match v {
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Some(b.clone()),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => hex::decode(s.replace('-', "")).ok(),
        _ => None,
    }
}

fn trace_id(v: &ScalarValue) -> Option<TraceId> {
    bytes_of(v)?.try_into().ok()
}

fn span_id(v: &ScalarValue) -> Option<SpanId> {
    bytes_of(v)?.try_into().ok()
}

fn string(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Timestamp literal in nanoseconds, any unit.
pub fn timestamp_ns(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::TimestampNanosecond(Some(x), _) => Some(*x),
        ScalarValue::TimestampMicrosecond(Some(x), _) => x.checked_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(x), _) => x.checked_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(x), _) => x.checked_mul(1_000_000_000),
        _ => None,
    }
}

fn flip(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        o => o,
    }
}

/// Narrow `p` with one conjunct. Returns false when the filter proves no row can match.
fn apply(p: &mut ScanPredicate, e: &Expr) -> bool {
    match e {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => apply(p, left) && apply(p, right),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (col, lit, op) = match (
                column_name(left),
                literal(right),
                column_name(right),
                literal(left),
            ) {
                (Some(c), Some(l), _, _) => (c, l, *op),
                (_, _, Some(c), Some(l)) => (c, l, flip(*op)),
                _ => return true,
            };
            match (col, op) {
                (PROJECT_ID, Operator::Eq) => {
                    if let Some(s) = string(lit) {
                        if p.project_id.as_ref().is_some_and(|x| x != &s) {
                            return false;
                        }
                        p.project_id = Some(s);
                    }
                }
                (TRACE_ID, Operator::Eq) => {
                    if let Some(t) = trace_id(lit) {
                        p.trace_ids = Some(match p.trace_ids.take() {
                            Some(existing) => existing.into_iter().filter(|x| *x == t).collect(),
                            None => vec![t],
                        });
                    }
                }
                (SPAN_ID, Operator::Eq) => {
                    if let Some(s) = span_id(lit) {
                        p.span_ids = Some(match p.span_ids.take() {
                            Some(existing) => existing.into_iter().filter(|x| *x == s).collect(),
                            None => vec![s],
                        });
                    }
                }
                (
                    START_TIME,
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq,
                ) => {
                    if let Some(ns) = timestamp_ns(lit) {
                        let (lo, hi) = match op {
                            Operator::Gt => (Some(ns.saturating_add(1)), None),
                            Operator::GtEq => (Some(ns), None),
                            Operator::Lt => (None, Some(ns)),
                            Operator::LtEq => (None, Some(ns.saturating_add(1))),
                            _ => (Some(ns), Some(ns.saturating_add(1))),
                        };
                        if let Some(lo) = lo {
                            p.start_min = Some(p.start_min.map_or(lo, |m| m.max(lo)));
                        }
                        if let Some(hi) = hi {
                            p.start_max = Some(p.start_max.map_or(hi, |m| m.min(hi)));
                        }
                    }
                }
                _ => {}
            }
            true
        }
        Expr::Between(b) if !b.negated && column_name(&b.expr) == Some(START_TIME) => {
            if let (Some(lo), Some(hi)) = (
                literal(&b.low).and_then(timestamp_ns),
                literal(&b.high).and_then(timestamp_ns),
            ) {
                p.start_min = Some(p.start_min.map_or(lo, |m| m.max(lo)));
                let hi = hi.saturating_add(1);
                p.start_max = Some(p.start_max.map_or(hi, |m| m.min(hi)));
            }
            true
        }
        Expr::InList(l) if !l.negated => {
            match column_name(&l.expr) {
                Some(TRACE_ID) => {
                    let ids: Option<Vec<TraceId>> = l
                        .list
                        .iter()
                        .map(|e| literal(e).and_then(trace_id))
                        .collect();
                    if let Some(ids) = ids {
                        p.trace_ids = Some(ids);
                    }
                }
                Some(SPAN_ID) => {
                    let ids: Option<Vec<SpanId>> = l
                        .list
                        .iter()
                        .map(|e| literal(e).and_then(span_id))
                        .collect();
                    if let Some(ids) = ids {
                        p.span_ids = Some(ids);
                    }
                }
                _ => {}
            }
            true
        }
        Expr::IsNull(inner) if column_name(inner) == Some(PARENT_SPAN_ID) => {
            p.roots_only = true;
            true
        }
        _ => true,
    }
}

/// Build a predicate from DataFusion filters. `None` means no row can match.
pub fn predicate_from_filters(filters: &[Expr], base: ScanPredicate) -> Option<ScanPredicate> {
    let mut p = base;
    for f in filters {
        if !apply(&mut p, f) {
            return None;
        }
    }
    if p.trace_ids.as_ref().is_some_and(|t| t.is_empty())
        || p.span_ids.as_ref().is_some_and(|s| s.is_empty())
        || matches!((p.start_min, p.start_max), (Some(a), Some(b)) if a >= b)
    {
        return None;
    }
    Some(p)
}

/// Accept hex ids in API paths.
pub fn parse_ids(trace: &str, span: Option<&str>) -> Option<(TraceId, Option<SpanId>)> {
    let t = parse_trace_id(trace)?;
    let s = match span {
        Some(s) => Some(parse_span_id(s)?),
        None => None,
    };
    Some((t, s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    #[test]
    fn extracts_stable_filters() {
        let t = [7u8; 16];
        let filters = vec![
            col("project_id").eq(lit("p")),
            col("trace_id").eq(lit(ScalarValue::FixedSizeBinary(16, Some(t.to_vec())))),
            col("start_time").gt_eq(lit(ScalarValue::TimestampNanosecond(Some(100), None))),
            col("start_time").lt(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            col("parent_span_id").is_null(),
            col("status").eq(lit("error")),
        ];
        let p = predicate_from_filters(&filters, ScanPredicate::default()).unwrap();
        assert_eq!(p.project_id.as_deref(), Some("p"));
        assert_eq!(p.trace_ids, Some(vec![t]));
        assert_eq!((p.start_min, p.start_max), (Some(100), Some(1_000_000)));
        assert!(p.roots_only);
        // Contradictions short-circuit.
        let base = ScanPredicate {
            project_id: Some("a".into()),
            ..Default::default()
        };
        assert!(predicate_from_filters(&[col("project_id").eq(lit("b"))], base).is_none());
        let hex = "0707070707070707070707070707070707".to_string();
        assert!(
            predicate_from_filters(&[col("trace_id").eq(lit(hex))], ScanPredicate::default())
                .is_some()
        );
    }
}
