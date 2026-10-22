//! Index terms. Each token is indexed twice: plain, and qualified by the JSON path it was found
//! under, so `search()` and `json_key_search()` share one index:
//!
//! ```text
//! <column> \0 <token>
//! <column> \0 <json path> \0 <token>      e.g.  inputs\0$.messages[*].content\0refund
//! ```

use std::collections::HashSet;

use forge_model::schema::INDEXED_COLUMNS;
use forge_model::traits::SearchExpr;
use forge_model::{ForgeError, Result};
use serde_json::Value;

use crate::tokenize::{MAX_FIELD_BYTES, for_each_token, tokenize};

pub const SEP: u8 = 0;

pub fn plain_term(column: &str, token: &str) -> Vec<u8> {
    let mut t = Vec::with_capacity(column.len() + token.len() + 1);
    t.extend_from_slice(column.as_bytes());
    t.push(SEP);
    t.extend_from_slice(token.as_bytes());
    t
}

pub fn path_term(column: &str, path: &str, token: &str) -> Vec<u8> {
    let mut t = Vec::with_capacity(column.len() + path.len() + token.len() + 2);
    t.extend_from_slice(column.as_bytes());
    t.push(SEP);
    t.extend_from_slice(path.as_bytes());
    t.push(SEP);
    t.extend_from_slice(token.as_bytes());
    t
}

/// Whether a column holds JSON that should be flattened.
pub fn is_json_column(column: &str) -> bool {
    matches!(column, "inputs" | "outputs" | "metadata")
}

/// Normalize a user-written JSON path: `messages[0].content` -> `$.messages[*].content`.
pub fn normalize_path(path: &str) -> String {
    let p = path.trim();
    let p = p.strip_prefix('$').unwrap_or(p);
    let p = p.strip_prefix('.').unwrap_or(p);
    let mut out = String::from("$");
    let mut chars = p.chars().peekable();
    let mut seg = String::new();
    let flush = |out: &mut String, seg: &mut String| {
        if !seg.is_empty() {
            out.push('.');
            out.push_str(seg);
            seg.clear();
        }
    };
    while let Some(c) = chars.next() {
        match c {
            '.' => flush(&mut out, &mut seg),
            '[' => {
                flush(&mut out, &mut seg);
                let mut inner = String::new();
                for c in chars.by_ref() {
                    if c == ']' {
                        break;
                    }
                    inner.push(c);
                }
                let inner = inner.trim_matches(|c| c == '"' || c == '\'');
                if inner == "*" || inner.chars().all(|c| c.is_ascii_digit()) {
                    out.push_str("[*]");
                } else {
                    out.push('.');
                    out.push_str(inner);
                }
            }
            c => seg.push(c),
        }
    }
    flush(&mut out, &mut seg);
    out
}

/// Visit every string-ish leaf of a JSON value with its normalized path.
pub fn walk_json(v: &Value, path: &mut String, f: &mut dyn FnMut(&str, &str)) {
    match v {
        Value::Object(m) => {
            for (k, child) in m {
                let len = path.len();
                path.push('.');
                path.push_str(k);
                walk_json(child, path, f);
                path.truncate(len);
            }
        }
        Value::Array(a) => {
            let len = path.len();
            path.push_str("[*]");
            for child in a {
                walk_json(child, path, f);
            }
            path.truncate(len);
        }
        Value::String(s) => f(path, s),
        Value::Number(n) => f(path, &n.to_string()),
        Value::Bool(b) => f(path, if *b { "true" } else { "false" }),
        Value::Null => {}
    }
}

/// All index terms for one column value. The slice passed to `out` is only valid for the
/// duration of the call.
pub fn terms_for(column: &str, text: &str, out: &mut dyn FnMut(&[u8])) {
    let text = if text.len() > MAX_FIELD_BYTES {
        let mut end = MAX_FIELD_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        &text[..end]
    } else {
        text
    };
    let parsed = if is_json_column(column) && text.trim_start().starts_with(['{', '[']) {
        serde_json::from_str::<Value>(text).ok()
    } else {
        None
    };
    let mut plain: Vec<u8> = Vec::with_capacity(64);
    plain.extend_from_slice(column.as_bytes());
    plain.push(SEP);
    let base = plain.len();
    match parsed {
        Some(v) => {
            let mut qualified: Vec<u8> = Vec::with_capacity(128);
            let mut path = String::from("$");
            walk_json(&v, &mut path, &mut |p, s| {
                qualified.clear();
                qualified.extend_from_slice(column.as_bytes());
                qualified.push(SEP);
                qualified.extend_from_slice(p.as_bytes());
                qualified.push(SEP);
                let qbase = qualified.len();
                for_each_token(s, |t| {
                    plain.truncate(base);
                    plain.extend_from_slice(t.as_bytes());
                    out(&plain);
                    qualified.truncate(qbase);
                    qualified.extend_from_slice(t.as_bytes());
                    out(&qualified);
                });
            });
        }
        None => for_each_token(text, |t| {
            plain.truncate(base);
            plain.extend_from_slice(t.as_bytes());
            out(&plain);
        }),
    }
}

/// Parse a search string. Whitespace-separated clauses are ANDed; `OR` between clauses unions
/// them; `column:word` restricts a clause to one indexed column.
pub fn parse_query(q: &str, default_column: Option<&str>) -> Result<SearchExpr> {
    let mut or_groups: Vec<Vec<SearchExpr>> = vec![vec![]];
    for clause in q.split_whitespace() {
        if clause == "OR" {
            or_groups.push(vec![]);
            continue;
        }
        if clause == "AND" {
            continue;
        }
        let (column, text) = match clause.split_once(':') {
            Some((c, t)) if INDEXED_COLUMNS.contains(&c) => (Some(c.to_string()), t),
            _ => (default_column.map(str::to_string), clause),
        };
        for token in tokenize(text) {
            or_groups.last_mut().unwrap().push(SearchExpr::Term {
                column: column.clone(),
                path: None,
                token,
            });
        }
    }
    let mut ors: Vec<SearchExpr> = or_groups
        .into_iter()
        .filter(|g| !g.is_empty())
        .map(|mut g| {
            if g.len() == 1 {
                g.pop().unwrap()
            } else {
                SearchExpr::And(g)
            }
        })
        .collect();
    match ors.len() {
        0 => Err(ForgeError::invalid("search query has no searchable terms")),
        1 => Ok(ors.pop().unwrap()),
        _ => Ok(SearchExpr::Or(ors)),
    }
}

/// Query for text under one JSON path of one column (all tokens must match).
pub fn path_query(column: &str, path: &str, text: &str) -> Result<SearchExpr> {
    let path = normalize_path(path);
    let mut terms: Vec<SearchExpr> = tokenize(text)
        .into_iter()
        .map(|token| SearchExpr::Term {
            column: Some(column.to_string()),
            path: Some(path.clone()),
            token,
        })
        .collect();
    match terms.len() {
        0 => Err(ForgeError::invalid("search text has no searchable terms")),
        1 => Ok(terms.pop().unwrap()),
        _ => Ok(SearchExpr::And(terms)),
    }
}

/// The index keys a leaf term can match (several when the column is unspecified).
pub fn leaf_keys(column: &Option<String>, path: &Option<String>, token: &str) -> Vec<Vec<u8>> {
    let cols: Vec<&str> = match column {
        Some(c) => vec![c.as_str()],
        None => INDEXED_COLUMNS.to_vec(),
    };
    cols.into_iter()
        .map(|c| match path {
            Some(p) => path_term(c, p, token),
            None => plain_term(c, token),
        })
        .collect()
}

/// Brute-force evaluation against one row, for the memtable and for verifying index hits.
pub fn matches(expr: &SearchExpr, text_of: &dyn Fn(&str) -> Option<String>) -> bool {
    // Collect the terms of the columns the expression mentions.
    fn columns(e: &SearchExpr, out: &mut HashSet<String>) {
        match e {
            SearchExpr::Term {
                column: Some(c), ..
            } => {
                out.insert(c.clone());
            }
            SearchExpr::Term { column: None, .. } => {
                out.extend(INDEXED_COLUMNS.iter().map(|c| c.to_string()));
            }
            SearchExpr::And(v) | SearchExpr::Or(v) => v.iter().for_each(|x| columns(x, out)),
        }
    }
    let mut cols = HashSet::new();
    columns(expr, &mut cols);
    let mut terms: HashSet<Vec<u8>> = HashSet::new();
    for c in &cols {
        if let Some(t) = text_of(c) {
            terms_for(c, &t, &mut |k| {
                if !terms.contains(k) {
                    terms.insert(k.to_vec());
                }
            });
        }
    }
    fn eval(e: &SearchExpr, terms: &HashSet<Vec<u8>>) -> bool {
        match e {
            SearchExpr::Term {
                column,
                path,
                token,
            } => leaf_keys(column, path, token)
                .iter()
                .any(|k| terms.contains(k)),
            SearchExpr::And(v) => v.iter().all(|x| eval(x, terms)),
            SearchExpr::Or(v) => v.iter().any(|x| eval(x, terms)),
        }
    }
    eval(expr, &terms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_terms() {
        let mut terms = vec![];
        terms_for(
            "inputs",
            r#"{"messages":[{"role":"user","content":"Refund please"}]}"#,
            &mut |t| terms.push(String::from_utf8(t.to_vec()).unwrap().replace('\0', "|")),
        );
        assert!(terms.contains(&"inputs|refund".to_string()));
        assert!(terms.contains(&"inputs|$.messages[*].content|refund".to_string()));
        assert!(terms.contains(&"inputs|$.messages[*].role|user".to_string()));
    }

    #[test]
    fn paths() {
        assert_eq!(
            normalize_path("$.messages[*].content"),
            "$.messages[*].content"
        );
        assert_eq!(
            normalize_path("messages[0].content"),
            "$.messages[*].content"
        );
        assert_eq!(normalize_path("$['a'].b"), "$.a.b");
    }

    #[test]
    fn queries() {
        let q = parse_query("refund policy", None).unwrap();
        assert!(matches!(q, SearchExpr::And(ref v) if v.len() == 2));
        let q = parse_query("inputs:refund OR error:timeout", None).unwrap();
        assert!(matches!(q, SearchExpr::Or(_)));
        assert!(parse_query("  ", None).is_err());

        let text = |c: &str| match c {
            "inputs" => Some(r#"{"messages":[{"content":"I want a refund"}]}"#.to_string()),
            "name" => Some("ChatOpenAI".to_string()),
            _ => None,
        };
        assert!(matches(&parse_query("refund", None).unwrap(), &text));
        assert!(matches(
            &parse_query("chatopenai refund", None).unwrap(),
            &text
        ));
        assert!(!matches(&parse_query("name:refund", None).unwrap(), &text));
        assert!(matches(
            &path_query("inputs", "messages[0].content", "REFUND").unwrap(),
            &text
        ));
        assert!(!matches(
            &path_query("inputs", "$.other", "refund").unwrap(),
            &text
        ));
    }
}
