//! `POST /v1/search` and `GET /v1/runs?q=`.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use forge_index::terms::{parse_query, path_query};
use forge_model::ForgeError;
use forge_query::fastpath::RunFilter;
use forge_query::tree::span_json;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Forge;
use crate::error::ApiResult;

#[derive(Deserialize)]
pub struct SearchRequest {
    pub query: String,
    pub project: Option<String>,
    /// Restrict to one indexed column: name, error, inputs, outputs, metadata.
    pub column: Option<String>,
    /// Match only text under this JSON path of `column` (default `inputs`).
    pub path: Option<String>,
    #[serde(default)]
    pub roots_only: bool,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    #[serde(default)]
    pub filters: RunFilter,
}

pub async fn run_search(
    f: &Forge,
    project: &str,
    text: &str,
    column: Option<&str>,
    filter: &RunFilter,
    limit: usize,
    cursor: Option<&str>,
) -> Result<Value, ForgeError> {
    run_search_expr(
        f,
        project,
        parse_query(text, column)?,
        filter,
        false,
        limit,
        cursor,
    )
    .await
}

async fn run_search_expr(
    f: &Forge,
    project: &str,
    expr: forge_model::traits::SearchExpr,
    filter: &RunFilter,
    roots_only: bool,
    limit: usize,
    cursor: Option<&str>,
) -> Result<Value, ForgeError> {
    let started = std::time::Instant::now();
    let page = forge_query::search::search(
        &f.query.reader,
        project,
        &expr,
        filter,
        roots_only,
        limit,
        cursor,
    )
    .await?;
    Ok(json!({
        "runs": page.runs.iter().map(|r| Value::Object(span_json(r, false))).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "elapsed_ms": started.elapsed().as_secs_f64() * 1e3,
    }))
}

pub async fn search(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Json(req): Json<SearchRequest>,
) -> ApiResult<Json<Value>> {
    let principal = f.auth.authenticate(&headers).await?;
    let project = f.resolve_project(&principal, req.project.as_deref())?;
    if let Some(c) = &req.column
        && !forge_model::schema::INDEXED_COLUMNS.contains(&c.as_str())
    {
        return Err(ForgeError::invalid(format!(
            "column must be one of {:?}",
            forge_model::schema::INDEXED_COLUMNS
        ))
        .into());
    }
    let expr = match &req.path {
        Some(p) => path_query(req.column.as_deref().unwrap_or("inputs"), p, &req.query)?,
        None => parse_query(&req.query, req.column.as_deref())?,
    };
    let v = run_search_expr(
        &f,
        &project,
        expr,
        &req.filters,
        req.roots_only,
        req.limit.unwrap_or(50),
        req.cursor.as_deref(),
    )
    .await?;
    Ok(Json(v))
}
