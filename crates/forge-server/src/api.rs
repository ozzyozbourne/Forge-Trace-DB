//! The JSON API on port 8080.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use forge_model::row::{parse_span_id, parse_trace_id};
use forge_model::traits::ProjectRecord;
use forge_model::{ForgeError, now_ms};
use forge_query::fastpath::{self, RunFilter};
use forge_query::tree::span_json;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Forge;
use crate::auth::{Principal, generate_key, hash_key};
use crate::error::{ApiError, ApiResult};

/// Parse a time given as RFC 3339 or as an integer (ns, ms or s by magnitude).
pub fn parse_time(s: &str) -> Result<i64, ForgeError> {
    if let Ok(n) = s.parse::<i64>() {
        return Ok(if n.abs() >= 100_000_000_000_000_000 {
            n
        } else if n.abs() >= 100_000_000_000 {
            n * 1_000_000
        } else {
            n * 1_000_000_000
        });
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|d| d.timestamp_nanos_opt().unwrap_or(i64::MAX))
        .map_err(|_| {
            ForgeError::invalid(format!("invalid time {s:?}: use RFC 3339 or epoch ns/ms/s"))
        })
}

impl Forge {
    /// The project a request reads or writes, checked against its principal.
    pub fn resolve_project(
        &self,
        principal: &Principal,
        requested: Option<&str>,
    ) -> Result<String, ForgeError> {
        let p = match (principal, requested) {
            (Principal::Project(own), Some(r)) if r != own => {
                return Err(ForgeError::Unauthorized(format!(
                    "key does not grant access to project {r}"
                )));
            }
            (Principal::Project(own), _) => own.clone(),
            (Principal::Admin { project }, r) => r
                .map(str::to_string)
                .or_else(|| project.clone())
                .unwrap_or_else(|| self.config.ingest.default_project.clone()),
        };
        forge_storage::paths::validate_project_id(&p)?;
        Ok(p)
    }
}

#[derive(Deserialize, Default)]
struct ProjectQuery {
    project: Option<String>,
    payloads: Option<bool>,
}

async fn get_trace(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    Query(q): Query<ProjectQuery>,
) -> ApiResult<Json<Value>> {
    let principal = f.auth.authenticate(&headers).await?;
    let project = f.resolve_project(&principal, q.project.as_deref())?;
    let trace = parse_trace_id(&trace_id)
        .ok_or_else(|| ForgeError::invalid("trace id must be 32 hex chars"))?;
    let tree = fastpath::trace_tree(
        &f.query.reader,
        &project,
        &trace,
        q.payloads.unwrap_or(true),
    )
    .await?;
    tree.map(Json)
        .ok_or_else(|| ApiError(ForgeError::NotFound(format!("trace {trace_id}"))))
}

async fn get_span(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Path((trace_id, span_id)): Path<(String, String)>,
    Query(q): Query<ProjectQuery>,
) -> ApiResult<Json<Value>> {
    let principal = f.auth.authenticate(&headers).await?;
    let project = f.resolve_project(&principal, q.project.as_deref())?;
    let trace = parse_trace_id(&trace_id).ok_or_else(|| ForgeError::invalid("invalid trace id"))?;
    let span = parse_span_id(&span_id).ok_or_else(|| ForgeError::invalid("invalid span id"))?;
    let s = fastpath::get_span(&f.query.reader, &project, &trace, &span).await?;
    s.map(Json)
        .ok_or_else(|| ApiError(ForgeError::NotFound(format!("span {span_id}"))))
}

/// Build a [`RunFilter`] from query parameters.
fn run_filter(q: &HashMap<String, String>) -> Result<RunFilter, ForgeError> {
    let num = |k: &str| -> Result<Option<f64>, ForgeError> {
        q.get(k)
            .map(|v| {
                v.parse::<f64>()
                    .map_err(|_| ForgeError::invalid(format!("{k} must be a number")))
            })
            .transpose()
    };
    let int = |k: &str| -> Result<Option<i64>, ForgeError> {
        q.get(k)
            .map(|v| {
                v.parse::<i64>()
                    .map_err(|_| ForgeError::invalid(format!("{k} must be an integer")))
            })
            .transpose()
    };
    let mut metadata = BTreeMap::new();
    for (k, v) in q {
        if let Some(key) = k.strip_prefix("metadata.") {
            metadata.insert(key.to_string(), v.clone());
        }
    }
    if let Some(m) = q.get("metadata") {
        for pair in m.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                metadata.insert(k.to_string(), v.to_string());
            }
        }
    }
    Ok(RunFilter {
        start: q.get("start").map(|s| parse_time(s)).transpose()?,
        end: q.get("end").map(|s| parse_time(s)).transpose()?,
        run_type: q.get("run_type").cloned(),
        status: q.get("status").cloned(),
        name: q.get("name").cloned(),
        session_id: q.get("session_id").cloned(),
        model: q.get("model").cloned(),
        tag: q.get("tag").cloned(),
        min_latency_ms: num("min_latency_ms")?,
        max_latency_ms: num("max_latency_ms")?,
        min_tokens: int("min_tokens")?,
        max_tokens: int("max_tokens")?,
        min_cost: num("min_cost")?,
        max_cost: num("max_cost")?,
        metadata,
        trace_id: q.get("trace_id").cloned(),
        all_spans: q.get("all_spans").is_some_and(|v| v == "true" || v == "1"),
    })
}

async fn list_runs(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let principal = f.auth.authenticate(&headers).await?;
    let project = f.resolve_project(&principal, q.get("project").map(String::as_str))?;
    let filter = run_filter(&q)?;
    let limit = q
        .get("limit")
        .map(|l| {
            l.parse::<usize>()
                .map_err(|_| ForgeError::invalid("limit must be an integer"))
        })
        .transpose()?
        .unwrap_or(50);
    if let Some(text) = q.get("q").filter(|s| !s.trim().is_empty()) {
        let page = crate::search::run_search(
            &f,
            &project,
            text,
            None,
            &filter,
            limit,
            q.get("cursor").map(String::as_str),
        )
        .await?;
        return Ok(Json(page));
    }
    let page = fastpath::list_runs(
        &f.query.reader,
        &project,
        &filter,
        limit,
        q.get("cursor").map(String::as_str),
    )
    .await?;
    Ok(Json(json!({
        "runs": page.runs.iter().map(|r| Value::Object(span_json(r, false))).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
    })))
}

#[derive(Deserialize)]
struct SqlRequest {
    query: String,
    project: Option<String>,
    /// `json` (default) or `table`.
    format: Option<String>,
}

async fn sql(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> ApiResult<Response> {
    let principal = f.auth.authenticate(&headers).await?;
    // Project keys always see only their project. Admins see everything unless they name one.
    let scope = match &principal {
        Principal::Project(p) => {
            Some(f.resolve_project(&principal, req.project.as_deref().or(Some(p)))?)
        }
        Principal::Admin { .. } => req.project.clone(),
    };
    let started = std::time::Instant::now();
    let result = f.query.sql(&req.query, scope).await?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1e3;
    if req.format.as_deref() == Some("table") {
        return Ok((StatusCode::OK, forge_query::to_pretty(&result.batches)?).into_response());
    }
    Ok(Json(json!({
        "columns": result.schema.fields().iter().map(|f| json!({"name": f.name(), "type": f.data_type().to_string()})).collect::<Vec<_>>(),
        "rows": forge_query::to_json_rows(&result.batches)?,
        "row_count": result.num_rows(),
        "elapsed_ms": elapsed_ms,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct NewProject {
    id: String,
    name: Option<String>,
    retention_days: Option<u32>,
}

fn require_admin(p: &Principal) -> Result<(), ForgeError> {
    if p.is_admin() {
        Ok(())
    } else {
        Err(ForgeError::Unauthorized("admin key required".into()))
    }
}

async fn create_project(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Json(req): Json<NewProject>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = f.auth.authenticate(&headers).await?;
    require_admin(&principal)?;
    forge_storage::paths::validate_project_id(&req.id)?;
    let key = generate_key();
    f.metastore
        .create_project(ProjectRecord {
            id: req.id.clone(),
            name: req.name.clone().unwrap_or_else(|| req.id.clone()),
            api_key_hash: Some(hash_key(&key)),
            retention_days: req.retention_days,
            created_at_ms: now_ms(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": req.id, "api_key": key, "retention_days": req.retention_days })),
    ))
}

async fn list_projects(State(f): State<Arc<Forge>>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    let principal = f.auth.authenticate(&headers).await?;
    let projects = f.metastore.list_projects().await?;
    let visible: Vec<_> = projects
        .into_iter()
        .filter(|p| principal.can_access(&p.id))
        .collect();
    Ok(Json(json!({ "projects": visible })))
}

async fn rotate_key(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let principal = f.auth.authenticate(&headers).await?;
    require_admin(&principal)?;
    let key = generate_key();
    f.metastore.set_project_key(&id, &hash_key(&key)).await?;
    f.auth.invalidate().await;
    Ok(Json(json!({ "id": id, "api_key": key })))
}

pub fn router(f: Arc<Forge>) -> Router {
    let limit = f.config.ingest.max_request.bytes() as usize;
    Router::new()
        .route("/v1/traces/{trace_id}", get(get_trace))
        .route("/v1/traces/{trace_id}/spans/{span_id}", get(get_span))
        .route("/v1/runs", get(list_runs).post(crate::runs::create_run))
        .route("/v1/runs/batch", post(crate::runs::batch_runs))
        .route("/v1/runs/{id}", patch(crate::runs::update_run))
        .route("/v1/search", post(crate::search::search))
        .route("/v1/sql", post(sql))
        .route("/v1/projects", get(list_projects).post(create_project))
        .route("/v1/projects/{id}/rotate-key", post(rotate_key))
        .layer(DefaultBodyLimit::max(limit))
        .layer(tower_http::decompression::RequestDecompressionLayer::new())
        .layer(axum::middleware::from_fn_with_state(
            f.clone(),
            crate::metrics::track_api,
        ))
        .with_state(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times() {
        assert_eq!(
            parse_time("2025-10-22T16:00:00Z").unwrap(),
            1_761_148_800_000_000_000
        );
        assert_eq!(parse_time("1761148800").unwrap(), 1_761_148_800_000_000_000);
        assert_eq!(
            parse_time("1761148800000").unwrap(),
            1_761_148_800_000_000_000
        );
        assert_eq!(
            parse_time("1761148800000000000").unwrap(),
            1_761_148_800_000_000_000
        );
        assert!(parse_time("yesterday").is_err());
    }
}
