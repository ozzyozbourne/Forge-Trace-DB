//! Native JSON runs API in the shape of LangSmith's: `POST /v1/runs`, `PATCH /v1/runs/{id}`,
//! `POST /v1/runs/batch`.
//!
//! Run ids are UUIDs. `trace_id` is the UUID's 16 bytes (a root run's trace id is its own id);
//! span ids are the first 8 bytes of SHA-256 of the run UUID, since OTel span ids are 8 bytes.
//! The original run id is kept in `attributes["langsmith.run_id"]`.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use forge_model::row::{SpanId, SpanRow, TraceId, parse_span_id, parse_trace_id};
use forge_model::{ForgeError, Result};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::Forge;
use crate::api::parse_time;
use crate::auth::Principal;
use crate::error::ApiResult;

/// Accept a UUID (with or without dashes) or a 16-hex OTel span id.
pub fn span_id_for(run_id: &str) -> Result<SpanId> {
    if let Some(s) = parse_span_id(run_id) {
        return Ok(s);
    }
    let uuid = parse_trace_id(run_id)
        .ok_or_else(|| ForgeError::invalid(format!("invalid run id {run_id:?}")))?;
    let h = Sha256::digest(uuid);
    Ok(h[..8].try_into().unwrap())
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct RunIn {
    pub id: Option<String>,
    pub trace_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub name: Option<String>,
    pub run_type: Option<String>,
    pub start_time: Option<Value>,
    pub end_time: Option<Value>,
    pub inputs: Option<Value>,
    pub outputs: Option<Value>,
    pub error: Option<String>,
    pub status: Option<String>,
    pub tags: Option<Vec<String>>,
    pub extra: Option<Value>,
    pub metadata: Option<Value>,
    pub events: Option<Value>,
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub dotted_order: Option<String>,
    pub project: Option<String>,
    pub session_name: Option<String>,
    pub deleted: Option<bool>,
}

fn time_of(v: &Option<Value>) -> Result<Option<i64>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => {
            // LangSmith sends naive ISO times in UTC.
            let s = if s.ends_with('Z') || s.contains('+') || s.chars().all(|c| c.is_ascii_digit())
            {
                s.clone()
            } else {
                format!("{s}Z")
            };
            parse_time(&s).map(Some)
        }
        Some(Value::Number(n)) => parse_time(&n.to_string()).map(Some),
        Some(other) => Err(ForgeError::invalid(format!("invalid time {other}"))),
    }
}

impl RunIn {
    fn to_row(&self, project: &str, f: &Forge, run_id: &str) -> Result<SpanRow> {
        let span_id = span_id_for(run_id)?;
        let trace_id: TraceId = match &self.trace_id {
            Some(t) => parse_trace_id(t).ok_or_else(|| ForgeError::invalid("invalid trace_id"))?,
            None => [0; 16],
        };
        let mut r = SpanRow::new(project, trace_id, span_id);
        r.parent_span_id = self.parent_run_id.as_deref().map(span_id_for).transpose()?;
        r.name = self.name.clone();
        r.run_type = self.run_type.clone();
        r.start_time = time_of(&self.start_time)?;
        r.end_time = time_of(&self.end_time)?;
        r.error = self.error.clone();
        r.status = match (&self.status, &self.error, r.end_time) {
            (Some(s), _, _) => Some(match s.as_str() {
                "success" => "ok".to_string(),
                other => other.to_string(),
            }),
            (None, Some(_), _) => Some("error".into()),
            (None, None, Some(_)) => Some("ok".into()),
            _ => None,
        };
        r.tags = self.tags.clone();
        // LangSmith puts metadata under extra.metadata.
        let mut meta: Map<String, Value> = Map::new();
        if let Some(Value::Object(m)) = self.extra.as_ref().and_then(|e| e.get("metadata")) {
            meta.extend(m.clone());
        }
        if let Some(Value::Object(m)) = &self.metadata {
            meta.extend(m.clone());
        }
        r.session_id = self
            .session_id
            .clone()
            .or_else(|| self.thread_id.clone())
            .or_else(|| {
                ["session_id", "thread_id", "conversation_id"]
                    .iter()
                    .find_map(|k| meta.get(*k).and_then(|v| v.as_str()).map(String::from))
            });
        if !meta.is_empty() {
            r.metadata = Some(Value::Object(meta.clone()).to_string());
        }
        r.model = self.model.clone().or_else(|| {
            meta.get("ls_model_name")
                .and_then(|v| v.as_str())
                .map(String::from)
        });
        r.provider = self.provider.clone().or_else(|| {
            meta.get("ls_provider")
                .and_then(|v| v.as_str())
                .map(String::from)
        });
        r.prompt_tokens = self.prompt_tokens;
        r.completion_tokens = self.completion_tokens;
        r.total_tokens = self
            .total_tokens
            .or(match (r.prompt_tokens, r.completion_tokens) {
                (None, None) => None,
                (p, c) => Some(p.unwrap_or(0) + c.unwrap_or(0)),
            });
        r.cost_usd =
            f.normalizer
                .pricing
                .cost(r.model.as_deref(), r.prompt_tokens, r.completion_tokens);
        r.inputs = self.inputs.as_ref().map(|v| v.to_string());
        r.outputs = self.outputs.as_ref().map(|v| v.to_string());
        r.events = self.events.as_ref().map(|v| v.to_string());
        r.dotted_order = self.dotted_order.clone();
        r.deleted = self.deleted.unwrap_or(false);
        let mut attrs = Map::new();
        attrs.insert("langsmith.run_id".into(), json!(run_id));
        if let Some(Value::Object(extra)) = &self.extra {
            for (k, v) in extra {
                if k != "metadata" {
                    attrs.insert(format!("langsmith.extra.{k}"), v.clone());
                }
            }
        }
        r.attributes = Some(Value::Object(attrs).to_string());
        Ok(r)
    }
}

/// Fill identity fields of an update from the run's existing state, so that the stable
/// columns (`trace_id`, `parent_span_id`, `start_time`) are set on every version.
async fn fill_identity(f: &Forge, row: &mut SpanRow, trace_known: bool) -> Result<()> {
    let snapshot = f.ingester.snapshot();
    let existing = if trace_known {
        match snapshot.find_span(&row.project_id, &row.trace_id, &row.span_id)? {
            Some(r) => Some(r),
            None => {
                let b = f
                    .query
                    .reader
                    .trace_rows(&row.project_id, &row.trace_id, None)
                    .await?;
                forge_model::row::batch_to_rows(&b)
                    .into_iter()
                    .find(|r| r.span_id == row.span_id)
            }
        }
    } else {
        snapshot.find_span_any_trace(&row.project_id, &row.span_id)?
    };
    match existing {
        Some(e) => {
            row.trace_id = e.trace_id;
            if row.parent_span_id.is_none() {
                row.parent_span_id = e.parent_span_id;
            }
            if row.start_time.is_none() {
                row.start_time = e.start_time;
            }
            if row.dotted_order.is_none() {
                row.dotted_order = e.dotted_order;
            }
            // Cost is computed per version at ingest; a patch that reports usage needs the
            // model from the run it updates.
            if row.cost_usd.is_none()
                && (row.prompt_tokens.is_some() || row.completion_tokens.is_some())
            {
                let model = row.model.clone().or(e.model);
                row.cost_usd = f.normalizer.pricing.cost(
                    model.as_deref(),
                    row.prompt_tokens,
                    row.completion_tokens,
                );
            }
            Ok(())
        }
        None if trace_known => Ok(()),
        None => Err(ForgeError::NotFound(
            "run not found among recent runs; include trace_id in the update".into(),
        )),
    }
}

async fn post_rows(f: &Forge, principal: &Principal, runs: Vec<RunIn>) -> Result<Vec<SpanRow>> {
    let mut rows = Vec::with_capacity(runs.len());
    for run in runs {
        let project = f.resolve_project(
            principal,
            run.project.as_deref().or(run.session_name.as_deref()),
        )?;
        let id = run
            .id
            .clone()
            .ok_or_else(|| ForgeError::invalid("run id is required"))?;
        let mut row = run.to_row(&project, f, &id)?;
        if run.trace_id.is_none() {
            if run.parent_run_id.is_some() {
                return Err(ForgeError::invalid("trace_id is required for child runs"));
            }
            // A root run's trace id is its own id.
            row.trace_id = parse_trace_id(&id).ok_or_else(|| {
                ForgeError::invalid("root run ids must be UUIDs when trace_id is omitted")
            })?;
        }
        rows.push(row);
    }
    Ok(rows)
}

async fn patch_row(f: &Forge, principal: &Principal, id: &str, run: RunIn) -> Result<SpanRow> {
    let project = f.resolve_project(
        principal,
        run.project.as_deref().or(run.session_name.as_deref()),
    )?;
    let trace_known = run.trace_id.is_some();
    let mut row = run.to_row(&project, f, id)?;
    fill_identity(f, &mut row, trace_known).await?;
    Ok(row)
}

pub async fn create_run(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Json(run): Json<RunIn>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = f.auth.authenticate(&headers).await?;
    let rows = post_rows(&f, &principal, vec![run]).await?;
    let n = f.ingester.ingest(rows).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({ "accepted": n }))))
}

pub async fn update_run(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(run): Json<RunIn>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = f.auth.authenticate(&headers).await?;
    let row = patch_row(&f, &principal, &id, run).await?;
    let n = f.ingester.ingest(vec![row]).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({ "accepted": n }))))
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct Batch {
    pub post: Vec<RunIn>,
    pub patch: Vec<RunIn>,
}

pub async fn batch_runs(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    Json(batch): Json<Batch>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = f.auth.authenticate(&headers).await?;
    let rows = post_rows(&f, &principal, batch.post).await?;
    // Posts are ingested first so the patches' identity lookups can see them.
    let posted = f.ingester.ingest(rows).await?;
    let mut patched = Vec::new();
    for run in batch.patch {
        let id = run
            .id
            .clone()
            .ok_or_else(|| ForgeError::invalid("run id is required"))?;
        patched.push(patch_row(&f, &principal, &id, run).await?);
    }
    let patched = f.ingester.ingest(patched).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "accepted": posted + patched, "posted": posted, "patched": patched })),
    ))
}
