//! Map OTLP `ExportTraceServiceRequest`s to `spans` rows.

use base64::Engine as _;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::trace::v1::{Span, span, status};
use serde_json::{Map, Value, json};

use crate::error::{ForgeError, Result};
use crate::genai;
use crate::pricing::Pricing;
use crate::row::SpanRow;

/// Resource attributes that select a project when the request is not authenticated.
pub const PROJECT_ATTRIBUTES: [&str; 4] = [
    "forge.project",
    "openinference.project.name",
    "langsmith.project",
    "project.name",
];

pub fn any_value_to_json(v: &AnyValue) -> Value {
    match &v.value {
        None => Value::Null,
        Some(any_value::Value::StringValue(s)) => Value::String(s.clone()),
        Some(any_value::Value::BoolValue(b)) => Value::Bool(*b),
        Some(any_value::Value::IntValue(i)) => json!(i),
        Some(any_value::Value::DoubleValue(d)) => json!(d),
        Some(any_value::Value::ArrayValue(a)) => {
            Value::Array(a.values.iter().map(any_value_to_json).collect())
        }
        Some(any_value::Value::KvlistValue(kv)) => Value::Object(attrs_to_map(&kv.values)),
        Some(any_value::Value::BytesValue(b)) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        #[allow(unreachable_patterns)]
        Some(_) => Value::Null,
    }
}

pub fn attrs_to_map(attrs: &[KeyValue]) -> Map<String, Value> {
    attrs
        .iter()
        .map(|kv| {
            (
                kv.key.clone(),
                kv.value
                    .as_ref()
                    .map(any_value_to_json)
                    .unwrap_or(Value::Null),
            )
        })
        .collect()
}

fn kind_name(kind: i32) -> &'static str {
    match span::SpanKind::try_from(kind).unwrap_or(span::SpanKind::Unspecified) {
        span::SpanKind::Unspecified => "unspecified",
        span::SpanKind::Internal => "internal",
        span::SpanKind::Server => "server",
        span::SpanKind::Client => "client",
        span::SpanKind::Producer => "producer",
        span::SpanKind::Consumer => "consumer",
    }
}

#[derive(Debug, Clone, Default)]
pub struct Normalizer {
    pub pricing: Pricing,
}

impl Normalizer {
    pub fn new(pricing: Pricing) -> Self {
        Normalizer { pricing }
    }

    /// Normalize a request. `project` forces the project (authenticated requests); otherwise
    /// it comes from resource attributes, falling back to `default_project`.
    pub fn normalize(
        &self,
        req: &ExportTraceServiceRequest,
        project: Option<&str>,
        default_project: &str,
    ) -> Result<Vec<SpanRow>> {
        let mut rows = Vec::new();
        for rs in &req.resource_spans {
            let resource = rs
                .resource
                .as_ref()
                .map(|r| attrs_to_map(&r.attributes))
                .unwrap_or_default();
            let project_id = match project {
                Some(p) => p.to_string(),
                None => PROJECT_ATTRIBUTES
                    .iter()
                    .find_map(|k| resource.get(*k).and_then(|v| v.as_str()))
                    .unwrap_or(default_project)
                    .to_string(),
            };
            for ss in &rs.scope_spans {
                let scope = ss.scope.as_ref();
                for sp in &ss.spans {
                    let mut row = self.span(sp, &project_id)?;
                    // Residual attributes keep resource and scope for reference.
                    let mut attrs: Map<String, Value> = row
                        .attributes
                        .take()
                        .and_then(|a| serde_json::from_str(&a).ok())
                        .unwrap_or_default();
                    if !resource.is_empty() {
                        attrs.insert("resource".into(), Value::Object(resource.clone()));
                    }
                    if let Some(s) = scope {
                        if !s.name.is_empty() {
                            attrs.insert("otel.scope.name".into(), json!(s.name));
                        }
                        if !s.version.is_empty() {
                            attrs.insert("otel.scope.version".into(), json!(s.version));
                        }
                    }
                    row.attributes = Some(Value::Object(attrs).to_string());
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    /// Normalize one span (without resource/scope context).
    pub fn span(&self, sp: &Span, project_id: &str) -> Result<SpanRow> {
        let trace_id: [u8; 16] = sp.trace_id.as_slice().try_into().map_err(|_| {
            ForgeError::invalid(format!(
                "trace_id must be 16 bytes, got {}",
                sp.trace_id.len()
            ))
        })?;
        let span_id: [u8; 8] = sp.span_id.as_slice().try_into().map_err(|_| {
            ForgeError::invalid(format!("span_id must be 8 bytes, got {}", sp.span_id.len()))
        })?;
        if trace_id == [0; 16] || span_id == [0; 8] {
            return Err(ForgeError::invalid("trace_id and span_id must be non-zero"));
        }
        let parent_span_id = match sp.parent_span_id.len() {
            0 => None,
            8 => {
                let p: [u8; 8] = sp.parent_span_id.as_slice().try_into().unwrap();
                (p != [0; 8]).then_some(p)
            }
            n => {
                return Err(ForgeError::invalid(format!(
                    "parent_span_id must be 8 bytes, got {n}"
                )));
            }
        };

        let mut attrs = attrs_to_map(&sp.attributes);
        let events: Vec<(String, Map<String, Value>)> = sp
            .events
            .iter()
            .map(|e| (e.name.clone(), attrs_to_map(&e.attributes)))
            .collect();
        let derived = genai::derive(&mut attrs, &events);

        let mut row = SpanRow::new(project_id, trace_id, span_id);
        row.parent_span_id = parent_span_id;
        row.name = (!sp.name.is_empty()).then(|| sp.name.clone());
        row.start_time = (sp.start_time_unix_nano > 0).then_some(sp.start_time_unix_nano as i64);
        row.end_time = (sp.end_time_unix_nano > 0).then_some(sp.end_time_unix_nano as i64);

        if let Some(st) = &sp.status {
            match status::StatusCode::try_from(st.code).unwrap_or(status::StatusCode::Unset) {
                status::StatusCode::Ok => row.status = Some("ok".into()),
                status::StatusCode::Error => {
                    row.status = Some("error".into());
                    if !st.message.is_empty() {
                        row.error = Some(st.message.clone());
                    }
                }
                status::StatusCode::Unset => {}
            }
        }
        // Exceptions recorded as events.
        if let Some((_, ex)) = events.iter().find(|(n, _)| n == "exception") {
            if row.error.is_none() {
                let ty = ex.get("exception.type").and_then(|v| v.as_str());
                let msg = ex.get("exception.message").and_then(|v| v.as_str());
                row.error = match (ty, msg) {
                    (Some(t), Some(m)) => Some(format!("{t}: {m}")),
                    (None, Some(m)) => Some(m.to_string()),
                    (Some(t), None) => Some(t.to_string()),
                    _ => None,
                };
            }
            if row.status.is_none() {
                row.status = Some("error".into());
            }
        }

        row.run_type = Some(derived.run_type.unwrap_or_else(|| "chain".into()));
        row.model = derived.model;
        row.provider = derived.provider;
        row.prompt_tokens = derived.prompt_tokens;
        row.completion_tokens = derived.completion_tokens;
        row.total_tokens = derived.total_tokens;
        row.cost_usd = self.pricing.cost(
            row.model.as_deref(),
            row.prompt_tokens,
            row.completion_tokens,
        );
        row.session_id = derived.session_id;
        row.tags = derived.tags;
        row.metadata = derived.metadata.map(|m| m.to_string());
        row.inputs = derived.inputs.map(|v| v.to_string());
        row.outputs = derived.outputs.map(|v| v.to_string());

        if !sp.events.is_empty() {
            let ev: Vec<Value> = sp
                .events
                .iter()
                .map(|e| {
                    json!({
                        "name": e.name,
                        "time_unix_nano": e.time_unix_nano,
                        "attributes": attrs_to_map(&e.attributes),
                    })
                })
                .collect();
            row.events = Some(Value::Array(ev).to_string());
        }
        attrs.insert("otel.span.kind".into(), json!(kind_name(sp.kind)));
        if !sp.links.is_empty() {
            let links: Vec<Value> = sp
                .links
                .iter()
                .map(|l| {
                    json!({
                        "trace_id": hex::encode(&l.trace_id),
                        "span_id": hex::encode(&l.span_id),
                        "attributes": attrs_to_map(&l.attributes),
                    })
                })
                .collect();
            attrs.insert("otel.links".into(), Value::Array(links));
        }
        row.attributes = Some(Value::Object(attrs).to_string());
        Ok(row)
    }
}

/// Decode an OTLP/HTTP body, protobuf or JSON.
pub fn decode_request(body: &[u8], json_body: bool) -> Result<ExportTraceServiceRequest> {
    if json_body {
        serde_json::from_slice(body).map_err(|e| ForgeError::invalid(format!("OTLP JSON: {e}")))
    } else {
        use prost::Message;
        ExportTraceServiceRequest::decode(body)
            .map_err(|e| ForgeError::invalid(format!("OTLP protobuf: {e}")))
    }
}
