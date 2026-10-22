//! Derive `run_type`, model, provider, tokens, inputs/outputs, session, tags and metadata from
//! span attributes. OTel GenAI semantic conventions win, then OpenInference, then OpenLLMetry
//! (Traceloop) and LangSmith's OTel attribute names.
//!
//! Attributes consumed into first-class columns are removed from the residual `attributes`
//! JSON so payloads are not stored twice.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

/// What the attribute mapping produced for one span.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Derived {
    pub run_type: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub session_id: Option<String>,
    pub tags: Option<Vec<String>>,
    pub metadata: Option<Value>,
    pub inputs: Option<Value>,
    pub outputs: Option<Value>,
}

pub const RUN_TYPES: [&str; 8] = [
    "llm",
    "chat_model",
    "tool",
    "chain",
    "agent",
    "retriever",
    "embedding",
    "other",
];

fn take_str(attrs: &mut Map<String, Value>, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(v) = attrs.get(*k) {
            let s = match v {
                Value::String(s) if !s.is_empty() => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            };
            if s.is_some() {
                attrs.remove(*k);
                return s;
            }
        }
    }
    None
}

fn peek_str<'a>(attrs: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|k| {
        attrs
            .get(*k)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    })
}

fn take_int(attrs: &mut Map<String, Value>, keys: &[&str]) -> Option<i64> {
    for k in keys {
        if let Some(v) = attrs.get(*k) {
            let n = match v {
                Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
                Value::String(s) => s.parse().ok(),
                _ => None,
            };
            if n.is_some() {
                attrs.remove(*k);
                return n;
            }
        }
    }
    None
}

/// Parse a string that may hold JSON; fall back to the string itself.
fn json_or_string(v: Value) -> Value {
    match v {
        Value::String(s) => {
            let t = s.trim_start();
            if t.starts_with('{') || t.starts_with('[') {
                serde_json::from_str(&s).unwrap_or(Value::String(s))
            } else {
                Value::String(s)
            }
        }
        other => other,
    }
}

/// Collect `prefix.{i}.{field...}` attributes into an ordered list of objects.
/// `inner` strips an extra segment, e.g. OpenInference's `message.` in
/// `llm.input_messages.0.message.role`.
fn take_indexed(attrs: &mut Map<String, Value>, prefix: &str, inner: &str) -> Option<Vec<Value>> {
    let mut items: BTreeMap<usize, Map<String, Value>> = BTreeMap::new();
    let keys: Vec<String> = attrs
        .keys()
        .filter(|k| k.starts_with(prefix))
        .cloned()
        .collect();
    for key in keys {
        let rest = &key[prefix.len()..];
        let Some((idx, field)) = rest.split_once('.') else {
            continue;
        };
        let Ok(idx) = idx.parse::<usize>() else {
            continue;
        };
        let field = field.strip_prefix(inner).unwrap_or(field);
        let v = attrs.remove(&key).unwrap();
        insert_path(items.entry(idx).or_default(), field, json_or_string(v));
    }
    if items.is_empty() {
        None
    } else {
        Some(items.into_values().map(Value::Object).collect())
    }
}

/// Insert `a.b.c = v` into nested objects.
fn insert_path(obj: &mut Map<String, Value>, path: &str, v: Value) {
    match path.split_once('.') {
        None => {
            obj.insert(path.to_string(), v);
        }
        Some((head, tail)) => {
            let child = obj
                .entry(head.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(m) = child {
                insert_path(m, tail, v);
            } else {
                obj.insert(path.to_string(), v);
            }
        }
    }
}

fn take_prefixed(attrs: &mut Map<String, Value>, prefix: &str) -> Map<String, Value> {
    let keys: Vec<String> = attrs
        .keys()
        .filter(|k| k.starts_with(prefix))
        .cloned()
        .collect();
    let mut out = Map::new();
    for k in keys {
        let v = attrs.remove(&k).unwrap();
        out.insert(k[prefix.len()..].to_string(), json_or_string(v));
    }
    out
}

fn map_run_type(attrs: &Map<String, Value>) -> Option<String> {
    let rt = |s: &str| Some(s.to_string());
    if let Some(op) = peek_str(attrs, &["gen_ai.operation.name"]) {
        let m = match op {
            "chat" | "generate_content" => rt("chat_model"),
            "text_completion" | "completion" => rt("llm"),
            "embeddings" | "embedding" => rt("embedding"),
            "execute_tool" => rt("tool"),
            "invoke_agent" | "create_agent" => rt("agent"),
            "retrieve" | "retrieval" => rt("retriever"),
            _ => None,
        };
        if m.is_some() {
            return m;
        }
    }
    if let Some(kind) = peek_str(attrs, &["openinference.span.kind"]) {
        let m = match kind.to_ascii_uppercase().as_str() {
            "LLM" => rt("llm"),
            "CHAIN" | "GUARDRAIL" | "EVALUATOR" | "PROMPT" => rt("chain"),
            "TOOL" => rt("tool"),
            "AGENT" => rt("agent"),
            "RETRIEVER" | "RERANKER" => rt("retriever"),
            "EMBEDDING" => rt("embedding"),
            _ => None,
        };
        if m.is_some() {
            return m;
        }
    }
    if let Some(kind) = peek_str(attrs, &["llm.request.type"]) {
        let m = match kind {
            "chat" => rt("chat_model"),
            "completion" => rt("llm"),
            "embedding" => rt("embedding"),
            "rerank" => rt("retriever"),
            _ => None,
        };
        if m.is_some() {
            return m;
        }
    }
    if let Some(kind) = peek_str(attrs, &["traceloop.span.kind"]) {
        let m = match kind {
            "workflow" | "task" => rt("chain"),
            "agent" => rt("agent"),
            "tool" => rt("tool"),
            _ => None,
        };
        if m.is_some() {
            return m;
        }
    }
    if let Some(kind) = peek_str(attrs, &["langsmith.span.kind"]) {
        let k = kind.to_ascii_lowercase();
        if RUN_TYPES.contains(&k.as_str()) {
            return Some(k);
        }
        return rt("chain");
    }
    if peek_str(
        attrs,
        &[
            "gen_ai.request.model",
            "gen_ai.response.model",
            "llm.model_name",
        ],
    )
    .is_some()
    {
        return rt("llm");
    }
    None
}

/// Map attributes in place. `events` are the span's events as `(name, attributes)`, used for
/// GenAI message events.
pub fn derive(attrs: &mut Map<String, Value>, events: &[(String, Map<String, Value>)]) -> Derived {
    let mut d = Derived {
        run_type: map_run_type(attrs),
        ..Default::default()
    };

    d.model = take_str(
        attrs,
        &[
            "gen_ai.request.model",
            "gen_ai.response.model",
            "llm.model_name",
            "llm.request.model",
            "llm.response.model",
        ],
    );
    // Response model is redundant once request model is taken.
    attrs.remove("gen_ai.response.model");
    d.provider = take_str(
        attrs,
        &[
            "gen_ai.provider.name",
            "gen_ai.system",
            "llm.provider",
            "llm.system",
        ],
    );
    d.prompt_tokens = take_int(
        attrs,
        &[
            "gen_ai.usage.input_tokens",
            "gen_ai.usage.prompt_tokens",
            "llm.token_count.prompt",
            "llm.usage.prompt_tokens",
        ],
    );
    d.completion_tokens = take_int(
        attrs,
        &[
            "gen_ai.usage.output_tokens",
            "gen_ai.usage.completion_tokens",
            "llm.token_count.completion",
            "llm.usage.completion_tokens",
        ],
    );
    d.total_tokens = take_int(
        attrs,
        &[
            "gen_ai.usage.total_tokens",
            "llm.usage.total_tokens",
            "llm.token_count.total",
        ],
    )
    .or(match (d.prompt_tokens, d.completion_tokens) {
        (None, None) => None,
        (p, c) => Some(p.unwrap_or(0) + c.unwrap_or(0)),
    });

    d.session_id = take_str(
        attrs,
        &[
            "session.id",
            "gen_ai.conversation.id",
            "langsmith.metadata.session_id",
            "langsmith.metadata.thread_id",
            "langsmith.metadata.conversation_id",
            "traceloop.association.properties.session_id",
            "traceloop.association.properties.thread_id",
            "thread_id",
        ],
    );

    // Tags: OpenInference `tag.tags` is a list; LangSmith uses a comma string.
    let mut tags: Vec<String> = Vec::new();
    for key in ["tag.tags", "langsmith.span.tags"] {
        if let Some(v) = attrs.remove(key) {
            match json_or_string(v) {
                Value::Array(a) => tags.extend(a.into_iter().filter_map(|x| match x {
                    Value::String(s) => Some(s),
                    _ => None,
                })),
                Value::String(s) => tags.extend(
                    s.split(',')
                        .map(|t| t.trim().to_string())
                        .filter(|t| !t.is_empty()),
                ),
                _ => {}
            }
        }
    }
    if !tags.is_empty() {
        d.tags = Some(tags);
    }

    // Metadata.
    let mut meta = Map::new();
    if let Some(v) = attrs.remove("metadata") {
        match json_or_string(v) {
            Value::Object(m) => meta.extend(m),
            other => {
                meta.insert("metadata".into(), other);
            }
        }
    }
    meta.extend(take_prefixed(attrs, "langsmith.metadata."));
    meta.extend(take_prefixed(attrs, "traceloop.association.properties."));
    if let Some(u) = take_str(attrs, &["user.id", "enduser.id"]) {
        meta.insert("user_id".into(), Value::String(u));
    }
    if !meta.is_empty() {
        d.metadata = Some(Value::Object(meta));
    }

    d.inputs = derive_inputs(attrs, events);
    d.outputs = derive_outputs(attrs, events);
    d
}

fn derive_inputs(
    attrs: &mut Map<String, Value>,
    events: &[(String, Map<String, Value>)],
) -> Option<Value> {
    let mut out = Map::new();
    if let Some(v) = attrs.remove("gen_ai.input.messages") {
        out.insert("messages".into(), json_or_string(v));
    }
    if let Some(v) = attrs.remove("gen_ai.system_instructions") {
        out.insert("system_instructions".into(), json_or_string(v));
    }
    if let Some(v) = attrs.remove("gen_ai.tool.call.arguments") {
        out.insert("arguments".into(), json_or_string(v));
    }
    if !out.contains_key("messages") {
        if let Some(m) = take_indexed(attrs, "llm.input_messages.", "message.") {
            out.insert("messages".into(), Value::Array(m));
        } else if let Some(m) = take_indexed(attrs, "gen_ai.prompt.", "") {
            out.insert("messages".into(), Value::Array(m));
        }
    }
    if !out.contains_key("messages") {
        let msgs: Vec<Value> = events
            .iter()
            .filter_map(|(name, a)| {
                let role = match name.as_str() {
                    "gen_ai.system.message" => "system",
                    "gen_ai.user.message" => "user",
                    "gen_ai.assistant.message" => "assistant",
                    "gen_ai.tool.message" => "tool",
                    _ => return None,
                };
                let mut m = Map::new();
                m.insert("role".into(), json!(role));
                for (k, v) in a {
                    if k != "gen_ai.system" {
                        m.insert(k.clone(), json_or_string(v.clone()));
                    }
                }
                Some(Value::Object(m))
            })
            .collect();
        if !msgs.is_empty() {
            out.insert("messages".into(), Value::Array(msgs));
        }
    }
    let value_input = attrs
        .remove("input.value")
        .or_else(|| attrs.remove("traceloop.entity.input"))
        .or_else(|| attrs.remove("langsmith.inputs"));
    attrs.remove("input.mime_type");
    if let Some(v) = value_input {
        match json_or_string(v) {
            Value::Object(m) if out.is_empty() => out = m,
            other => {
                out.insert("input".into(), other);
            }
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(Value::Object(out))
    }
}

fn derive_outputs(
    attrs: &mut Map<String, Value>,
    events: &[(String, Map<String, Value>)],
) -> Option<Value> {
    let mut out = Map::new();
    if let Some(v) = attrs.remove("gen_ai.output.messages") {
        out.insert("messages".into(), json_or_string(v));
    }
    if let Some(v) = attrs.remove("gen_ai.tool.call.result") {
        out.insert("result".into(), json_or_string(v));
    }
    if !out.contains_key("messages") {
        if let Some(m) = take_indexed(attrs, "llm.output_messages.", "message.") {
            out.insert("messages".into(), Value::Array(m));
        } else if let Some(m) = take_indexed(attrs, "gen_ai.completion.", "") {
            out.insert("messages".into(), Value::Array(m));
        }
    }
    if !out.contains_key("messages") {
        let choices: Vec<Value> = events
            .iter()
            .filter(|(n, _)| n == "gen_ai.choice")
            .map(|(_, a)| {
                Value::Object(
                    a.iter()
                        .filter(|(k, _)| k.as_str() != "gen_ai.system")
                        .map(|(k, v)| (k.clone(), json_or_string(v.clone())))
                        .collect(),
                )
            })
            .collect();
        if !choices.is_empty() {
            out.insert("messages".into(), Value::Array(choices));
        }
    }
    let value_output = attrs
        .remove("output.value")
        .or_else(|| attrs.remove("traceloop.entity.output"))
        .or_else(|| attrs.remove("langsmith.outputs"));
    attrs.remove("output.mime_type");
    if let Some(v) = value_output {
        match json_or_string(v) {
            Value::Object(m) if out.is_empty() => out = m,
            other => {
                out.insert("output".into(), other);
            }
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(Value::Object(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn genai_semconv() {
        let mut a = attrs(json!({
            "gen_ai.operation.name": "chat",
            "gen_ai.provider.name": "openai",
            "gen_ai.request.model": "gpt-4o",
            "gen_ai.usage.input_tokens": 100,
            "gen_ai.usage.output_tokens": 20,
            "gen_ai.input.messages": "[{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"content\":\"refund please\"}]}]",
            "gen_ai.conversation.id": "conv-1",
            "http.method": "POST"
        }));
        let d = derive(&mut a, &[]);
        assert_eq!(d.run_type.as_deref(), Some("chat_model"));
        assert_eq!(d.model.as_deref(), Some("gpt-4o"));
        assert_eq!(d.provider.as_deref(), Some("openai"));
        assert_eq!(d.total_tokens, Some(120));
        assert_eq!(d.session_id.as_deref(), Some("conv-1"));
        assert_eq!(d.inputs.unwrap()["messages"][0]["role"], "user");
        assert!(a.contains_key("http.method"));
        assert!(!a.contains_key("gen_ai.input.messages"));
    }

    #[test]
    fn openinference() {
        let mut a = attrs(json!({
            "openinference.span.kind": "LLM",
            "llm.model_name": "claude-sonnet-4",
            "llm.provider": "anthropic",
            "llm.token_count.prompt": 5,
            "llm.token_count.completion": 7,
            "llm.token_count.total": 12,
            "llm.input_messages.0.message.role": "system",
            "llm.input_messages.0.message.content": "be nice",
            "llm.input_messages.1.message.role": "user",
            "llm.input_messages.1.message.content": "hi",
            "llm.output_messages.0.message.role": "assistant",
            "llm.output_messages.0.message.content": "hello",
            "tag.tags": ["a", "b"],
            "metadata": "{\"env\":\"prod\"}",
            "session.id": "s1"
        }));
        let d = derive(&mut a, &[]);
        assert_eq!(d.run_type.as_deref(), Some("llm"));
        assert_eq!(d.total_tokens, Some(12));
        let inputs = d.inputs.unwrap();
        assert_eq!(inputs["messages"][1]["content"], "hi");
        assert_eq!(d.outputs.unwrap()["messages"][0]["content"], "hello");
        assert_eq!(d.tags.unwrap(), vec!["a", "b"]);
        assert_eq!(d.metadata.unwrap()["env"], "prod");
        assert_eq!(
            a.keys().collect::<Vec<_>>(),
            vec!["openinference.span.kind"]
        );
    }

    #[test]
    fn openllmetry() {
        let mut a = attrs(json!({
            "llm.request.type": "chat",
            "gen_ai.system": "OpenAI",
            "gen_ai.request.model": "gpt-4o-mini",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.prompt.0.content": "What is 2+2?",
            "gen_ai.completion.0.role": "assistant",
            "gen_ai.completion.0.content": "4",
            "gen_ai.usage.prompt_tokens": 9,
            "gen_ai.usage.completion_tokens": 1,
            "llm.usage.total_tokens": 10,
            "traceloop.association.properties.user_id": "u1"
        }));
        let d = derive(&mut a, &[]);
        assert_eq!(d.run_type.as_deref(), Some("chat_model"));
        assert_eq!(d.provider.as_deref(), Some("OpenAI"));
        assert_eq!(d.inputs.unwrap()["messages"][0]["content"], "What is 2+2?");
        assert_eq!(d.outputs.unwrap()["messages"][0]["content"], "4");
        assert_eq!(d.metadata.unwrap()["user_id"], "u1");

        let mut wf = attrs(json!({
            "traceloop.span.kind": "workflow",
            "traceloop.entity.input": "{\"inputs\":{\"question\":\"refund\"}}",
            "traceloop.entity.output": "{\"outputs\":\"done\"}"
        }));
        let d = derive(&mut wf, &[]);
        assert_eq!(d.run_type.as_deref(), Some("chain"));
        assert_eq!(d.inputs.unwrap()["inputs"]["question"], "refund");
    }

    #[test]
    fn message_events() {
        let mut a = attrs(json!({"gen_ai.operation.name": "chat"}));
        let events = vec![
            (
                "gen_ai.user.message".to_string(),
                attrs(json!({"content": "hello"})),
            ),
            (
                "gen_ai.choice".to_string(),
                attrs(json!({"index": 0, "message": "{\"content\":\"hey\"}"})),
            ),
        ];
        let d = derive(&mut a, &events);
        assert_eq!(d.inputs.unwrap()["messages"][0]["role"], "user");
        assert_eq!(
            d.outputs.unwrap()["messages"][0]["message"]["content"],
            "hey"
        );
    }
}
