//! Golden fixtures: OTLP/JSON payloads in the shapes OpenLLMetry (LangGraph), OpenInference
//! (LangChain) and the OTel GenAI semantic conventions emit, and the rows they normalize to.
//! Run with `FORGE_BLESS=1` to regenerate the `.expected.json` files after a deliberate change.

use std::path::PathBuf;

use forge_model::dotted;
use forge_model::otlp::{Normalizer, decode_request};
use forge_model::row::SpanRow;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn normalize(name: &str) -> Vec<SpanRow> {
    let body = std::fs::read(fixtures().join(format!("{name}.otlp.json"))).unwrap();
    let req = decode_request(&body, true).unwrap();
    let mut rows = Normalizer::default()
        .normalize(&req, None, "default")
        .unwrap();
    dotted::fill(&mut rows, &|_, _| None);
    rows
}

fn check_golden(name: &str) -> Vec<SpanRow> {
    let rows = normalize(name);
    let path = fixtures().join(format!("{name}.expected.json"));
    let got = serde_json::to_string_pretty(&rows).unwrap();
    if std::env::var_os("FORGE_BLESS").is_some() || !path.exists() {
        std::fs::write(&path, &got).unwrap();
    }
    let want = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        got, want,
        "{name} normalized differently; rerun with FORGE_BLESS=1 if intended"
    );
    rows
}

fn json(s: &Option<String>) -> serde_json::Value {
    serde_json::from_str(s.as_deref().unwrap()).unwrap()
}

#[test]
fn openllmetry_langgraph() {
    let rows = check_golden("openllmetry_langgraph");
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|r| r.project_id == "support"));
    let wf = &rows[0];
    assert_eq!(wf.run_type.as_deref(), Some("chain"));
    assert_eq!(wf.parent_span_id, None);
    assert_eq!(wf.status.as_deref(), Some("ok"));
    assert_eq!(wf.session_id.as_deref(), Some("thread-42"));
    assert_eq!(json(&wf.metadata)["user_id"], "u-7");
    assert_eq!(
        json(&wf.inputs)["inputs"]["messages"][0]["content"],
        "I need a refund for order 1234"
    );
    let llm = &rows[2];
    assert_eq!(llm.run_type.as_deref(), Some("chat_model"));
    assert_eq!(llm.model.as_deref(), Some("gpt-4o-mini"));
    assert_eq!(llm.provider.as_deref(), Some("openai"));
    assert_eq!(
        (llm.prompt_tokens, llm.completion_tokens, llm.total_tokens),
        (Some(412), Some(23), Some(435))
    );
    assert!(llm.cost_usd.unwrap() > 0.0);
    assert_eq!(
        json(&llm.inputs)["messages"][1]["content"],
        "I need a refund for order 1234"
    );
    assert_eq!(
        json(&llm.outputs)["messages"][0]["tool_calls"]["0"]["name"],
        "issue_refund"
    );
    assert_eq!(rows[3].run_type.as_deref(), Some("tool"));
    // dotted order nests: workflow > task > llm
    assert!(
        llm.dotted_order
            .as_ref()
            .unwrap()
            .starts_with(rows[1].dotted_order.as_ref().unwrap())
    );
    assert!(
        rows[1]
            .dotted_order
            .as_ref()
            .unwrap()
            .starts_with(wf.dotted_order.as_ref().unwrap())
    );
}

#[test]
fn openinference_langchain() {
    let rows = check_golden("openinference_langchain");
    assert!(rows.iter().all(|r| r.project_id == "rag"));
    let chain = &rows[0];
    assert_eq!(chain.run_type.as_deref(), Some("chain"));
    assert_eq!(
        chain.tags.as_ref().unwrap(),
        &vec!["prod".to_string(), "rag".to_string()]
    );
    assert_eq!(json(&chain.metadata)["tenant"], "acme");
    assert_eq!(json(&chain.inputs)["query"], "What is the warranty period?");
    assert_eq!(json(&chain.outputs)["output"], "Two years.");
    assert_eq!(chain.session_id.as_deref(), Some("sess-9"));
    assert_eq!(rows[1].run_type.as_deref(), Some("retriever"));
    let llm = &rows[2];
    assert_eq!(llm.run_type.as_deref(), Some("llm"));
    assert_eq!(llm.model.as_deref(), Some("claude-sonnet-4-20250514"));
    assert_eq!(llm.total_tokens, Some(1205));
    assert_eq!(llm.status.as_deref(), Some("error"));
    assert_eq!(llm.error.as_deref(), Some("rate limited, retried"));
    assert!((llm.cost_usd.unwrap() - (1200.0 * 3.0 + 5.0 * 15.0) / 1e6).abs() < 1e-12);
    assert!(json(&llm.events)[0]["name"] == "exception");
}

#[test]
fn genai_semconv() {
    let rows = check_golden("genai_semconv");
    assert!(rows.iter().all(|r| r.project_id == "default"));
    assert_eq!(rows[0].run_type.as_deref(), Some("agent"));
    assert_eq!(rows[0].session_id.as_deref(), Some("conv-123"));
    let chat = &rows[1];
    assert_eq!(chat.run_type.as_deref(), Some("chat_model"));
    assert_eq!(chat.total_tokens, Some(80));
    assert_eq!(
        json(&chat.inputs)["messages"][0]["parts"][0]["content"],
        "Plan 3 days in Lisbon"
    );
    let attrs = json(&chat.attributes);
    assert_eq!(attrs["gen_ai.request.temperature"], 0.2);
    assert_eq!(attrs["resource"]["service.name"], "agent-sdk");
    let tool = &rows[2];
    assert_eq!(tool.run_type.as_deref(), Some("tool"));
    assert_eq!(json(&tool.inputs)["arguments"]["to"], "LIS");
    assert_eq!(json(&tool.outputs)["result"]["flights"], 3);
    // Still-open span: no end time; messages from events.
    let open = &rows[3];
    assert_eq!(open.end_time, None);
    assert_eq!(
        json(&open.inputs)["messages"][0]["content"],
        "Book the cheapest one"
    );
}

#[test]
fn rejects_bad_ids() {
    let body = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abcd","spanId":"00f067aa0ba902b7","name":"x"}]}]}]}"#;
    let err =
        decode_request(body, true).and_then(|r| Normalizer::default().normalize(&r, None, "d"));
    assert!(err.is_err());
}
