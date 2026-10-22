//! End to end through the network: OTLP gRPC and HTTP in, JSON API out.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use forge_model::config::Config;
use forge_model::opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use forge_model::opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use forge_model::opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use forge_model::opentelemetry_proto::tonic::resource::v1::Resource;
use forge_model::opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status, status,
};
use forge_server::Forge;
use serde_json::{Value, json};

fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Running {
    _dir: tempfile::TempDir,
    forge: Arc<Forge>,
    api: String,
    http: String,
    grpc: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
}

async fn start(auth: bool) -> Running {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default().with_data_dir(dir.path());
    let (g, h, a, o) = (port(), port(), port(), port());
    config.server.otlp_grpc_addr = format!("127.0.0.1:{g}");
    config.server.otlp_http_addr = format!("127.0.0.1:{h}");
    config.server.api_addr = format!("127.0.0.1:{a}");
    config.server.metrics_addr = format!("127.0.0.1:{o}");
    config.compaction.enabled = false;
    if auth {
        config.auth.enabled = true;
        config.auth.admin_key = Some("admin-secret".into());
    }
    let forge = Forge::open(config).await.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let f = forge.clone();
    let handle = tokio::spawn(async move {
        forge_server::serve(f, async {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", a))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Running {
        _dir: dir,
        forge,
        api: format!("http://127.0.0.1:{a}"),
        http: format!("http://127.0.0.1:{h}"),
        grpc: format!("http://127.0.0.1:{g}"),
        stop: Some(tx),
        handle,
    }
}

impl Running {
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.handle.await.unwrap();
    }
}

fn kv(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.into())),
        }),
        ..Default::default()
    }
}

fn otlp(trace: [u8; 16], project: &str) -> ExportTraceServiceRequest {
    let t0 = 1_761_148_800_000_000_000u64;
    let root = Span {
        trace_id: trace.to_vec(),
        span_id: vec![1, 0, 0, 0, 0, 0, 0, 1],
        name: "agent".into(),
        start_time_unix_nano: t0,
        end_time_unix_nano: t0 + 2_000_000_000,
        attributes: vec![
            kv("gen_ai.operation.name", "invoke_agent"),
            kv("input.value", "{\"question\":\"where is my refund\"}"),
        ],
        status: Some(Status {
            code: status::StatusCode::Ok as i32,
            message: String::new(),
        }),
        ..Default::default()
    };
    let child = Span {
        trace_id: trace.to_vec(),
        span_id: vec![2, 0, 0, 0, 0, 0, 0, 2],
        parent_span_id: vec![1, 0, 0, 0, 0, 0, 0, 1],
        name: "chat gpt-4o".into(),
        start_time_unix_nano: t0 + 1_000_000,
        end_time_unix_nano: t0 + 1_000_000_000,
        attributes: vec![
            kv("gen_ai.operation.name", "chat"),
            kv("gen_ai.request.model", "gpt-4o"),
        ],
        ..Default::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("forge.project", project)],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![root, child],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[tokio::test]
async fn otlp_in_json_out() {
    let s = start(false).await;
    let http = reqwest::Client::new();

    // gRPC.
    let mut client = TraceServiceClient::connect(s.grpc.clone()).await.unwrap();
    client.export(otlp([7; 16], "demo")).await.unwrap();
    // HTTP protobuf.
    use prost::Message;
    let r = http
        .post(format!("{}/v1/traces", s.http))
        .header("content-type", "application/x-protobuf")
        .body(otlp([8; 16], "demo").encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // Visible immediately, from the memtable.
    let t: Value = http
        .get(format!(
            "{}/v1/traces/{}?project=demo",
            s.api,
            hex::encode([7u8; 16])
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(t["span_count"], 2, "{t}");
    assert_eq!(t["spans"][0]["run_type"], "agent");
    assert_eq!(t["spans"][0]["children"][0]["model"], "gpt-4o");
    assert_eq!(t["spans"][0]["inputs"]["question"], "where is my refund");

    s.forge.ingester.flush_now().await.unwrap();

    let runs: Value = http
        .get(format!("{}/v1/runs?project=demo&limit=1", s.api))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(runs["runs"].as_array().unwrap().len(), 1);
    let cursor = runs["next_cursor"].as_str().unwrap().to_string();
    let page2: Value = http
        .get(format!(
            "{}/v1/runs?project=demo&limit=1&cursor={cursor}",
            s.api
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page2["runs"].as_array().unwrap().len(), 1);
    assert!(page2["next_cursor"].is_null());
    assert_ne!(runs["runs"][0]["trace_id"], page2["runs"][0]["trace_id"]);

    let found: Value = http
        .get(format!("{}/v1/runs?project=demo&q=refund", s.api))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(found["runs"].as_array().unwrap().len(), 2, "{found}");
    let found: Value = http
        .post(format!("{}/v1/search", s.api))
        .json(&json!({"project": "demo", "query": "refund", "column": "inputs", "path": "$.question"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(found["runs"].as_array().unwrap().len(), 2, "{found}");

    let q: Value = http
        .post(format!("{}/v1/sql", s.api))
        .json(&json!({"query": "SELECT run_type, count(*) AS n FROM spans GROUP BY run_type ORDER BY run_type"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        q["rows"],
        json!([{"run_type": "agent", "n": 2}, {"run_type": "chat_model", "n": 2}]),
        "{q}"
    );
    let bad = http
        .post(format!("{}/v1/sql", s.api))
        .json(&json!({"query": "DROP TABLE spans"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let missing = http
        .get(format!(
            "{}/v1/traces/{}?project=demo",
            s.api,
            hex::encode([9u8; 16])
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    s.stop().await;
}

#[tokio::test]
async fn runs_api_upserts() {
    let s = start(false).await;
    let http = reqwest::Client::new();
    let root = "0199f5c4-6c4e-7d1a-9a6b-1f2e3d4c5b6a";
    let child = "0199f5c4-6c4e-7d1a-9a6b-1f2e3d4c5b6b";
    let r = http
        .post(format!("{}/v1/runs/batch", s.api))
        .json(&json!({"post": [
            {"id": root, "name": "Agent", "run_type": "chain", "start_time": "2025-10-22T16:00:00.000000", "inputs": {"q": "hello"}, "project": "ls"},
            {"id": child, "trace_id": root, "parent_run_id": root, "name": "ChatOpenAI", "run_type": "llm", "start_time": "2025-10-22T16:00:00.100000", "project": "ls",
             "extra": {"metadata": {"ls_model_name": "gpt-4o-mini", "thread_id": "th-1"}}}
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202, "{}", r.text().await.unwrap());
    // Flush so the patch's identity lookup must go to segments.
    s.forge.ingester.flush_now().await.unwrap();
    let r = http
        .patch(format!("{}/v1/runs/{child}", s.api))
        .json(&json!({"trace_id": root, "end_time": "2025-10-22T16:00:01.100000", "outputs": {"text": "hi"}, "prompt_tokens": 10, "completion_tokens": 5, "project": "ls"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202, "{}", r.text().await.unwrap());
    // Patch without trace id resolves from recent memtable state.
    let r = http
        .patch(format!("{}/v1/runs/{root}", s.api))
        .json(&json!({"end_time": "2025-10-22T16:00:02", "outputs": {"answer": "hi"}, "project": "ls"}))
        .send()
        .await;
    // The root's start version was flushed, so without trace_id this is a 404 by design.
    assert_eq!(r.unwrap().status(), 404);
    let r = http
        .patch(format!("{}/v1/runs/{root}", s.api))
        .json(&json!({"trace_id": root, "end_time": "2025-10-22T16:00:02", "outputs": {"answer": "hi"}, "project": "ls"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);

    let t: Value = http
        .get(format!("{}/v1/traces/{}?project=ls", s.api, root))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(t["span_count"], 2, "{t}");
    let root_run = &t["spans"][0];
    assert_eq!(root_run["status"], "ok");
    assert_eq!(root_run["latency_ms"], 2000.0);
    let c = &root_run["children"][0];
    assert_eq!(c["model"], "gpt-4o-mini");
    assert_eq!(c["session_id"], "th-1");
    assert_eq!(c["total_tokens"], 15);
    assert!(c["cost_usd"].as_f64().unwrap() > 0.0);
    assert_eq!(c["outputs"]["text"], "hi");
    assert_eq!(c["inputs"], Value::Null);
    // The patch carried identity fields, so the child is not mistaken for a root.
    let runs: Value = http
        .get(format!("{}/v1/runs?project=ls", s.api))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(runs["runs"].as_array().unwrap().len(), 1, "{runs}");
    s.stop().await;
}

#[tokio::test]
async fn api_keys() {
    let s = start(true).await;
    let http = reqwest::Client::new();
    let r = http.get(format!("{}/v1/runs", s.api)).send().await.unwrap();
    assert_eq!(r.status(), 401);
    let created: Value = http
        .post(format!("{}/v1/projects", s.api))
        .header("authorization", "Bearer admin-secret")
        .json(&json!({"id": "team-a", "retention_days": 30}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key = created["api_key"].as_str().unwrap().to_string();
    let r = http
        .post(format!("{}/v1/projects", s.api))
        .header("x-api-key", &key)
        .json(&json!({"id": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "project keys cannot create projects");

    // Writes with a project key land in that project regardless of resource attributes.
    use prost::Message;
    let r = http
        .post(format!("{}/v1/traces", s.http))
        .header("content-type", "application/x-protobuf")
        .header("x-api-key", &key)
        .body(otlp([5; 16], "someone-else").encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let q: Value = http
        .post(format!("{}/v1/sql", s.api))
        .header("x-api-key", &key)
        .json(&json!({"query": "SELECT DISTINCT project_id FROM spans"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(q["rows"], json!([{"project_id": "team-a"}]));
    let r = http
        .get(format!("{}/v1/runs?project=other", s.api))
        .header("x-api-key", &key)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let r = http
        .get(format!("{}/v1/runs", s.api))
        .header("x-api-key", "fk_wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let mut client = TraceServiceClient::connect(s.grpc.clone()).await.unwrap();
    let err = client.export(otlp([6; 16], "x")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    s.stop().await;
}
