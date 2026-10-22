//! OTLP receivers: gRPC `TraceService/Export` and OTLP/HTTP `POST /v1/traces`.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use forge_model::opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use forge_model::opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use forge_model::otlp::decode_request;
use prost::Message;
use tonic::codec::CompressionEncoding;

use crate::Forge;
use crate::auth::PROJECT_HEADER;
use crate::error::{ApiResult, to_status};

impl Forge {
    /// Normalize and ingest one OTLP request on behalf of `project` (if bound).
    pub async fn ingest_otlp(
        &self,
        req: &ExportTraceServiceRequest,
        project: Option<&str>,
    ) -> forge_model::Result<usize> {
        let rows = self
            .normalizer
            .normalize(req, project, &self.config.ingest.default_project)?;
        self.ingester.ingest(rows).await
    }
}

fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"))
}

async fn http_export(
    State(f): State<Arc<Forge>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let principal = f.auth.authenticate(&headers).await?;
    let json = is_json(&headers);
    let req = decode_request(&body, json)?;
    f.ingest_otlp(&req, principal.project()).await?;
    let resp = ExportTraceServiceResponse {
        partial_success: None,
    };
    Ok(if json {
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            "{}",
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            )],
            resp.encode_to_vec(),
        )
            .into_response()
    })
}

pub fn http_router(f: Arc<Forge>) -> Router {
    let limit = f.config.ingest.max_request.bytes() as usize;
    Router::new()
        .route("/v1/traces", post(http_export))
        .layer(DefaultBodyLimit::max(limit))
        .layer(tower_http::decompression::RequestDecompressionLayer::new())
        .layer(axum::middleware::from_fn_with_state(
            f.clone(),
            crate::metrics::track_otlp,
        ))
        .with_state(f)
}

pub struct GrpcTraceService {
    forge: Arc<Forge>,
}

#[tonic::async_trait]
impl TraceService for GrpcTraceService {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let md = request.metadata();
        let key = md
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .or_else(|| {
                md.get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|a| a.strip_prefix("Bearer ").map(str::to_string))
            });
        let named = md
            .get(PROJECT_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let principal = self
            .forge
            .auth
            .authenticate_key(key.as_deref(), named.as_deref())
            .await
            .map_err(to_status)?;
        self.forge
            .ingest_otlp(request.get_ref(), principal.project())
            .await
            .map_err(to_status)?;
        Ok(tonic::Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

pub fn grpc_service(f: Arc<Forge>) -> TraceServiceServer<GrpcTraceService> {
    let limit = f.config.ingest.max_request.bytes() as usize;
    TraceServiceServer::new(GrpcTraceService { forge: f })
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(limit)
}
