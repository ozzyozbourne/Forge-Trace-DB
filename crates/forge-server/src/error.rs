//! Mapping Forge errors to HTTP responses and gRPC statuses.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use forge_model::ForgeError;
use serde_json::json;

pub struct ApiError(pub ForgeError);

impl From<ForgeError> for ApiError {
    fn from(e: ForgeError) -> Self {
        ApiError(e)
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError(ForgeError::Internal(e))
    }
}

pub fn status_of(e: &ForgeError) -> StatusCode {
    match e {
        ForgeError::InvalidInput(_) => StatusCode::BAD_REQUEST,
        ForgeError::NotFound(_) => StatusCode::NOT_FOUND,
        ForgeError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
        ForgeError::Backpressure(_) => StatusCode::TOO_MANY_REQUESTS,
        ForgeError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
        ForgeError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = status_of(&self.0);
        if status.is_server_error() {
            tracing::error!(error = ?self.0, "request failed");
        }
        let mut resp = (status, Json(json!({ "error": self.0.to_string() }))).into_response();
        if status == StatusCode::TOO_MANY_REQUESTS {
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        resp
    }
}

pub fn to_status(e: ForgeError) -> tonic::Status {
    match e {
        ForgeError::InvalidInput(m) => tonic::Status::invalid_argument(m),
        ForgeError::NotFound(m) => tonic::Status::not_found(m),
        ForgeError::Unauthorized(m) => tonic::Status::unauthenticated(m),
        ForgeError::Backpressure(m) => tonic::Status::resource_exhausted(m),
        ForgeError::Timeout(m) => tonic::Status::deadline_exceeded(m),
        ForgeError::Internal(e) => {
            tracing::error!(error = ?e, "grpc request failed");
            tonic::Status::internal(e.to_string())
        }
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
