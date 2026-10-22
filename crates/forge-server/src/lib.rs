//! Forge process assembly and network endpoints.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use forge_model::Result;
use forge_model::config::Config;
use forge_model::traits::MetastoreRef;
use forge_storage::ObjectStoreRef;
use tokio::net::TcpListener;

pub struct Forge {
    pub config: Config,
    pub metastore: MetastoreRef,
    pub store: ObjectStoreRef,
    pub ready: AtomicBool,
}

impl Forge {
    pub async fn open(config: Config) -> Result<Arc<Forge>> {
        let metastore = forge_meta::open(&config.metastore.url)?;
        let store = forge_storage::from_url(&config.storage.url, &config.storage.options)?;
        Ok(Arc::new(Forge {
            config,
            metastore,
            store,
            ready: AtomicBool::new(false),
        }))
    }
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(f): State<Arc<Forge>>) -> (StatusCode, &'static str) {
    if f.ready.load(Ordering::Acquire) {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "starting")
    }
}

pub fn ops_router(f: Arc<Forge>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(f)
}

/// Run every role until `shutdown` resolves.
pub async fn serve(
    f: Arc<Forge>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let ops = TcpListener::bind(&f.config.server.metrics_addr).await?;
    tracing::info!(addr = %ops.local_addr()?, "ops endpoint listening");
    f.ready.store(true, Ordering::Release);
    axum::serve(ops, ops_router(f.clone()))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}
