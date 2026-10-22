//! Forge process assembly and network endpoints: OTLP receivers, the JSON API, ops endpoints.

pub mod api;
pub mod auth;
pub mod error;
pub mod otlp;
pub mod runs;
pub mod search;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use forge_ingest::{Ingester, IngesterRef};
use forge_model::Result;
use forge_model::config::Config;
use forge_model::otlp::Normalizer;
use forge_model::pricing::Pricing;
use forge_model::traits::{MetastoreRef, SegmentStoreRef};
use forge_query::QueryEngine;
use forge_storage::{ObjectStoreRef, VortexSegmentStore};
use tokio::net::TcpListener;
use tokio::sync::watch;

pub struct Forge {
    pub config: Config,
    pub metastore: MetastoreRef,
    pub object_store: ObjectStoreRef,
    pub segments: SegmentStoreRef,
    pub vortex: Arc<VortexSegmentStore>,
    pub ingester: IngesterRef,
    pub query: QueryEngine,
    pub lifecycle: forge_ingest::lifecycle::LifecycleRef,
    pub fts: Arc<forge_index::FtsReader>,
    pub normalizer: Normalizer,
    pub auth: auth::Auth,
    pub cache_stats: Option<Arc<forge_storage::CacheStats>>,
    pub ready: AtomicBool,
}

impl Forge {
    pub async fn open(config: Config) -> Result<Arc<Forge>> {
        let metastore = forge_meta::open(&config.metastore.url)?;
        let object_store = forge_storage::from_url(&config.storage.url, &config.storage.options)?;
        Self::open_with(config, metastore, object_store).await
    }

    /// Assemble from an explicit metastore and object store (tests, fault injection).
    pub async fn open_with(
        config: Config,
        metastore: MetastoreRef,
        base_store: ObjectStoreRef,
    ) -> Result<Arc<Forge>> {
        // Retries for everything; the read cache for segments, sidecars and blobs.
        let layers = forge_storage::layer(base_store, &config)?;
        let object_store = layers.direct.clone();
        let cache_stats = layers.cache_stats.clone();
        let vortex = Arc::new(VortexSegmentStore::new(layers.cached.clone()));
        let segments: SegmentStoreRef = vortex.clone();
        let ingester = Ingester::open(
            config.clone(),
            metastore.clone(),
            segments.clone(),
            object_store.clone(),
        )
        .await?;
        let fts = Arc::new(forge_index::FtsReader::new(
            segments.clone(),
            config.cache.memory.bytes() / 4,
        ));
        let query = QueryEngine::new(
            metastore.clone(),
            segments.clone(),
            Some(fts.clone()),
            ingester.clone(),
            config.query.clone(),
        );
        let lifecycle = forge_ingest::lifecycle::Lifecycle::new(
            config.clone(),
            metastore.clone(),
            segments.clone(),
        );
        let auth = auth::Auth::new(
            config.auth.enabled,
            config.auth.admin_key.as_deref(),
            metastore.clone(),
        )?;
        Ok(Arc::new(Forge {
            normalizer: Normalizer::new(Pricing::new(&config.pricing)),
            config,
            metastore,
            object_store,
            segments,
            vortex,
            ingester,
            query,
            lifecycle,
            fts,
            auth,
            cache_stats,
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

async fn bind(addr: &str, what: &str) -> Result<TcpListener> {
    let l = TcpListener::bind(addr)
        .await
        .map_err(|e| forge_model::ForgeError::internal(format!("binding {what} on {addr}: {e}")))?;
    tracing::info!(addr = %l.local_addr()?, "{what} listening");
    Ok(l)
}

/// Run every role until `shutdown` resolves, then flush the memtables.
pub async fn serve(
    f: Arc<Forge>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let (stop_tx, stop_rx) = watch::channel(false);
    let flusher = f.ingester.spawn_flusher(stop_rx.clone());
    let lifecycle = f
        .config
        .compaction
        .enabled
        .then(|| f.lifecycle.spawn(stop_rx.clone()));

    let ops = bind(&f.config.server.metrics_addr, "ops endpoint").await?;
    let otlp_http = bind(&f.config.server.otlp_http_addr, "OTLP/HTTP").await?;
    let grpc = bind(&f.config.server.otlp_grpc_addr, "OTLP/gRPC").await?;
    let api = bind(&f.config.server.api_addr, "API").await?;

    let wait = |mut rx: watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|v| *v).await;
    };
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(
        axum::serve(ops, ops_router(f.clone()))
            .with_graceful_shutdown(wait(stop_rx.clone()))
            .into_future(),
    );
    tasks.spawn(
        axum::serve(otlp_http, otlp::http_router(f.clone()))
            .with_graceful_shutdown(wait(stop_rx.clone()))
            .into_future(),
    );
    tasks.spawn(
        axum::serve(api, api::router(f.clone()))
            .with_graceful_shutdown(wait(stop_rx.clone()))
            .into_future(),
    );
    let grpc_service = otlp::grpc_service(f.clone());
    let grpc_stop = wait(stop_rx.clone());
    tasks.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming_shutdown(tokio_stream_listener(grpc), grpc_stop)
            .await
            .map_err(std::io::Error::other)
    });

    f.ready.store(true, Ordering::Release);
    tracing::info!("forge is ready");
    tokio::select! {
        _ = shutdown => {}
        Some(res) = tasks.join_next() => {
            tracing::error!(?res, "a server task exited");
        }
    }
    f.ready.store(false, Ordering::Release);
    let _ = stop_tx.send(true);
    while tasks.join_next().await.is_some() {}
    let _ = flusher.await;
    if let Some(l) = lifecycle {
        let _ = l.await;
    }
    match f.ingester.flush_now().await {
        Ok(n) => tracing::info!(segments = n, "flushed memtables on shutdown"),
        Err(e) => tracing::error!(error = %e, "final flush failed; data remains in the WAL"),
    }
    Ok(())
}

fn tokio_stream_listener(
    l: TcpListener,
) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    futures::stream::unfold(l, |l| async move {
        let r = l.accept().await.map(|(s, _)| s);
        Some((r, l))
    })
}
