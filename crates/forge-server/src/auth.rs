//! Per-project API keys. A request carries `x-api-key: <key>` (the OTel exporter header
//! pattern) or `Authorization: Bearer <key>`. Keys are stored as SHA-256 hashes.

use std::time::Duration;

use axum::http::HeaderMap;
use forge_model::traits::MetastoreRef;
use forge_model::{ForgeError, Result};
use rand::Rng;
use sha2::{Digest, Sha256};

pub const PROJECT_HEADER: &str = "x-forge-project";

/// Who a request is.
#[derive(Debug, Clone, PartialEq)]
pub enum Principal {
    /// Bound to one project.
    Project(String),
    /// Admin key, or auth disabled: may name any project.
    Admin { project: Option<String> },
}

impl Principal {
    /// The project a write or project-scoped query applies to. `None` lets OTLP pick it from
    /// resource attributes.
    pub fn project(&self) -> Option<&str> {
        match self {
            Principal::Project(p) => Some(p),
            Principal::Admin { project } => project.as_deref(),
        }
    }

    pub fn is_admin(&self) -> bool {
        matches!(self, Principal::Admin { .. })
    }

    /// Whether this principal may read or write `project`.
    pub fn can_access(&self, project: &str) -> bool {
        match self {
            Principal::Project(p) => p == project,
            Principal::Admin { .. } => true,
        }
    }
}

pub fn hash_key(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

pub fn generate_key() -> String {
    let bytes: [u8; 24] = rand::rng().random();
    format!("fk_{}", hex::encode(bytes))
}

pub struct Auth {
    enabled: bool,
    admin_hash: Option<String>,
    metastore: MetastoreRef,
    cache: moka::future::Cache<String, Option<String>>,
}

impl Auth {
    pub fn new(enabled: bool, admin_key: Option<&str>, metastore: MetastoreRef) -> Result<Self> {
        if enabled && admin_key.is_none() {
            return Err(ForgeError::invalid("auth.enabled requires auth.admin_key"));
        }
        Ok(Auth {
            enabled,
            admin_hash: admin_key.map(hash_key),
            metastore,
            cache: moka::future::Cache::builder()
                .time_to_live(Duration::from_secs(30))
                .max_capacity(10_000)
                .build(),
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
    }

    pub fn key_from_headers(headers: &HeaderMap) -> Option<String> {
        if let Some(k) = Self::header(headers, "x-api-key") {
            return Some(k.to_string());
        }
        let auth = Self::header(headers, "authorization")?;
        auth.strip_prefix("Bearer ")
            .or_else(|| auth.strip_prefix("bearer "))
            .map(|k| k.trim().to_string())
    }

    pub async fn authenticate_key(
        &self,
        key: Option<&str>,
        project_header: Option<&str>,
    ) -> Result<Principal> {
        let named = project_header.filter(|p| !p.is_empty()).map(str::to_string);
        if !self.enabled {
            return Ok(Principal::Admin { project: named });
        }
        let key = key.ok_or_else(|| ForgeError::Unauthorized("missing API key".into()))?;
        let hash = hash_key(key);
        if self.admin_hash.as_deref() == Some(hash.as_str()) {
            return Ok(Principal::Admin { project: named });
        }
        let ms = self.metastore.clone();
        let h = hash.clone();
        let project = self
            .cache
            .try_get_with(hash, async move {
                ms.project_by_key_hash(&h).await.map(|p| p.map(|p| p.id))
            })
            .await
            .map_err(|e| ForgeError::internal(e.to_string()))?;
        match project {
            Some(p) => {
                if let Some(n) = &named
                    && n != &p
                {
                    return Err(ForgeError::Unauthorized(format!(
                        "key does not grant access to project {n}"
                    )));
                }
                Ok(Principal::Project(p))
            }
            None => Err(ForgeError::Unauthorized("invalid API key".into())),
        }
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal> {
        let key = Self::key_from_headers(headers);
        self.authenticate_key(key.as_deref(), Self::header(headers, PROJECT_HEADER))
            .await
    }

    pub async fn invalidate(&self) {
        self.cache.invalidate_all();
    }
}
