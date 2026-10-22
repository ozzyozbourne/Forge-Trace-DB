//! The error type shared by every Forge crate. Variants that change an API status code are
//! explicit; everything else is `Internal`.

#[derive(Debug, thiserror::Error)]
pub enum ForgeError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("overloaded: {0}")]
    Backpressure(String),
    #[error("timed out: {0}")]
    Timeout(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type Result<T, E = ForgeError> = std::result::Result<T, E>;

impl ForgeError {
    pub fn invalid(msg: impl Into<String>) -> Self {
        ForgeError::InvalidInput(msg.into())
    }
    pub fn internal(msg: impl std::fmt::Display) -> Self {
        ForgeError::Internal(anyhow::anyhow!("{msg}"))
    }
}

macro_rules! internal_from {
    ($($t:ty),*) => {
        $(impl From<$t> for ForgeError {
            fn from(e: $t) -> Self { ForgeError::Internal(anyhow::Error::new(e)) }
        })*
    };
}
internal_from!(arrow::error::ArrowError, std::io::Error, serde_json::Error);
