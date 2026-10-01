use std::fmt;

/// Errors arising from Cosmos DB archival and HTTP REST operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CosmosError {
    /// Failure during HMAC-SHA256 authentication generation or base64 decoding.
    Auth(String),
    /// HTTP transport or response status error.
    Http(String),
    /// Cosmos DB 429 RequestRateTooLarge error with retry-after backoff directive.
    RateLimited { retry_after_ms: u64 },
    /// In-memory queue error or capacity exhaustion.
    Queue(String),
    /// JSON serialization or deserialization failure.
    Serialization(String),
    /// Background offloader worker terminated or panicked.
    WorkerDied(String),
}

impl fmt::Display for CosmosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auth(msg) => write!(f, "Cosmos DB auth error: {msg}"),
            Self::Http(msg) => write!(f, "Cosmos DB HTTP error: {msg}"),
            Self::RateLimited { retry_after_ms } => {
                write!(
                    f,
                    "Cosmos DB 429 rate limited, retry after {retry_after_ms}ms"
                )
            }
            Self::Queue(msg) => write!(f, "Cosmos DB queue error: {msg}"),
            Self::Serialization(msg) => write!(f, "Cosmos DB serialization error: {msg}"),
            Self::WorkerDied(msg) => write!(f, "Cosmos DB worker died: {msg}"),
        }
    }
}

impl std::error::Error for CosmosError {}
