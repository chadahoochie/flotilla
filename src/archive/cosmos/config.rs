/// Configuration parameters for the Azure Cosmos DB archival offloader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CosmosConfig {
    /// Azure Cosmos DB account endpoint URI (e.g. `https://example.documents.azure.com:443/`).
    pub endpoint: String,
    /// Base64-encoded Azure Master Key.
    pub master_key: String,
    /// Target database ID.
    pub database: String,
    /// Target container ID (e.g. `ingress-journal-v2`).
    pub container: String,
    /// Level 1 hierarchical partition key prefix (e.g. `adt`, `siu`).
    pub slice_key: String,
    /// Maximum entries accumulated in a single micro-batch before flushing.
    pub batch_size: usize,
    /// Maximum duration in milliseconds to buffer entries before forcing a flush.
    pub flush_interval_ms: u64,
    /// Time-to-live in seconds for Cosmos DB document retention (default: 7,776,000 = 90 days).
    pub ttl_seconds: i32,
    /// Maximum retry attempts on HTTP 429 rate limit responses.
    pub max_retries: usize,
}

impl CosmosConfig {
    /// Create a new configuration with production defaults for batching, TTL, and retries.
    pub fn new(
        endpoint: impl Into<String>,
        master_key: impl Into<String>,
        database: impl Into<String>,
        container: impl Into<String>,
        slice_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            master_key: master_key.into(),
            database: database.into(),
            container: container.into(),
            slice_key: slice_key.into(),
            batch_size: 100,
            flush_interval_ms: 250,
            ttl_seconds: 7_776_000, // 90 days retention
            max_retries: 5,
        }
    }
}
