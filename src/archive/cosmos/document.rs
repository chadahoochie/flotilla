use crate::archive::archived_entry::ArchivedEntry;
use base64::prelude::*;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Document structure persisted to Azure Cosmos DB container `ingress-journal-v2`.
/// Adheres to 2-level hierarchical partition keys: Level 1 (`/sliceKey`) and Level 2 (`/dateBucket`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CosmosDocument {
    pub id: String,
    #[serde(rename = "sliceKey")]
    pub slice_key: String,
    #[serde(rename = "dateBucket")]
    pub date_bucket: String,
    #[serde(rename = "logIndex")]
    pub log_index: u64,
    pub term: u64,
    #[serde(rename = "receivedAt")]
    pub received_at: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: String,
    pub ttl: i32,
    pub payload: String,
    pub source: String,
    #[serde(rename = "messageType")]
    pub message_type: String,
    pub outcome: String,
}

/// Format a UTC timestamp into date bucket string `yyyy-MM-dd` (Level 2 partition key).
pub fn format_date_bucket(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d").to_string()
}

/// Standalone constructor mapping an `ArchivedEntry` into a `CosmosDocument`.
pub fn create_cosmos_document(
    entry: &ArchivedEntry,
    slice_key: &str,
    ttl_seconds: i32,
    now: DateTime<Utc>,
) -> CosmosDocument {
    let date_bucket = format_date_bucket(now);
    let expires_at = now + Duration::seconds(ttl_seconds as i64);
    let id = format!("{}_{}", slice_key, entry.index.0);

    let payload_str = match std::str::from_utf8(&entry.payload) {
        Ok(s) => s.to_string(),
        Err(_) => base64::prelude::BASE64_STANDARD.encode(&entry.payload),
    };

    CosmosDocument {
        id,
        slice_key: slice_key.to_string(),
        date_bucket,
        log_index: entry.index.0,
        term: entry.term.0,
        received_at: now.to_rfc3339(),
        expires_at: expires_at.to_rfc3339(),
        ttl: ttl_seconds,
        payload: payload_str,
        source: "flotilla".to_string(),
        message_type: "RaftLogEntry".to_string(),
        outcome: "Committed".to_string(),
    }
}
