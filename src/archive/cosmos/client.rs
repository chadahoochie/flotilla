use super::auth::CosmosAuth;
use super::config::CosmosConfig;
use super::document::CosmosDocument;
use super::error::CosmosError;
use chrono::{DateTime, Utc};
use reqwest::Client;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use std::time::Duration;

/// Format date into RFC 1123 string required by Azure Cosmos DB REST API (`x-ms-date`).
pub fn format_rfc1123_date(dt: DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Format the 2-level hierarchical partition key header value: `["<sliceKey>", "<dateBucket>"]`.
pub fn format_partition_key_header(slice_key: &str, date_bucket: &str) -> String {
    serde_json::to_string(&[slice_key, date_bucket])
        .unwrap_or_else(|_| format!("[\"{slice_key}\",\"{date_bucket}\"]"))
}

/// Build the Cosmos DB REST resource link for document collections.
pub fn build_docs_resource_link(db: &str, container: &str) -> String {
    format!("dbs/{db}/colls/{container}")
}

/// Build the full HTTP endpoint URL for creating/upserting documents.
pub fn build_docs_url(endpoint: &str, db: &str, container: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    format!("{base}/dbs/{db}/colls/{container}/docs")
}

/// Build required HTTP headers for document creation in Cosmos DB.
pub fn build_document_headers(
    config: &CosmosConfig,
    doc: &CosmosDocument,
    date_rfc1123: &str,
) -> Result<HeaderMap, CosmosError> {
    let mut headers = HeaderMap::new();

    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        HeaderName::from_static("x-ms-version"),
        HeaderValue::from_static("2018-12-31"),
    );
    headers.insert(
        HeaderName::from_static("x-ms-date"),
        HeaderValue::from_str(date_rfc1123)
            .map_err(|e| CosmosError::Http(format!("Invalid date header: {e}")))?,
    );
    headers.insert(
        HeaderName::from_static("x-ms-documentdb-is-upsert"),
        HeaderValue::from_static("true"),
    );

    let pk_header = format_partition_key_header(&doc.slice_key, &doc.date_bucket);
    headers.insert(
        HeaderName::from_static("x-ms-documentdb-partitionkey"),
        HeaderValue::from_str(&pk_header)
            .map_err(|e| CosmosError::Http(format!("Invalid partition key header: {e}")))?,
    );

    let resource_id = build_docs_resource_link(&config.database, &config.container);
    let auth_header = CosmosAuth::generate_auth_header(
        "post",
        "docs",
        &resource_id,
        date_rfc1123,
        &config.master_key,
    )?;

    headers.insert(
        HeaderName::from_static("authorization"),
        HeaderValue::from_str(&auth_header)
            .map_err(|e| CosmosError::Auth(format!("Invalid auth header: {e}")))?,
    );

    Ok(headers)
}

/// Asynchronous REST client for Azure Cosmos DB with automatic 429 rate limit backoff.
#[derive(Clone)]
pub struct CosmosClient {
    pub client: Client,
    pub config: CosmosConfig,
}

impl CosmosClient {
    /// Create a new Cosmos REST client reusing an underlying connection pool.
    pub fn new(config: CosmosConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { client, config }
    }

    /// Upload a single document with exponential backoff on HTTP 429.
    pub async fn post_document(&self, doc: &CosmosDocument) -> Result<(), CosmosError> {
        let url = build_docs_url(
            &self.config.endpoint,
            &self.config.database,
            &self.config.container,
        );
        let body =
            serde_json::to_string(doc).map_err(|e| CosmosError::Serialization(e.to_string()))?;

        let mut attempts = 0;
        loop {
            attempts += 1;
            let date = format_rfc1123_date(Utc::now());
            let headers = build_document_headers(&self.config, doc, &date)?;

            let resp = self
                .client
                .post(&url)
                .headers(headers)
                .body(body.clone())
                .send()
                .await
                .map_err(|e| CosmosError::Http(e.to_string()))?;

            let status = resp.status();
            if status.is_success() {
                return Ok(());
            }

            if status.as_u16() == 429 {
                let retry_after_ms = resp
                    .headers()
                    .get("x-ms-retry-after-ms")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(100);

                if attempts >= self.config.max_retries {
                    return Err(CosmosError::RateLimited { retry_after_ms });
                }

                tokio::time::sleep(Duration::from_millis(retry_after_ms)).await;
                continue;
            }

            let err_body = resp.text().await.unwrap_or_default();
            return Err(CosmosError::Http(format!(
                "Cosmos POST failed with HTTP {status}: {err_body}"
            )));
        }
    }
}
