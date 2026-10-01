//! Azure Cosmos DB asynchronous archival offloader for committed Raft log entries.

pub mod auth;
pub mod client;
pub mod config;
pub mod document;
pub mod error;
pub mod offloader;
pub mod offloader_command;
pub mod sink;

pub use auth::CosmosAuth;
pub use client::{
    CosmosClient, build_docs_resource_link, build_docs_url, build_document_headers,
    format_partition_key_header, format_rfc1123_date,
};
pub use config::CosmosConfig;
pub use document::{CosmosDocument, create_cosmos_document, format_date_bucket};
pub use error::CosmosError;
pub use offloader::{CosmosOffloader, run_offloader_worker, upload_entry_batch};
pub use offloader_command::OffloaderCommand;
pub use sink::CosmosArchiveSink;
