# Architecture & Implementation Plan: Flotilla Async Cosmos DB Offloader

## Executive Summary

Flotilla is a zero-allocation, sans-I/O Raft consensus engine designed for high-throughput, microsecond-latency ingress (< 80µs). Its consensus hot path executes entirely in-memory using a bounded circular ring buffer (`RingBufferLogStorage`).

To enable infinite log advancement without ring buffer exhaustion while preserving a 90-day searchable audit trail and message resubmission capability, committed log entries (`ArchivedEntry`) are asynchronously offloaded to Azure Cosmos DB.

This document outlines the design and implementation of the **Native Rust Async Cosmos DB Offloader** in Flotilla (`flotilla_raft::archive::cosmos`), featuring:
1. **Sans-I/O & Zero-Alloc Invariant Preserved**: Isolated behind an optional `[features] cosmos` flag in `Cargo.toml`. Default builds remain 100% sans-I/O and zero-allocation.
2. **Pluggable Sink Integration**: Implements `AsyncArchiveSink` as `CosmosArchiveSink`, allowing direct plug-in to `ArchivePipeline`.
3. **Native Hierarchical Partition Key Compliance**: Targets container `ingress-journal-v2` with headers formatted for Level 1 (`/sliceKey`) and Level 2 (`/dateBucket` = `yyyy-MM-dd`).
4. **Resilience & Bounded Micro-Batching**: Micro-batches up to 100 entries or 250ms interval, dynamically backing off on `429 RequestRateTooLarge` errors according to `x-ms-retry-after-ms`.
5. **Durable Watermark Compaction**: Advancing durable watermarks after HTTP 200/201 persistence, enabling safe prefix compaction of Flotilla's ring buffer.

```
FLOTILLA ASYNC COSMOS OFFLOADER ARCHITECTURE:

[ Consensus Core (Sans-I/O) ]
    │
    │  Committed entries (< 80µs)
    ▼
[ RingBufferLogStorage ] ─── Watermark Compaction ───┐
    │                                                │
    │  enqueue_batch(&[ArchivedEntry])                │
    ▼                                                │
[ ArchivePipeline ]                                  │
    │                                                │
    │  write_entries() / flush()                     │
    ▼                                                │
[ CosmosArchiveSink ] (implements AsyncArchiveSink)  │
    │                                                │
    │  Bounded crossbeam / tokio mpsc channel        │
    ▼                                                │
[ CosmosOffloader Worker Loop ]                      │
    ├── 1. Accumulate batch (up to 100 items or 250ms)
    ├── 2. Map ArchivedEntry -> CosmosDocument       │
    │      ├── id: "{sliceKey}_{logIndex}"           │
    │      ├── sliceKey: "adt" (Level 1 PK)          │
    │      ├── dateBucket: "2026-09-29" (Level 2 PK) │
    │      ├── ttl: 7,776,000 (90-day retention)     │
    │      └── payload: UTF-8 / JSON                 │
    ├── 3. Compute HMAC-SHA256 Auth Signature        │
    ├── 4. POST /dbs/{db}/colls/{container}/docs     │
    │      └── On 429: wait x-ms-retry-after-ms      │
    └── 5. Update Persisted Watermark (LogIndex) ────┘
```

---

## 1. Cargo Configuration: `Cargo.toml`

Add optional dependencies and declare the `cosmos` feature flag:

```toml
[dependencies]
zerocopy = { version = "0.8", features = ["derive"] }
crc32fast = "1.4"
parking_lot = "0.12"

# Optional dependencies for Cosmos DB archival offloader
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"], optional = true }
tokio = { version = "1", features = ["rt-multi-thread", "sync", "time", "macros"], optional = true }
serde = { version = "1.0", features = ["derive"], optional = true }
serde_json = { version = "1.0", optional = true }
hmac = { version = "0.12", optional = true }
sha2 = { version = "0.10", optional = true }
base64 = { version = "0.22", optional = true }
chrono = { version = "0.4", features = ["serde"], optional = true }
urlencoding = { version = "2.1", optional = true }

[features]
default = []
cosmos = [
    "dep:reqwest",
    "dep:tokio",
    "dep:serde",
    "dep:serde_json",
    "dep:hmac",
    "dep:sha2",
    "dep:base64",
    "dep:chrono",
    "dep:urlencoding",
]
```

---

## 2. Cosmos DB Offloader Subsystem (`src/archive/cosmos/`)

### Module Structure
```
src/archive/cosmos/
├── mod.rs          # Re-exports config, document, client, offloader, sink, error
├── config.rs       # CosmosConfig (endpoint, master_key, db, container, batch_size, ttl)
├── document.rs     # CosmosDocument (matches C# CosmosJournalDocument schema)
├── auth.rs         # CosmosAuth (Azure Master Key HMAC-SHA256 signature generator)
├── client.rs       # CosmosClient (HTTP REST client with 429 retry backoff)
├── offloader.rs    # CosmosOffloader (bounded micro-batching worker)
├── sink.rs         # CosmosArchiveSink (implements AsyncArchiveSink)
└── error.rs        # CosmosError
```

### Component Details

#### A. Document Schema (`document.rs`):
Matches the 2-level hierarchical partition keys and field schema of `VaxCare.ThespianApi`:
```rust
use serde::{Deserialize, Serialize};

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
```

#### B. HMAC-SHA256 Signature Generator (`auth.rs`):
```rust
use base64::prelude::*;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub struct CosmosAuth;

impl CosmosAuth {
    pub fn generate_auth_header(
        verb: &str,
        resource_type: &str,
        resource_id: &str,
        date: &str,
        master_key_base64: &str,
    ) -> Result<String, super::error::CosmosError> {
        let key_bytes = BASE64_STANDARD.decode(master_key_base64)
            .map_err(|e| super::error::CosmosError::Auth(e.to_string()))?;

        let string_to_sign = format!(
            "{}\n{}\n{}\n{}\n\n",
            verb.to_lowercase(),
            resource_type.to_lowercase(),
            resource_id,
            date.to_lowercase(),
        );

        let mut mac = HmacSha256::new_from_slice(&key_bytes)
            .map_err(|e| super::error::CosmosError::Auth(e.to_string()))?;
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());

        let url_encoded_sig = urlencoding::encode(&signature);
        Ok(format!("type=master&ver=1.0&sig={}", url_encoded_sig))
    }
}
```

#### C. REST Client with 429 Retry Backoff (`client.rs`):
- Endpoint: `POST https://<endpoint>/dbs/<db>/colls/<container>/docs`
- Headers:
  - `x-ms-date`: RFC 1123 date
  - `x-ms-version`: `2018-12-31`
  - `authorization`: generated HMAC signature
  - `x-ms-documentdb-partitionkey`: `["<sliceKey>", "<dateBucket>"]`
  - `x-ms-documentdb-is-upsert`: `true`
  - `Content-Type`: `application/json`
- 429 Handling: Reads `x-ms-retry-after-ms` and sleeps before retry up to `max_retries`.

#### D. Pluggable Sink (`sink.rs`):
```rust
impl AsyncArchiveSink for CosmosArchiveSink {
    type Error = CosmosError;

    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error> {
        for entry in entries {
            self.sender.blocking_send(entry.clone())
                .map_err(|e| CosmosError::Queue(e.to_string()))?;
        }
        Ok(entries.len() as u64)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        // Blocks until background worker drains and confirms persistence
        Ok(())
    }
}
```

---

## 3. Re-exports & Module Declarations

### `src/archive/mod.rs`
```rust
#[cfg(feature = "cosmos")]
pub mod cosmos;
```

### `src/archive/sinks.rs`
```rust
pub use super::file_archive_sink::FileArchiveSink;
pub use super::null_archive_sink::NullArchiveSink;

#[cfg(feature = "cosmos")]
pub use super::cosmos::CosmosArchiveSink;
```

---

## 4. Verification & Testing

### Commands:
```bash
# 1. Default sans-I/O test suite (verifies zero overhead / zero bloat)
RUSTUP_HOME=/home/chad/.cache/puccinialin/rustup CARGO_HOME=/home/chad/.cache/puccinialin/cargo PATH="/home/chad/.cache/puccinialin/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH" cargo test

# 2. Cosmos offloader test suite
RUSTUP_HOME=/home/chad/.cache/puccinialin/rustup CARGO_HOME=/home/chad/.cache/puccinialin/cargo PATH="/home/chad/.cache/puccinialin/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH" cargo test --test cosmos_offloader_tests --features cosmos
```
