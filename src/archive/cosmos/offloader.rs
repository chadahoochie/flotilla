use super::client::CosmosClient;
use super::config::CosmosConfig;
use super::document::create_cosmos_document;
use super::error::CosmosError;
pub use super::offloader_command::OffloaderCommand;
use crate::archive::archived_entry::ArchivedEntry;
use crate::types::LogIndex;
use chrono::Utc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender as SyncSender;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Standalone pure batch uploader: converts entries to `CosmosDocument` and uploads them via `CosmosClient`.
pub async fn upload_entry_batch(
    client: &CosmosClient,
    entries: &[ArchivedEntry],
    config: &CosmosConfig,
) -> Result<Option<LogIndex>, CosmosError> {
    if entries.is_empty() {
        return Ok(None);
    }

    let mut last_persisted = None;
    for entry in entries {
        let doc = create_cosmos_document(entry, &config.slice_key, config.ttl_seconds, Utc::now());
        client.post_document(&doc).await?;
        last_persisted = Some(entry.index);
    }

    Ok(last_persisted)
}

/// Background worker loop that batches entries and uploads to Cosmos DB.
pub async fn run_offloader_worker(
    mut receiver: UnboundedReceiver<OffloaderCommand>,
    client: CosmosClient,
    config: CosmosConfig,
    watermark: Arc<AtomicU64>,
) {
    let mut buffer: Vec<ArchivedEntry> = Vec::with_capacity(config.batch_size);
    let mut flush_waiters: Vec<SyncSender<Result<(), CosmosError>>> = Vec::new();
    let flush_duration = Duration::from_millis(config.flush_interval_ms);

    loop {
        let timeout = tokio::time::sleep(flush_duration);
        tokio::pin!(timeout);

        tokio::select! {
            cmd = receiver.recv() => {
                match cmd {
                    Some(OffloaderCommand::WriteEntries(entries)) => {
                        buffer.extend(entries);
                        if buffer.len() >= config.batch_size {
                            let batch = std::mem::take(&mut buffer);
                            match upload_entry_batch(&client, &batch, &config).await {
                                Ok(Some(last_idx)) => {
                                    watermark.store(last_idx.0, Ordering::Release);
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    for waiter in flush_waiters.drain(..) {
                                        let _ = waiter.send(Err(err.clone()));
                                    }
                                }
                            }
                        }
                    }
                    Some(OffloaderCommand::Flush(waiter)) => {
                        flush_waiters.push(waiter);
                        let batch = std::mem::take(&mut buffer);
                        let result = upload_entry_batch(&client, &batch, &config).await;
                        match result {
                            Ok(Some(last_idx)) => {
                                watermark.store(last_idx.0, Ordering::Release);
                                for w in flush_waiters.drain(..) {
                                    let _ = w.send(Ok(()));
                                }
                            }
                            Ok(None) => {
                                for w in flush_waiters.drain(..) {
                                    let _ = w.send(Ok(()));
                                }
                            }
                            Err(err) => {
                                for w in flush_waiters.drain(..) {
                                    let _ = w.send(Err(err.clone()));
                                }
                            }
                        }
                    }
                    Some(OffloaderCommand::Shutdown) | None => {
                        if !buffer.is_empty() {
                            let batch = std::mem::take(&mut buffer);
                            if let Ok(Some(last_idx)) = upload_entry_batch(&client, &batch, &config).await {
                                watermark.store(last_idx.0, Ordering::Release);
                            }
                        }
                        for w in flush_waiters.drain(..) {
                            let _ = w.send(Ok(()));
                        }
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                if !buffer.is_empty() {
                    let batch = std::mem::take(&mut buffer);
                    match upload_entry_batch(&client, &batch, &config).await {
                        Ok(Some(last_idx)) => {
                            watermark.store(last_idx.0, Ordering::Release);
                            for w in flush_waiters.drain(..) {
                                let _ = w.send(Ok(()));
                            }
                        }
                        Ok(None) => {
                            for w in flush_waiters.drain(..) {
                                let _ = w.send(Ok(()));
                            }
                        }
                        Err(err) => {
                            for w in flush_waiters.drain(..) {
                                let _ = w.send(Err(err.clone()));
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Spawns and manages the asynchronous Cosmos offloader worker.
pub struct CosmosOffloader {
    pub sender: UnboundedSender<OffloaderCommand>,
    pub watermark: Arc<AtomicU64>,
}

impl CosmosOffloader {
    /// Spawn the background offloader worker on the provided Tokio runtime handle.
    pub fn spawn(client: CosmosClient, config: CosmosConfig) -> Self {
        let (tx, rx) = unbounded_channel();
        let watermark = Arc::new(AtomicU64::new(0));
        let worker_watermark = Arc::clone(&watermark);

        tokio::spawn(async move {
            run_offloader_worker(rx, client, config, worker_watermark).await;
        });

        Self {
            sender: tx,
            watermark,
        }
    }

    /// Return a clone of the command sender.
    pub fn sender(&self) -> UnboundedSender<OffloaderCommand> {
        self.sender.clone()
    }

    /// Return the highest persisted log index recorded by the background worker.
    pub fn persisted_watermark(&self) -> LogIndex {
        LogIndex(self.watermark.load(Ordering::Acquire))
    }
}
