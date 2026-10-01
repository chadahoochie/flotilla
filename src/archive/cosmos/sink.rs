use super::client::CosmosClient;
use super::config::CosmosConfig;
use super::error::CosmosError;
use super::offloader::{CosmosOffloader, OffloaderCommand};
use crate::archive::archived_entry::ArchivedEntry;
use crate::archive::async_archive_sink::AsyncArchiveSink;
use crate::types::LogIndex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::channel;
use tokio::sync::mpsc::UnboundedSender;

/// Asynchronous archive sink for Azure Cosmos DB implementing `AsyncArchiveSink`.
pub struct CosmosArchiveSink {
    sender: UnboundedSender<OffloaderCommand>,
    watermark: Arc<AtomicU64>,
}

impl CosmosArchiveSink {
    /// Initialize and spawn a new Cosmos archive sink with its background worker.
    pub fn new(client: CosmosClient, config: CosmosConfig) -> Self {
        let offloader = CosmosOffloader::spawn(client, config);
        Self {
            sender: offloader.sender(),
            watermark: offloader.watermark,
        }
    }

    /// Construct a sink from an existing command sender and watermark reference.
    pub fn from_sender(
        sender: UnboundedSender<OffloaderCommand>,
        watermark: Arc<AtomicU64>,
    ) -> Self {
        Self { sender, watermark }
    }

    /// Return the highest log index successfully persisted by the offloader.
    pub fn persisted_watermark(&self) -> LogIndex {
        LogIndex(self.watermark.load(Ordering::Acquire))
    }
}

impl AsyncArchiveSink for CosmosArchiveSink {
    type Error = CosmosError;

    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error> {
        if entries.is_empty() {
            return Ok(0);
        }
        self.sender
            .send(OffloaderCommand::WriteEntries(entries.to_vec()))
            .map_err(|e| CosmosError::Queue(format!("Failed to enqueue entries: {e}")))?;
        Ok(entries.len() as u64)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        let (tx, rx) = channel();
        self.sender
            .send(OffloaderCommand::Flush(tx))
            .map_err(|e| CosmosError::Queue(format!("Failed to send flush command: {e}")))?;
        rx.recv()
            .map_err(|e| CosmosError::WorkerDied(format!("Worker flush dropped: {e}")))?
    }
}
