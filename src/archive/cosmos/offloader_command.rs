use super::error::CosmosError;
use crate::archive::archived_entry::ArchivedEntry;
use std::sync::mpsc::Sender as SyncSender;

/// Commands sent to the background Cosmos offloader worker.
#[derive(Debug)]
pub enum OffloaderCommand {
    /// Ingest a batch of committed entries for archival.
    WriteEntries(Vec<ArchivedEntry>),
    /// Force persistence of all buffered entries and acknowledge through responder.
    Flush(SyncSender<Result<(), CosmosError>>),
    /// Terminate the worker loop.
    Shutdown,
}
