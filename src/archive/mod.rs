//! Pluggable asynchronous archival pipeline for committed Raft log entries.

pub mod channel;
pub mod sinks;

pub use channel::ArchivePipeline;
pub use sinks::{FileArchiveSink, NullArchiveSink};

use crate::types::{LogIndex, Term};

/// Single committed entry offloaded for persistent archival.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedEntry {
    pub index: LogIndex,
    pub term: Term,
    pub payload: Vec<u8>,
}

/// Abstract contract for asynchronous persistence sinks (WAL files, databases, S3, etc.).
pub trait AsyncArchiveSink: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persist a batch of committed entries to durable storage.
    /// Returns the number of entries successfully written.
    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error>;

    /// Flush any in-flight buffered writes to durable medium (e.g. fsync).
    fn flush(&mut self) -> Result<(), Self::Error>;
}
