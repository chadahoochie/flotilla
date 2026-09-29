use super::archived_entry::ArchivedEntry;

/// Abstract contract for asynchronous persistence sinks (WAL files, databases, S3, etc.).
pub trait AsyncArchiveSink: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persist a batch of committed entries to durable storage.
    /// Returns the number of entries successfully written.
    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error>;

    /// Flush any in-flight buffered writes to durable medium (e.g. fsync).
    fn flush(&mut self) -> Result<(), Self::Error>;
}
