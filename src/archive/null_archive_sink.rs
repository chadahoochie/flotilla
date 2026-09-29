use super::archived_entry::ArchivedEntry;
use super::async_archive_sink::AsyncArchiveSink;
use std::io;

/// No-op archive sink that discards entries, ideal for in-memory benchmarks and tests.
#[derive(Debug, Default, Clone)]
pub struct NullArchiveSink {
    pub total_written: u64,
}

impl NullArchiveSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AsyncArchiveSink for NullArchiveSink {
    type Error = io::Error;

    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error> {
        let count = entries.len() as u64;
        self.total_written += count;
        Ok(count)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
