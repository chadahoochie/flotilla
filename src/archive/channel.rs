use super::{ArchivedEntry, AsyncArchiveSink};
use crate::types::LogIndex;
use std::collections::VecDeque;

/// Errors emitted by the archive pipeline queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineError {
    QueueFull,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull => write!(f, "archive pipeline queue is at maximum capacity"),
        }
    }
}

impl std::error::Error for PipelineError {}

/// Pipeline managing buffered transfer of committed entries to an asynchronous archive sink.
pub struct ArchivePipeline<S: AsyncArchiveSink> {
    pub sink: S,
    pub max_capacity: usize,
    pub queue: VecDeque<ArchivedEntry>,
    pub watermark: LogIndex,
}

impl<S: AsyncArchiveSink> ArchivePipeline<S> {
    /// Create a new archive pipeline with the designated sink and queue capacity.
    pub fn new(sink: S, max_capacity: usize) -> Self {
        Self {
            sink,
            max_capacity,
            queue: VecDeque::with_capacity(max_capacity),
            watermark: LogIndex::ZERO,
        }
    }

    /// Enqueue a batch of committed entries into the pipeline queue.
    pub fn enqueue_batch(&mut self, entries: &[ArchivedEntry]) -> Result<(), PipelineError> {
        if self.queue.len() + entries.len() > self.max_capacity {
            return Err(PipelineError::QueueFull);
        }
        for entry in entries {
            self.queue.push_back(entry.clone());
        }
        Ok(())
    }

    /// Drain pending entries from the queue, write them to the sink, and advance the watermark.
    pub fn drain_and_persist(&mut self) -> Result<usize, S::Error> {
        if self.queue.is_empty() {
            return Ok(0);
        }

        let batch: Vec<ArchivedEntry> = self.queue.drain(..).collect();
        let count = batch.len();
        if let Some(last) = batch.last() {
            self.watermark = last.index;
        }

        self.sink.write_entries(&batch)?;
        self.sink.flush()?;

        Ok(count)
    }

    /// Return the highest log index successfully persisted to the sink.
    #[inline(always)]
    pub fn persisted_watermark(&self) -> LogIndex {
        self.watermark
    }

    /// Flush the underlying sink.
    pub fn flush(&mut self) -> Result<(), S::Error> {
        self.sink.flush()
    }
}
