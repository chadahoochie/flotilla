//! Pluggable asynchronous archival pipeline for committed Raft log entries.

pub mod archive_pipeline;
pub mod archived_entry;
pub mod async_archive_sink;
pub mod channel;
pub mod file_archive_sink;
pub mod null_archive_sink;
pub mod pipeline_error;
pub mod sinks;

pub use archive_pipeline::ArchivePipeline;
pub use archived_entry::ArchivedEntry;
pub use async_archive_sink::AsyncArchiveSink;
pub use channel::ArchivePipeline as PipelineChannel;
pub use file_archive_sink::FileArchiveSink;
pub use null_archive_sink::NullArchiveSink;
pub use pipeline_error::PipelineError;
