# Pluggable Archive Sinks Specification (L2)

## `AsyncArchiveSink` Trait
Defined in [`src/archive/async_archive_sink.rs`](file:///home/chad/source/rust/flotilla/src/archive/async_archive_sink.rs), this trait provides an asynchronous interface for persisting committed entry batches:

```rust
pub trait AsyncArchiveSink: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}
```

## Standard Implementations
1. [`NullArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/null_archive_sink.rs): Discards archived entries immediately. Used for in-memory testing and high-throughput non-persistent benchmarking.
2. [`FileArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/file_archive_sink.rs): Appends serialized log records to a sequential segment file on disk (Write-Ahead Log) with IEEE CRC32 verification.
3. [`CosmosArchiveSink`](cosmos_offloader_plan.md): Asynchronously offloads committed entries to Azure Cosmos DB with hierarchical partition keys (`["/sliceKey", "/dateBucket"]`), bounded micro-batching, and automatic watermark compaction. Gated behind feature flag `[features] cosmos`.

