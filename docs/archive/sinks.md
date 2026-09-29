# Pluggable Archive Sinks Specification (L2)

## `AsyncArchiveSink` Trait
The sink trait provides an asynchronous interface for persisting committed entry batches:

```rust
pub trait AsyncArchiveSink: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn write_entries(&mut self, entries: &[ArchiveEntry]) -> Result<u64, Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}
```

## Standard Implementations
1. `NullArchiveSink`: Discards archived entries immediately. Used for in-memory testing and high-throughput non-persistent benchmarking.
2. `FileArchiveSink`: Appends serialized log records to a sequential segment file on disk (Write-Ahead Log), updating an index file periodically.
