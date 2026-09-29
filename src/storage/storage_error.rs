use crate::types::LogIndex;

/// Error types emitted by the ring buffer storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    BufferFull,
    PayloadTooLarge { max: usize, actual: usize },
    IndexOutOfBounds { index: LogIndex },
    CompactionIndexTooHigh { watermark: LogIndex, last: LogIndex },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BufferFull => write!(f, "ring buffer storage is full; need compaction"),
            Self::PayloadTooLarge { max, actual } => {
                write!(f, "entry payload too large: max {max} bytes, got {actual} bytes")
            }
            Self::IndexOutOfBounds { index } => {
                write!(f, "requested log index {} is out of retained bounds", index.0)
            }
            Self::CompactionIndexTooHigh { watermark, last } => {
                write!(
                    f,
                    "compaction watermark {} cannot exceed last log index {}",
                    watermark.0, last.0
                )
            }
        }
    }
}

impl std::error::Error for StorageError {}
