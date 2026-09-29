use crate::codec::CodecError;
use crate::storage::StorageError;

/// Errors produced during Raft consensus stepping and proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineError {
    NotLeader,
    Storage(StorageError),
    Codec(CodecError),
    InvalidPacket,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader => write!(f, "current node is not the elected leader"),
            Self::Storage(e) => write!(f, "storage engine error: {e}"),
            Self::Codec(e) => write!(f, "codec serialization error: {e}"),
            Self::InvalidPacket => write!(f, "inbound packet failed decoding or verification"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<StorageError> for EngineError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

impl From<CodecError> for EngineError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}
