use crate::codec::CodecError;
use crate::types::NodeId;
use std::fmt;
use std::io;

/// Error types occurring during client RPC, encoding, or transport operations.
#[derive(Debug)]
pub enum ClientError {
    Timeout,
    Io(io::Error),
    Codec(CodecError),
    NotLeader { leader_id: Option<NodeId> },
    NoEndpoints,
    ConnectionFailed(String),
    RpcFailed(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => write!(f, "Operation timed out"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Codec(e) => write!(f, "Codec error: {e}"),
            Self::NotLeader { leader_id } => {
                write!(f, "Node is not leader, leader is: {leader_id:?}")
            }
            Self::NoEndpoints => write!(f, "No target endpoints provided"),
            Self::ConnectionFailed(msg) => write!(f, "Connection failed: {msg}"),
            Self::RpcFailed(msg) => write!(f, "RPC failed: {msg}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<CodecError> for ClientError {
    fn from(err: CodecError) -> Self {
        Self::Codec(err)
    }
}
