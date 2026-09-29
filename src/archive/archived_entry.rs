use crate::types::{LogIndex, Term};

/// Single committed entry offloaded for persistent archival.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedEntry {
    pub index: LogIndex,
    pub term: Term,
    pub payload: Vec<u8>,
}
