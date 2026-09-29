use crate::types::{LogIndex, NodeId};

/// Replication progress for a single peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerProgress {
    pub id: NodeId,
    pub match_index: LogIndex,
    pub next_index: LogIndex,
}
