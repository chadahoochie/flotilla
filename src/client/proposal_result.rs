use crate::types::{LogIndex, NodeId, Term};

/// Outcome of a consensus proposal submission to the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalResult {
    pub success: bool,
    pub index: LogIndex,
    pub term: Term,
    pub leader_id: Option<NodeId>,
}

impl ProposalResult {
    /// Create a successful proposal result with the committed or accepted index.
    pub const fn success(index: LogIndex, term: Term, leader_id: NodeId) -> Self {
        Self {
            success: true,
            index,
            term,
            leader_id: Some(leader_id),
        }
    }

    /// Create a failed proposal result, indicating the current leader if known.
    pub const fn failure(leader_id: Option<NodeId>) -> Self {
        Self {
            success: false,
            index: LogIndex::ZERO,
            term: Term::ZERO,
            leader_id,
        }
    }

    /// Returns true if the proposal was successfully accepted by the cluster leader.
    pub const fn is_success(&self) -> bool {
        self.success
    }
}
