use crate::types::{LogIndex, NodeId, Term};

/// Compute the majority quorum size required for consensus decisions in a cluster of `cluster_size` nodes.
#[inline(always)]
pub fn quorum_size(cluster_size: usize) -> usize {
    (cluster_size / 2) + 1
}

/// Check if the number of affirmative votes is sufficient to reach quorum.
#[inline(always)]
pub fn is_quorum_reached(votes_granted: usize, cluster_size: usize) -> bool {
    votes_granted >= quorum_size(cluster_size)
}

/// Determine if a candidate's log is at least as up-to-date as the receiver's log (Raft §5.4.1).
#[inline]
pub fn is_log_up_to_date(
    candidate_last_term: Term,
    candidate_last_index: LogIndex,
    local_last_term: Term,
    local_last_index: LogIndex,
) -> bool {
    if candidate_last_term != local_last_term {
        candidate_last_term.0 > local_last_term.0
    } else {
        candidate_last_index.0 >= local_last_index.0
    }
}

/// Determine if the receiver node is eligible to cast a vote for `candidate_id` in `candidate_term`.
#[inline]
pub fn is_vote_eligible(
    current_term: Term,
    voted_for: NodeId,
    candidate_term: Term,
    candidate_id: NodeId,
) -> bool {
    if candidate_term.0 < current_term.0 {
        false
    } else if candidate_term.0 > current_term.0 {
        true
    } else {
        voted_for == NodeId::NONE || voted_for == candidate_id
    }
}
