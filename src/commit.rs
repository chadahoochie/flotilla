use crate::types::{LogIndex, Term};

/// Calculate the median match index across cluster nodes representing the highest index replicated on a majority of nodes.
///
/// Given a mutable slice of `match_indices` (containing the leader's own last index and all followers' match indices):
/// 1. Sorts the slice in descending order.
/// 2. Returns the element at `(len - 1) / 2`.
///
/// # Examples:
/// - 3-node cluster: sorted indices `[10, 10, 5]` -> index `(3-1)/2 = 1` -> `LogIndex(10)`. (At least 2 nodes have $\ge 10$).
/// - 5-node cluster: sorted indices `[100, 100, 95, 20, 10]` -> index `(5-1)/2 = 2` -> `LogIndex(95)`. (At least 3 nodes have $\ge 95$).
#[inline]
pub fn calculate_quorum_commit_index(match_indices: &mut [LogIndex]) -> LogIndex {
    assert!(!match_indices.is_empty(), "match_indices must not be empty");
    match_indices.sort_unstable_by(|a, b| b.cmp(a));
    let quorum_pos = (match_indices.len() - 1) / 2;
    match_indices[quorum_pos]
}

/// Evaluates if the leader's commit index can advance according to Raft §5.3 and §5.4.2.
///
/// Invariants enforced:
/// 1. $N > \text{current\_commit}$
/// 2. A majority of $\text{match\_index}\[i\] \ge N$
/// 3. The entry at $N$ was written during the leader's current term ($\text{term}(N) == \text{current\_term}$)
pub fn evaluate_commit_advancement<F>(
    match_indices: &[LogIndex],
    current_commit: LogIndex,
    current_term: Term,
    term_lookup: F,
) -> Option<LogIndex>
where
    F: Fn(LogIndex) -> Option<Term>,
{
    if match_indices.is_empty() {
        return None;
    }

    // Use a stack buffer for clusters up to 16 nodes to guarantee zero heap allocations
    let mut scratch = [LogIndex::ZERO; 16];
    let n = match_indices.len();
    let slice = if n <= scratch.len() {
        scratch[..n].copy_from_slice(match_indices);
        &mut scratch[..n]
    } else {
        // Fallback for huge test clusters
        return None;
    };

    let candidate_commit = calculate_quorum_commit_index(slice);

    if candidate_commit.0 > current_commit.0 && term_lookup(candidate_commit) == Some(current_term)
    {
        return Some(candidate_commit);
    }

    None
}
