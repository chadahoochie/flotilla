use flotilla::commit::{calculate_quorum_commit_index, evaluate_commit_advancement};
use flotilla::types::{LogIndex, Term};

#[test]
fn test_quorum_median_calculation_3_nodes() {
    // 3 nodes: leader at 10, peer1 at 10, peer2 at 5
    let mut indices = [LogIndex(10), LogIndex(10), LogIndex(5)];
    let median = calculate_quorum_commit_index(&mut indices);
    assert_eq!(median, LogIndex(10));

    // 3 nodes: leader at 10, peer1 at 8, peer2 at 4
    let mut indices2 = [LogIndex(10), LogIndex(8), LogIndex(4)];
    let median2 = calculate_quorum_commit_index(&mut indices2);
    assert_eq!(median2, LogIndex(8));
}

#[test]
fn test_quorum_median_calculation_5_nodes() {
    // 5 nodes: [100, 100, 95, 20, 10] -> quorum at 95
    let mut indices = [
        LogIndex(100),
        LogIndex(100),
        LogIndex(95),
        LogIndex(20),
        LogIndex(10),
    ];
    let median = calculate_quorum_commit_index(&mut indices);
    assert_eq!(median, LogIndex(95));
}

#[test]
fn test_evaluate_commit_advancement_current_term_only() {
    let current_commit = LogIndex(10);
    let current_term = Term(2);

    let match_indices = [LogIndex(15), LogIndex(15), LogIndex(10)];

    // Case 1: Entry 15 is from current term 2 -> commit advances to 15
    let new_commit = evaluate_commit_advancement(
        &match_indices,
        current_commit,
        current_term,
        |idx| {
            if idx == LogIndex(15) {
                Some(Term(2))
            } else {
                Some(Term(1))
            }
        },
    );
    assert_eq!(new_commit, Some(LogIndex(15)));

    // Case 2: Entry 15 is from older term 1 -> Raft prohibits committing older terms directly!
    let blocked_commit = evaluate_commit_advancement(
        &match_indices,
        current_commit,
        current_term,
        |_idx| Some(Term(1)),
    );
    assert_eq!(blocked_commit, None);
}
