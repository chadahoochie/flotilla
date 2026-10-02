use flotilla_raft::message::AppendEntriesHeader;
use flotilla_raft::replication::{
    FollowerAppendResult, PeerProgressTracker, evaluate_follower_append_entries,
};
use flotilla_raft::storage::ring_buffer::RingBufferLogStorage;
use flotilla_raft::types::{LogIndex, NodeId, Term};

#[test]
fn test_peer_progress_tracker_updates() {
    let peers = vec![NodeId(2), NodeId(3)];
    let mut tracker = PeerProgressTracker::new(&peers, LogIndex(10));

    assert_eq!(tracker.get_match_index(NodeId(2)), LogIndex(0));
    assert_eq!(tracker.get_next_index(NodeId(2)), LogIndex(11));

    // Peer 2 responds successfully with match index 10
    tracker.record_success(NodeId(2), LogIndex(10));
    assert_eq!(tracker.get_match_index(NodeId(2)), LogIndex(10));
    assert_eq!(tracker.get_next_index(NodeId(2)), LogIndex(11));

    // Peer 3 rejects -> decrement next index
    let decr = tracker.record_rejection(NodeId(3));
    assert_eq!(decr, LogIndex(10));
    assert_eq!(tracker.get_next_index(NodeId(3)), LogIndex(10));
}

#[test]
fn test_follower_accepts_valid_append_entries() {
    let mut storage = RingBufferLogStorage::<16, 64>::new();
    // Storage has 2 entries in term 1
    storage.append_entry(Term(1), b"e1").unwrap();
    storage.append_entry(Term(1), b"e2").unwrap();

    let header = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(2),
        prev_log_term: Term(1),
        leader_commit: LogIndex(2),
        entries_count: 1,
        _pad: [0; 4],
    };

    let entries_raw = b"e3";
    let mut current_commit = LogIndex(2);

    let result =
        evaluate_follower_append_entries(&header, entries_raw, &mut storage, &mut current_commit);

    assert_eq!(
        result,
        FollowerAppendResult::Success {
            match_index: LogIndex(3)
        }
    );
    assert_eq!(storage.last_index(), LogIndex(3));
    assert_eq!(current_commit, LogIndex(2));
}

#[test]
fn test_follower_rejects_log_gap_or_term_mismatch() {
    let mut storage = RingBufferLogStorage::<16, 64>::new();
    storage.append_entry(Term(1), b"e1").unwrap();

    // Leader assumes follower has up to index 5
    let header_gap = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(5),
        prev_log_term: Term(1),
        leader_commit: LogIndex(1),
        entries_count: 1,
        _pad: [0; 4],
    };

    let mut current_commit = LogIndex(1);
    let result_gap =
        evaluate_follower_append_entries(&header_gap, b"e6", &mut storage, &mut current_commit);

    assert_eq!(result_gap, FollowerAppendResult::Rejected);

    // Leader has term mismatch at prev_log_index 1 (term 2 vs term 1)
    let header_mismatch = AppendEntriesHeader {
        term: Term(2),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(2), // Mismatch! Local is Term 1
        leader_commit: LogIndex(1),
        entries_count: 0,
        _pad: [0; 4],
    };

    let result_mismatch =
        evaluate_follower_append_entries(&header_mismatch, b"", &mut storage, &mut current_commit);

    assert_eq!(result_mismatch, FollowerAppendResult::Rejected);
}
