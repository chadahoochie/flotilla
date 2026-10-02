use flotilla_raft::election::{
    ElectionAction, ElectionConfig, ElectionState, is_log_up_to_date, is_quorum_reached,
    is_vote_eligible, quorum_size,
};
use flotilla_raft::message::{RequestVoteArgs, RequestVoteReply};
use flotilla_raft::types::{LogIndex, NodeId, Role, Term};

#[test]
fn test_quorum_calculation() {
    assert_eq!(quorum_size(1), 1);
    assert_eq!(quorum_size(2), 2);
    assert_eq!(quorum_size(3), 2);
    assert_eq!(quorum_size(4), 3);
    assert_eq!(quorum_size(5), 3);
    assert_eq!(quorum_size(7), 4);

    assert!(is_quorum_reached(2, 3));
    assert!(!is_quorum_reached(1, 3));
    assert!(is_quorum_reached(3, 5));
    assert!(!is_quorum_reached(2, 5));
}

#[test]
fn test_log_up_to_date_rule() {
    // Higher term wins regardless of index
    assert!(is_log_up_to_date(
        Term(2),
        LogIndex(1),
        Term(1),
        LogIndex(100)
    ));
    assert!(!is_log_up_to_date(
        Term(1),
        LogIndex(100),
        Term(2),
        LogIndex(1)
    ));

    // Same term: longer or equal log wins
    assert!(is_log_up_to_date(
        Term(2),
        LogIndex(5),
        Term(2),
        LogIndex(4)
    ));
    assert!(is_log_up_to_date(
        Term(2),
        LogIndex(5),
        Term(2),
        LogIndex(5)
    ));
    assert!(!is_log_up_to_date(
        Term(2),
        LogIndex(4),
        Term(2),
        LogIndex(5)
    ));
}

#[test]
fn test_vote_eligibility() {
    // Lower term cannot get vote
    assert!(!is_vote_eligible(Term(2), NodeId::NONE, Term(1), NodeId(2)));

    // Fresh term with no previous vote is eligible
    assert!(is_vote_eligible(Term(1), NodeId::NONE, Term(1), NodeId(2)));

    // Same term: already voted for someone else
    assert!(!is_vote_eligible(Term(1), NodeId(3), Term(1), NodeId(2)));

    // Same term: already voted for this same candidate
    assert!(is_vote_eligible(Term(1), NodeId(2), Term(1), NodeId(2)));
}

#[test]
fn test_election_timeout_triggers_campaign() {
    let config = ElectionConfig {
        node_id: NodeId(1),
        cluster_size: 3,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
    };
    let mut state = ElectionState::new(config);

    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.current_term, Term(0));

    // 4 ticks: no timeout yet
    for _ in 0..4 {
        let action = state.handle_tick();
        assert_eq!(action, ElectionAction::None);
        assert_eq!(state.role, Role::Follower);
    }

    // 5th tick: timeout fires!
    let action = state.handle_tick();
    assert_eq!(action, ElectionAction::Campaign);
    assert_eq!(state.role, Role::Candidate);
    assert_eq!(state.current_term, Term(1));
    assert_eq!(state.voted_for, NodeId(1));
    assert_eq!(state.votes_granted, 1);
}

#[test]
fn test_candidate_wins_election_on_quorum() {
    let config = ElectionConfig {
        node_id: NodeId(1),
        cluster_size: 3,
        election_timeout_ticks: 2,
        heartbeat_interval_ticks: 1,
    };
    let mut state = ElectionState::new(config);

    // Timeout and start campaign
    state.handle_tick();
    state.handle_tick();
    assert_eq!(state.role, Role::Candidate);
    assert_eq!(state.votes_granted, 1);

    // Receive negative vote from peer 2
    let neg_reply = RequestVoteReply {
        term: Term(1),
        vote_granted: 0,
        _pad: [0; 7],
    };
    let action = state.handle_vote_reply(NodeId(2), &neg_reply);
    assert_eq!(action, ElectionAction::None);
    assert_eq!(state.role, Role::Candidate);

    // Receive affirmative vote from peer 3 -> Quorum of 2 reached!
    let pos_reply = RequestVoteReply {
        term: Term(1),
        vote_granted: 1,
        _pad: [0; 7],
    };
    let action = state.handle_vote_reply(NodeId(3), &pos_reply);
    assert_eq!(action, ElectionAction::ElectedLeader);
    assert_eq!(state.role, Role::Leader);
}

#[test]
fn test_higher_term_causes_step_down() {
    let config = ElectionConfig {
        node_id: NodeId(1),
        cluster_size: 3,
        election_timeout_ticks: 2,
        heartbeat_interval_ticks: 1,
    };
    let mut state = ElectionState::new(config);

    // Advance to candidate in term 1
    state.handle_tick();
    state.handle_tick();
    assert_eq!(state.role, Role::Candidate);
    assert_eq!(state.current_term, Term(1));

    // Message arrives with term 3
    let action = state.check_peer_term(Term(3));
    assert_eq!(action, ElectionAction::StepDown(Term(3)));
    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.current_term, Term(3));
    assert_eq!(state.voted_for, NodeId::NONE);
}

#[test]
fn test_handle_request_vote_args() {
    let config = ElectionConfig {
        node_id: NodeId(2),
        cluster_size: 3,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
    };
    let mut state = ElectionState::new(config);

    let vote_req = RequestVoteArgs {
        term: Term(1),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(5),
        last_log_term: Term(1),
    };

    // Follower has empty log (term 0, index 0)
    let reply = state.handle_request_vote(&vote_req, Term(0), LogIndex(0));
    assert_eq!(reply.term, Term(1));
    assert_eq!(reply.vote_granted, 1);
    assert_eq!(state.voted_for, NodeId(1));

    // Subsequent request in same term from different candidate should be rejected
    let vote_req_other = RequestVoteArgs {
        term: Term(1),
        candidate_id: NodeId(3),
        last_log_index: LogIndex(5),
        last_log_term: Term(1),
    };
    let reply_other = state.handle_request_vote(&vote_req_other, Term(0), LogIndex(0));
    assert_eq!(reply_other.vote_granted, 0);
}
