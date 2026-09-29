use flotilla::engine::{OutboundMessage, RaftConfig, RaftNode};
use flotilla::types::{LogIndex, NodeId, Role, Term};

#[test]
fn test_single_node_cluster_elects_itself() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 2,
        heartbeat_interval_ticks: 1,
    };
    let mut node = RaftNode::<16, 64>::new(config);
    assert_eq!(node.role(), Role::Follower);

    // Ticks trigger campaign
    node.tick();
    let msgs = node.tick();
    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.current_term(), Term(1));
    assert!(msgs.is_empty(), "No peers to send messages to");
}

#[test]
fn test_3_node_cluster_election_and_proposal() {
    let config1 = RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(2), NodeId(3)],
        election_timeout_ticks: 2,
        heartbeat_interval_ticks: 1,
    };
    let config2 = RaftConfig {
        node_id: NodeId(2),
        peers: vec![NodeId(1), NodeId(3)],
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 1,
    };
    let config3 = RaftConfig {
        node_id: NodeId(3),
        peers: vec![NodeId(1), NodeId(2)],
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 1,
    };

    let mut node1 = RaftNode::<16, 64>::new(config1);
    let mut node2 = RaftNode::<16, 64>::new(config2);
    let mut node3 = RaftNode::<16, 64>::new(config3);

    // Node 1 times out and campaigns
    node1.tick();
    let msgs1 = node1.tick();
    assert_eq!(node1.role(), Role::Candidate);
    assert_eq!(msgs1.len(), 2, "Candidate should send RequestVote to 2 peers");

    // Deliver vote requests to Node 2 and Node 3
    for msg in msgs1 {
        match msg {
            OutboundMessage::SendPacket { to, packet } => {
                if to == NodeId(2) {
                    let replies = node2.step(NodeId(1), &packet).unwrap();
                    assert_eq!(replies.len(), 1);
                    // Deliver reply back to Node 1
                    if let OutboundMessage::SendPacket { packet: reply_pkt, .. } = &replies[0] {
                        let n1_actions = node1.step(NodeId(2), reply_pkt).unwrap();
                        // Node 1 should now be leader because it has 2 votes (self + node 2)
                        assert_eq!(node1.role(), Role::Leader);
                        assert!(!n1_actions.is_empty(), "Leader should immediately send heartbeats");
                    }
                } else if to == NodeId(3) {
                    let replies = node3.step(NodeId(1), &packet).unwrap();
                    assert_eq!(replies.len(), 1);
                }
            }
            _ => panic!("Expected SendPacket"),
        }
    }

    assert_eq!(node1.role(), Role::Leader);

    // Leader proposes a command
    let prop_idx = node1.propose(b"put key=value").expect("Leader can propose");
    assert_eq!(prop_idx, LogIndex(1));
    assert_eq!(node1.last_log_index(), LogIndex(1));

    // Replicate entry to follower Node 2
    let heartbeat_msgs = node1.tick(); // Heartbeat tick triggers replication
    for msg in heartbeat_msgs {
        if let OutboundMessage::SendPacket { to, packet } = msg
            && to == NodeId(2)
        {
            let replies = node2.step(NodeId(1), &packet).unwrap();
            assert_eq!(replies.len(), 1);
            if let OutboundMessage::SendPacket { packet: reply_pkt, .. } = &replies[0] {
                let leader_replies = node1.step(NodeId(2), reply_pkt).unwrap();
                // Majority (Node 1 + Node 2) has replicated -> Entry 1 committed!
                assert_eq!(node1.commit_index(), LogIndex(1));
                assert!(leader_replies.iter().any(|m| matches!(m, OutboundMessage::ApplyEntries { from_index, to_index } if *from_index == LogIndex(1) && *to_index == LogIndex(1))));
            }
        }
    }
}
