use flotilla::engine::{OutboundMessage, RaftConfig, RaftNode};
use flotilla::types::{LogIndex, NodeId, Role};
use flotilla::udp::{UdpClusterRouter, UdpDriver};
use std::io::ErrorKind;

#[test]
fn test_3_node_cluster_udp_loopback() {
    // 1. Create UDP drivers on loopback
    let driver1 = UdpDriver::bind("127.0.0.1:0").expect("Bind node 1");
    let driver2 = UdpDriver::bind("127.0.0.1:0").expect("Bind node 2");
    let driver3 = UdpDriver::bind("127.0.0.1:0").expect("Bind node 3");

    driver1.set_nonblocking(true).unwrap();
    driver2.set_nonblocking(true).unwrap();
    driver3.set_nonblocking(true).unwrap();

    let addr1 = driver1.local_addr().unwrap();
    let addr2 = driver2.local_addr().unwrap();
    let addr3 = driver3.local_addr().unwrap();

    // 2. Setup router
    let mut router = UdpClusterRouter::new();
    router.register_peer(NodeId(1), addr1);
    router.register_peer(NodeId(2), addr2);
    router.register_peer(NodeId(3), addr3);

    // 3. Initialize Raft nodes (Node 1 has shorter election timeout to lead election)
    let config1 = RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(2), NodeId(3)],
        election_timeout_ticks: 3,
        heartbeat_interval_ticks: 2,
    };
    let config2 = RaftConfig {
        node_id: NodeId(2),
        peers: vec![NodeId(1), NodeId(3)],
        election_timeout_ticks: 15,
        heartbeat_interval_ticks: 2,
    };
    let config3 = RaftConfig {
        node_id: NodeId(3),
        peers: vec![NodeId(1), NodeId(2)],
        election_timeout_ticks: 15,
        heartbeat_interval_ticks: 2,
    };

    let mut node1 = RaftNode::<32, 64>::new(config1);
    let mut node2 = RaftNode::<32, 64>::new(config2);
    let mut node3 = RaftNode::<32, 64>::new(config3);

    let mut buf = [0u8; 1500];

    // Helper closure to pump UDP packets for a node
    let mut pump_packets = |node: &mut RaftNode<32, 64>, driver: &UdpDriver| {
        let mut outbound = Vec::new();
        loop {
            match driver.recv_from(&mut buf) {
                Ok((len, src_addr)) => {
                    if let Some(sender_id) = router.peer_id(&src_addr)
                        && let Ok(actions) = node.step(sender_id, &buf[..len])
                    {
                        outbound.extend(actions);
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => panic!("Socket recv error: {e}"),
            }
        }
        for act in outbound {
            if let OutboundMessage::SendPacket { to, packet } = act
                && let Some(dest_addr) = router.peer_addr(to)
            {
                let _ = driver.send_to(&packet, dest_addr);
            }
        }
    };

    // Helper closure to dispatch node's ticks
    let pump_ticks = |node: &mut RaftNode<32, 64>, driver: &UdpDriver| {
        let actions = node.tick();
        for act in actions {
            if let OutboundMessage::SendPacket { to, packet } = act
                && let Some(dest_addr) = router.peer_addr(to)
            {
                let _ = driver.send_to(&packet, dest_addr);
            }
        }
    };

    // 4. Run loop until leader is elected
    let mut elected = false;
    for _ in 0..50 {
        pump_ticks(&mut node1, &driver1);
        pump_ticks(&mut node2, &driver2);
        pump_ticks(&mut node3, &driver3);

        pump_packets(&mut node1, &driver1);
        pump_packets(&mut node2, &driver2);
        pump_packets(&mut node3, &driver3);

        if node1.role() == Role::Leader {
            elected = true;
            break;
        }
    }
    assert!(elected, "Node 1 should be elected leader over UDP network");

    // 5. Propose a command to the leader
    let cmd = b"client_cmd_over_udp";
    let proposed_idx = node1.propose(cmd).expect("Proposal should succeed");
    assert_eq!(proposed_idx, LogIndex(1));

    // 6. Pump network until command is committed across the cluster
    let mut committed = false;
    for _ in 0..50 {
        pump_ticks(&mut node1, &driver1);
        pump_ticks(&mut node2, &driver2);
        pump_ticks(&mut node3, &driver3);

        pump_packets(&mut node1, &driver1);
        pump_packets(&mut node2, &driver2);
        pump_packets(&mut node3, &driver3);

        if node1.commit_index() >= LogIndex(1) && node2.commit_index() >= LogIndex(1) {
            committed = true;
            break;
        }
    }

    assert!(committed, "Command should be replicated and committed via UDP");
    assert_eq!(node1.storage.entry_at(LogIndex(1)).unwrap().payload_bytes(), cmd);
    assert_eq!(node2.storage.entry_at(LogIndex(1)).unwrap().payload_bytes(), cmd);
}
