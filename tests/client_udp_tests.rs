#![cfg(feature = "client-udp")]

use flotilla_raft::client::udp::UdpClient;
use flotilla_raft::client::{ClientConfig, ClientError, FlotillaClient};
use flotilla_raft::engine::{RaftConfig, RaftNode};
use flotilla_raft::server::UdpListener;
use flotilla_raft::types::{LogIndex, NodeId, Role, Term};
use flotilla_raft::udp::UdpClusterRouter;
use std::time::Duration;

#[tokio::test]
async fn test_udp_client_propose_to_leader() {
    let mut router = UdpClusterRouter::new();
    let listener = UdpListener::bind("127.0.0.1:0", router.clone()).expect("Bind UDP listener");
    let server_addr = listener.local_addr().expect("Local addr");
    router.register_peer(NodeId(1), server_addr);

    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    };
    let mut node = RaftNode::<32, 64>::new(config);
    node.tick();
    assert_eq!(node.role(), Role::Leader);

    let client = UdpClient::connect(server_addr).expect("Connect client");

    let listener_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 1500];
        for _ in 0..20 {
            if let Ok(len) = listener.poll_and_step(&mut node, &mut buf)
                && len > 0
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        node
    });

    let res = client
        .propose(b"hello_udp_consensus")
        .await
        .expect("Propose success");
    assert!(res.is_success());
    assert_eq!(res.index, LogIndex(1));
    assert_eq!(res.term, Term(1));
    assert_eq!(res.leader_id, Some(NodeId(1)));

    let final_node = listener_thread.join().expect("Join thread");
    assert_eq!(final_node.last_log_index(), LogIndex(1));
    assert_eq!(
        final_node
            .storage
            .entry_at(LogIndex(1))
            .unwrap()
            .payload_bytes(),
        b"hello_udp_consensus"
    );
}

#[tokio::test]
async fn test_udp_client_propose_to_follower_fails() {
    let mut router = UdpClusterRouter::new();
    let listener = UdpListener::bind("127.0.0.1:0", router.clone()).expect("Bind UDP listener");
    let server_addr = listener.local_addr().expect("Local addr");
    router.register_peer(NodeId(2), server_addr);

    let config = RaftConfig {
        node_id: NodeId(2),
        peers: vec![NodeId(1)],
        election_timeout_ticks: 50,
        heartbeat_interval_ticks: 10,
    };
    let mut node = RaftNode::<32, 64>::new(config);
    assert_eq!(node.role(), Role::Follower);

    let client = UdpClient::connect(server_addr).expect("Connect client");

    let listener_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 1500];
        for _ in 0..20 {
            if let Ok(len) = listener.poll_and_step(&mut node, &mut buf)
                && len > 0
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    let res = client
        .propose(b"follower_cmd")
        .await
        .expect("Propose response");
    assert!(!res.is_success());

    listener_thread.join().expect("Join thread");
}

#[tokio::test]
async fn test_udp_client_config_and_ping() {
    let config = ClientConfig::new(vec!["127.0.0.1:0".to_string()])
        .with_timeout(Duration::from_millis(100))
        .with_retries(1);

    let empty_config = ClientConfig::new(vec![]);
    assert!(matches!(
        UdpClient::from_config(&empty_config),
        Err(ClientError::NoEndpoints)
    ));

    let mut client = UdpClient::from_config(&config).expect("Create from config");
    assert_eq!(client.timeout, Duration::from_millis(100));
    assert!(client.set_timeout(Duration::from_millis(200)).is_ok());
    assert_eq!(client.timeout, Duration::from_millis(200));

    // IPv6 and IPv4 connect branches across instantiations
    let _ = UdpClient::connect("[::1]:0");
    let _ = UdpClient::connect("127.0.0.1:0");
    if let Ok(ipv6_addr) = "[::1]:0".parse::<std::net::SocketAddr>() {
        let _ = UdpClient::connect(ipv6_addr);
    }
    if let Ok(ipv4_addr) = "127.0.0.1:0".parse::<std::net::SocketAddr>() {
        let _ = UdpClient::connect(ipv4_addr);
    }
    assert!(UdpClient::connect("not an address").is_err());
}

#[tokio::test]
async fn test_udp_client_ping_leader() {
    let mut router = UdpClusterRouter::new();
    let listener = UdpListener::bind("127.0.0.1:0", router.clone()).expect("Bind UDP listener");
    let server_addr = listener.local_addr().expect("Local addr");
    router.register_peer(NodeId(1), server_addr);

    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    };
    let mut node = RaftNode::<32, 64>::new(config);
    node.tick();
    assert_eq!(node.role(), Role::Leader);

    let client = UdpClient::connect(server_addr).expect("Connect client");

    let listener_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 1500];
        for _ in 0..20 {
            if let Ok(len) = listener.poll_and_step(&mut node, &mut buf)
                && len > 0
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    let ping_res = client.ping().await.expect("Ping success");
    assert!(ping_res);
    listener_thread.join().expect("Join thread");
}

#[tokio::test]
async fn test_udp_client_error_responses_from_server() {
    use flotilla_raft::codec::{
        CodecError, HEADER_SIZE, MAGIC, PROTOCOL_VERSION, PacketHeader, calculate_crc32,
        encode_client_proposal_reply, encode_packet_header,
    };
    use flotilla_raft::message::{ClientProposalReply, MsgType};
    use zerocopy::IntoBytes;

    let server_sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr = server_sock.local_addr().unwrap();
    let client = UdpClient::connect(server_addr).unwrap();

    let server_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 1500];

        // 1. Checksum mismatch reply
        let (_len, src) = server_sock.recv_from(&mut buf).unwrap();
        let reply = ClientProposalReply {
            success: 1,
            _pad: [0; 7],
            index: LogIndex(1),
            term: Term(1),
            leader_id: NodeId(1),
        };
        let payload = reply.as_bytes();
        let bad_crc_hdr = PacketHeader {
            magic: MAGIC,
            version: PROTOCOL_VERSION,
            msg_type: MsgType::ClientProposalReply as u16,
            sender_id: NodeId(1),
            receiver_id: NodeId(0),
            term: Term(1),
            checksum: 0xDEADBEEF,
            payload_len: payload.len() as u32,
        };
        let mut pkt1 = [0u8; HEADER_SIZE + 32];
        encode_packet_header(&mut pkt1, &bad_crc_hdr).unwrap();
        pkt1[HEADER_SIZE..].copy_from_slice(payload);
        server_sock.send_to(&pkt1, src).unwrap();

        // 2. Unexpected message type (e.g. RequestVoteArgs)
        let (_len, src) = server_sock.recv_from(&mut buf).unwrap();
        let wrong_type_hdr = PacketHeader {
            magic: MAGIC,
            version: PROTOCOL_VERSION,
            msg_type: MsgType::RequestVoteArgs as u16,
            sender_id: NodeId(1),
            receiver_id: NodeId(0),
            term: Term(1),
            checksum: calculate_crc32(payload),
            payload_len: payload.len() as u32,
        };
        let mut pkt2 = [0u8; HEADER_SIZE + 32];
        encode_packet_header(&mut pkt2, &wrong_type_hdr).unwrap();
        pkt2[HEADER_SIZE..].copy_from_slice(payload);
        server_sock.send_to(&pkt2, src).unwrap();

        // 3. Short payload (< 32 bytes)
        let (_len, src) = server_sock.recv_from(&mut buf).unwrap();
        let short_hdr = PacketHeader {
            magic: MAGIC,
            version: PROTOCOL_VERSION,
            msg_type: MsgType::ClientProposalReply as u16,
            sender_id: NodeId(1),
            receiver_id: NodeId(0),
            term: Term(1),
            checksum: calculate_crc32(&[0u8; 4]),
            payload_len: 4,
        };
        let mut pkt3 = [0u8; HEADER_SIZE + 4];
        encode_packet_header(&mut pkt3, &short_hdr).unwrap();
        server_sock.send_to(&pkt3, src).unwrap();

        // 4. Failure reply with known leader_id != 0
        let (_len, src) = server_sock.recv_from(&mut buf).unwrap();
        let fail_reply = ClientProposalReply {
            success: 0,
            _pad: [0; 7],
            index: LogIndex::ZERO,
            term: Term(1),
            leader_id: NodeId(42),
        };
        let mut pkt4 = [0u8; HEADER_SIZE + 32];
        let rep_len =
            encode_client_proposal_reply(&mut pkt4, NodeId(1), NodeId(0), Term(1), &fail_reply)
                .unwrap();
        server_sock.send_to(&pkt4[..rep_len], src).unwrap();
    });

    // 1. Checksum mismatch
    let res1 = client.propose(b"bad_crc").await;
    assert!(matches!(
        res1,
        Err(ClientError::Codec(CodecError::ChecksumMismatch { .. }))
    ));

    // 2. Wrong message type
    let res2 = client.propose(b"wrong_type").await;
    assert!(matches!(res2, Err(ClientError::RpcFailed(_))));

    // 3. Short payload serialization error
    let res3 = client.propose(b"short").await;
    assert!(matches!(
        res3,
        Err(ClientError::Codec(CodecError::SerializationError))
    ));

    // 4. Failure with known leader
    let res4 = client.propose(b"redir").await.unwrap();
    assert!(!res4.is_success());
    assert_eq!(res4.leader_id, Some(NodeId(42)));

    server_thread.join().unwrap();
}

#[test]
fn test_udp_listener_branches_and_routing() {
    use flotilla_raft::engine::{create_append_entries_packet, create_append_entries_reply_packet};
    use flotilla_raft::message::AppendEntriesHeader;

    let mut router = UdpClusterRouter::new();
    let listener = UdpListener::bind("127.0.0.1:0", router.clone()).expect("Bind listener");
    let server_addr = listener.local_addr().expect("Local addr");
    assert_ne!(server_addr.port(), 0);

    // 1. Invalid bind address -> Err
    assert!(UdpListener::bind("999.999.999.999:9999", router.clone()).is_err());

    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(2)],
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 2,
    };
    let mut node = RaftNode::<32, 64>::new(config);

    // 2. Nonblocking recv_from when no packet waiting -> Err (WouldBlock)
    listener.driver.set_nonblocking(true).unwrap();
    let mut buf = [0u8; 1500];
    let err = listener.poll_and_step(&mut node, &mut buf).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    listener.driver.set_nonblocking(false).unwrap();

    // 3. Packet with invalid bytes -> node.step() returns Err
    let client_sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .send_to(b"corrupted_packet_data", server_addr)
        .unwrap();
    let len = listener.poll_and_step(&mut node, &mut buf).unwrap();
    assert_eq!(len, b"corrupted_packet_data".len());

    // 4. Packet that produces OutboundMessage::SendPacket to a peer registered in router
    let client_addr = client_sock.local_addr().unwrap();
    router.register_peer(NodeId(2), client_addr);
    let listener2 = UdpListener::bind("127.0.0.1:0", router).unwrap();
    let server_addr2 = listener2.local_addr().unwrap();

    node.election.role = Role::Leader;
    let reply_pkt =
        create_append_entries_reply_packet(NodeId(2), NodeId(1), Term(1), true, LogIndex(1))
            .unwrap();
    client_sock.send_to(&reply_pkt, server_addr2).unwrap();
    let len2 = listener2.poll_and_step(&mut node, &mut buf).unwrap();
    assert!(len2 > 0);

    // 5. Follower receives AppendEntries causing ApplyEntries message (non-SendPacket act)
    let mut follower_node = RaftNode::<32, 64>::new(RaftConfig {
        node_id: NodeId(2),
        peers: vec![NodeId(1)],
        election_timeout_ticks: 50,
        heartbeat_interval_ticks: 10,
    });
    let _ = follower_node
        .storage
        .append_entry(Term(1), b"data")
        .unwrap();
    let ae_hdr = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(1),
        leader_commit: LogIndex(1),
        entries_count: 0,
        _pad: [0; 4],
    };
    let ae_pkt = create_append_entries_packet(NodeId(2), &ae_hdr, &[]).unwrap();
    client_sock.send_to(&ae_pkt, server_addr2).unwrap();
    let len3 = listener2
        .poll_and_step(&mut follower_node, &mut buf)
        .unwrap();
    assert!(len3 > 0);
    assert_eq!(follower_node.commit_index(), LogIndex(1));

    // 6. Packet where outbound action destination is not in router and not sender_id
    let mut node_with_unrouted_peer = RaftNode::<32, 64>::new(RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(99)],
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 2,
    });
    node_with_unrouted_peer.election.role = Role::Leader;
    let mut prop_buf = [0u8; 128];
    let plen = flotilla_raft::codec::encode_client_proposal(
        &mut prop_buf,
        NodeId(0),
        NodeId(1),
        Term(0),
        b"unrouted_peer_test",
    )
    .unwrap();
    let empty_router = UdpClusterRouter::new();
    let unrouted_listener = UdpListener::bind("127.0.0.1:0", empty_router).unwrap();
    let unrouted_addr = unrouted_listener.local_addr().unwrap();
    client_sock
        .send_to(&prop_buf[..plen], unrouted_addr)
        .unwrap();
    let len4 = unrouted_listener
        .poll_and_step(&mut node_with_unrouted_peer, &mut buf)
        .unwrap();
    assert!(len4 > 0);
}
