#![cfg(feature = "tcp")]

use flotilla_raft::client::tcp::TcpClient;
use flotilla_raft::client::tcp::framing::{read_packet_frame, write_packet_frame};
use flotilla_raft::client::{ClientConfig, ClientError, FlotillaClient};
use flotilla_raft::engine::{RaftConfig, RaftNode};
use flotilla_raft::server::TcpListener;
use flotilla_raft::types::{LogIndex, NodeId, Role, Term};
use parking_lot::Mutex;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_tcp_client_propose_to_leader() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    };
    let mut raw_node = RaftNode::<32, 64>::new(config);
    raw_node.tick();
    assert_eq!(raw_node.role(), Role::Leader);

    let node = Arc::new(Mutex::new(raw_node));
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), Arc::clone(&node))
        .await
        .expect("Bind TCP listener");
    let server_addr = listener.local_addr();

    let client = TcpClient::connect(server_addr.to_string());
    let res = client
        .propose(b"tcp_consensus_cmd")
        .await
        .expect("Propose over TCP");

    assert!(res.is_success());
    assert_eq!(res.index, LogIndex(1));
    assert_eq!(res.term, Term(1));
    assert_eq!(res.leader_id, Some(NodeId(1)));

    let locked = node.lock();
    assert_eq!(locked.last_log_index(), LogIndex(1));
    assert_eq!(
        locked
            .storage
            .entry_at(LogIndex(1))
            .unwrap()
            .payload_bytes(),
        b"tcp_consensus_cmd"
    );
}

#[tokio::test]
async fn test_tcp_client_multiple_proposals() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    };
    let mut raw_node = RaftNode::<32, 64>::new(config);
    raw_node.tick();

    let node = Arc::new(Mutex::new(raw_node));
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), Arc::clone(&node))
        .await
        .expect("Bind TCP listener");
    let server_addr = listener.local_addr();

    let client = TcpClient::connect(server_addr.to_string());

    for i in 1..=5 {
        let payload = format!("item_{i}");
        let res = client
            .propose(payload.as_bytes())
            .await
            .expect("Propose item");
        assert!(res.is_success());
        assert_eq!(res.index, LogIndex(i));
    }

    let locked = node.lock();
    assert_eq!(locked.last_log_index(), LogIndex(5));
}

#[tokio::test]
async fn test_tcp_client_follower_rejection() {
    let config = RaftConfig {
        node_id: NodeId(2),
        peers: vec![NodeId(1)],
        election_timeout_ticks: 50,
        heartbeat_interval_ticks: 10,
    };
    let raw_node = RaftNode::<32, 64>::new(config);
    assert_eq!(raw_node.role(), Role::Follower);

    let node = Arc::new(Mutex::new(raw_node));
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), Arc::clone(&node))
        .await
        .expect("Bind TCP listener");
    let server_addr = listener.local_addr();

    let client = TcpClient::connect(server_addr.to_string());
    let res = client
        .propose(b"follower_tcp_cmd")
        .await
        .expect("Propose response");
    assert!(!res.is_success());
}

#[tokio::test]
async fn test_tcp_client_config_and_ping() {
    let config =
        ClientConfig::new(vec!["127.0.0.1:0".to_string()]).with_timeout(Duration::from_millis(200));

    let empty = ClientConfig::new(vec![]);
    assert!(matches!(
        TcpClient::from_config(&empty),
        Err(ClientError::NoEndpoints)
    ));

    let mut client = TcpClient::from_config(&config).expect("From config");
    client.set_timeout(Duration::from_millis(50));
    assert_eq!(client.timeout, Duration::from_millis(50));

    // Ping non-existent endpoint fails gracefully
    assert!(client.ping().await.is_err());
}

#[tokio::test]
async fn test_tcp_framing_validation() {
    let bad_magic_bytes = [0u8; 40];
    let mut cursor = Cursor::new(bad_magic_bytes);
    let mut buf = [0u8; 100];
    let res = read_packet_frame(&mut cursor, &mut buf).await;
    assert!(res.is_err());

    let mut out = Vec::new();
    write_packet_frame(&mut out, b"framed_packet_test")
        .await
        .expect("Write frame");
    assert_eq!(out, b"framed_packet_test");
}

#[tokio::test]
async fn test_tcp_client_propose_to_leader_with_peers() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(2), NodeId(3)],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    };
    let mut raw_node = RaftNode::<32, 64>::new(config);
    raw_node.election.role = Role::Leader;
    raw_node.election.current_term = Term(1);

    let node = Arc::new(Mutex::new(raw_node));
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), Arc::clone(&node))
        .await
        .expect("Bind TCP listener");
    let server_addr = listener.local_addr();

    let client = TcpClient::connect(server_addr.to_string());
    let res = client
        .propose(b"cmd_with_peers")
        .await
        .expect("Propose with peers");

    assert!(res.is_success());
    assert_eq!(res.index, LogIndex(1));
}

#[tokio::test]
async fn test_tcp_subscriber_stream_commits() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    };
    let mut raw_node = RaftNode::<32, 64>::new(config);
    raw_node.tick();
    assert_eq!(raw_node.role(), Role::Leader);

    let node = Arc::new(Mutex::new(raw_node));
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), Arc::clone(&node))
        .await
        .expect("Bind TCP listener");
    let server_addr = listener.local_addr();

    // 1. Connect a subscriber via TCP
    let mut sub_stream = tokio::net::TcpStream::connect(server_addr)
        .await
        .expect("Connect subscriber");

    // 2. Send HeartbeatArgs handshake frame
    use flotilla_raft::codec::{HEADER_SIZE, MAGIC, PROTOCOL_VERSION, PacketHeader, decode_packet};
    use flotilla_raft::message::MsgType;

    let handshake = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: MsgType::HeartbeatArgs as u16,
        sender_id: NodeId(0),
        receiver_id: NodeId(1),
        term: Term(1),
        checksum: 0,
        payload_len: 0,
    };
    let mut handshake_buf = [0u8; HEADER_SIZE];
    flotilla_raft::codec::encode_packet_header(&mut handshake_buf, &handshake).unwrap();
    write_packet_frame(&mut sub_stream, &handshake_buf).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // 3. Propose a command via separate client
    let client = TcpClient::connect(server_addr.to_string());
    let res = client.propose(b"subscriber_test_payload").await.expect("Propose");
    assert!(res.is_success());

    // 4. Subscriber should receive AppendEntriesArgs frame with committed entry
    let mut recv_buf = [0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(2), read_packet_frame(&mut sub_stream, &mut recv_buf))
        .await
        .expect("Timeout waiting for commit frame")
        .expect("Read commit frame");

    let (hdr, payload) = decode_packet(&recv_buf[..n]).expect("Decode commit packet");
    assert_eq!(hdr.msg_type, MsgType::AppendEntriesArgs as u16);
    assert_eq!(hdr.sender_id, NodeId(1));
    assert!(payload.len() >= 16);

    let log_idx = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let term_val = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    assert_eq!(log_idx, 1);
    assert_eq!(term_val, 1);
    assert_eq!(&payload[16..], b"subscriber_test_payload");
}
