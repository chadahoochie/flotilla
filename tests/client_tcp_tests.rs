#![cfg(feature = "tcp")]

use flotilla::client::tcp::TcpClient;
use flotilla::client::tcp::framing::{read_packet_frame, write_packet_frame};
use flotilla::client::{ClientConfig, ClientError, FlotillaClient};
use flotilla::engine::{RaftConfig, RaftNode};
use flotilla::server::TcpListener;
use flotilla::types::{LogIndex, NodeId, Role, Term};
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
