use flotilla::client::udp::UdpClient;
use flotilla::client::{ClientConfig, ClientError, FlotillaClient};
use flotilla::engine::{RaftConfig, RaftNode};
use flotilla::server::UdpListener;
use flotilla::types::{LogIndex, NodeId, Role, Term};
use flotilla::udp::UdpClusterRouter;
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

    let client = UdpClient::from_config(&config).expect("Create from config");
    assert_eq!(client.timeout, Duration::from_millis(100));
}
