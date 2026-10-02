#![cfg(feature = "grpc")]

use flotilla_raft::client::grpc::GrpcClient;
use flotilla_raft::client::{ClientConfig, ClientError, FlotillaClient};
use flotilla_raft::engine::{RaftConfig, RaftNode};
use flotilla_raft::server::GrpcService;
use flotilla_raft::server::proto::flotilla_service_client::FlotillaServiceClient;
use flotilla_raft::server::proto::{StatusRequest, StepRequest};
use flotilla_raft::types::{LogIndex, NodeId, Role, Term};
use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

async fn start_test_grpc_server<const C: usize, const M: usize>(
    node: Arc<Mutex<RaftNode<C, M>>>,
) -> SocketAddr {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Bind ephemeral port");
    let addr = std_listener.local_addr().expect("Local addr");
    drop(std_listener);

    let service = GrpcService::new(node);
    tokio::spawn(async move {
        let _ = service.serve(addr).await;
    });

    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    addr
}

#[tokio::test]
async fn test_grpc_client_propose_to_leader() {
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
    let addr = start_test_grpc_server(Arc::clone(&node)).await;

    let client = GrpcClient::connect(format!("http://{addr}"));
    let res = client
        .propose(b"grpc_proposal_cmd")
        .await
        .expect("Propose via gRPC");

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
        b"grpc_proposal_cmd"
    );
}

#[tokio::test]
async fn test_grpc_client_status_and_ping() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    };
    let mut raw_node = RaftNode::<32, 64>::new(config);
    raw_node.tick();

    let node = Arc::new(Mutex::new(raw_node));
    let addr = start_test_grpc_server(Arc::clone(&node)).await;

    let client = GrpcClient::connect(format!("http://{addr}"));
    let ping_res = client.ping().await.expect("Ping success");
    assert!(ping_res);

    let mut rpc_client = FlotillaServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("Connect raw rpc client");

    let status = rpc_client
        .cluster_status(tonic::Request::new(StatusRequest {}))
        .await
        .expect("Get cluster status")
        .into_inner();

    assert_eq!(status.node_id, 1);
    assert_eq!(status.role, "Leader");
    assert_eq!(status.term, 1);
}

#[tokio::test]
async fn test_grpc_client_follower_rejection() {
    let config = RaftConfig {
        node_id: NodeId(2),
        peers: vec![NodeId(1)],
        election_timeout_ticks: 50,
        heartbeat_interval_ticks: 10,
    };
    let raw_node = RaftNode::<32, 64>::new(config);
    assert_eq!(raw_node.role(), Role::Follower);

    let node = Arc::new(Mutex::new(raw_node));
    let addr = start_test_grpc_server(Arc::clone(&node)).await;

    let client = GrpcClient::connect(format!("http://{addr}"));
    let res = client
        .propose(b"follower_grpc_cmd")
        .await
        .expect("Propose response");

    assert!(!res.is_success());
}

#[tokio::test]
async fn test_grpc_service_step_rpc() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 5,
    };
    let raw_node = RaftNode::<32, 64>::new(config);
    let node = Arc::new(Mutex::new(raw_node));
    let addr = start_test_grpc_server(Arc::clone(&node)).await;

    let mut rpc_client = FlotillaServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("Connect raw rpc client");

    // Send dummy packet through step
    let step_req = StepRequest {
        sender_id: 2,
        packet: vec![0u8; 40],
    };
    // Should return Err status because header is invalid
    let step_res = rpc_client.step(tonic::Request::new(step_req)).await;
    assert!(step_res.is_err());
}

#[tokio::test]
async fn test_grpc_client_config_edge_cases() {
    let empty_config = ClientConfig::new(vec![]);
    assert!(matches!(
        GrpcClient::from_config(&empty_config),
        Err(ClientError::NoEndpoints)
    ));

    let config = ClientConfig::new(vec!["http://127.0.0.1:9090".to_string()])
        .with_timeout(Duration::from_millis(50));
    let mut client = GrpcClient::from_config(&config).expect("From config");
    client.set_timeout(Duration::from_millis(25));
    assert_eq!(client.timeout, Duration::from_millis(25));
}
