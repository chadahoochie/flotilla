//! Flotilla consensus server daemon executable.
//!
//! Provides an integrated multi-transport server node (UDP, TCP, and gRPC)
//! driving a high-throughput, sans-I/O Raft state machine.

use flotilla_raft::engine::{OutboundMessage, RaftConfig, RaftNode};
use flotilla_raft::server::{GrpcService, TcpListener, UdpListener};
use flotilla_raft::types::NodeId;
use flotilla_raft::udp::UdpClusterRouter;
use parking_lot::Mutex;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

struct ServerSettings {
    node_id: NodeId,
    udp_addr: SocketAddr,
    tcp_addr: SocketAddr,
    grpc_addr: SocketAddr,
    peers: Vec<(NodeId, SocketAddr)>,
    election_timeout_ticks: u32,
    heartbeat_interval_ticks: u32,
    tick_interval_ms: u64,
    metrics_addr: Option<SocketAddr>,
    metrics_interval_secs: u64,
}

fn parse_peer_entry(entry: &str) -> Option<(NodeId, SocketAddr)> {
    let mut parts = entry.split('=');
    let id_str = parts.next()?.trim();
    let addr_str = parts.next()?.trim();
    let id: u64 = id_str.parse().ok()?;
    let addr: SocketAddr = addr_str.parse().ok()?;
    Some((NodeId(id), addr))
}

fn parse_peers_list(s: &str) -> Vec<(NodeId, SocketAddr)> {
    s.split(',')
        .filter_map(|item| {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                None
            } else {
                parse_peer_entry(trimmed)
            }
        })
        .collect()
}

fn print_usage() {
    println!(
        r#"Flotilla Consensus Server Daemon

USAGE:
    flotilla-server [OPTIONS]

OPTIONS:
    -h, --help                                 Print this help information
    --node-id <ID>                             Node ID (default: 1, env: FLOTILLA_NODE_ID)
    --udp-addr <ADDR>                          UDP listen address (default: 0.0.0.0:9000, env: FLOTILLA_UDP_ADDR)
    --tcp-addr <ADDR>                          TCP listen address (default: 0.0.0.0:9001, env: FLOTILLA_TCP_ADDR)
    --grpc-addr <ADDR>                         gRPC listen address (default: 0.0.0.0:50051, env: FLOTILLA_GRPC_ADDR)
    --peer <ID=ADDR>                           Register peer (e.g. --peer 2=10.0.0.2:9000, can repeat)
    --peers <ID=ADDR,...>                      Comma-separated peers (env: FLOTILLA_PEERS)
    --election-timeout-ticks <TICKS>          Election timeout ticks (default: 10, env: FLOTILLA_ELECTION_TIMEOUT_TICKS)
    --heartbeat-interval-ticks <TICKS>        Heartbeat interval ticks (default: 3, env: FLOTILLA_HEARTBEAT_INTERVAL_TICKS)
    --tick-ms <MS>                             Logical tick duration in ms (default: 100, env: FLOTILLA_TICK_MS)
    --metrics-addr <ADDR>                      Prometheus metrics HTTP listen address (env: FLOTILLA_METRICS_ADDR)
    --metrics-interval-secs <SECS>            Telemetry log interval in seconds (default: 10, env: FLOTILLA_METRICS_INTERVAL_SECS)
"#
    );
}

fn parse_server_settings_from_args(args: &[String]) -> Result<Option<ServerSettings>, String> {
    let mut node_id: u64 = env::var("FLOTILLA_NODE_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let mut udp_addr: SocketAddr = env::var("FLOTILLA_UDP_ADDR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| "0.0.0.0:9000".parse().unwrap());

    let mut tcp_addr: SocketAddr = env::var("FLOTILLA_TCP_ADDR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| "0.0.0.0:9001".parse().unwrap());

    let mut grpc_addr: SocketAddr = env::var("FLOTILLA_GRPC_ADDR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| "0.0.0.0:50051".parse().unwrap());

    let mut election_timeout_ticks: u32 = env::var("FLOTILLA_ELECTION_TIMEOUT_TICKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let mut heartbeat_interval_ticks: u32 = env::var("FLOTILLA_HEARTBEAT_INTERVAL_TICKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    let mut tick_interval_ms: u64 = env::var("FLOTILLA_TICK_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let mut metrics_addr: Option<SocketAddr> = env::var("FLOTILLA_METRICS_ADDR")
        .ok()
        .and_then(|v| v.parse().ok());

    let mut metrics_interval_secs: u64 = env::var("FLOTILLA_METRICS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let mut peers = Vec::new();
    if let Ok(env_peers) = env::var("FLOTILLA_PEERS") {
        peers.extend(parse_peers_list(&env_peers));
    }

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print_usage();
                return Ok(None);
            }
            "--node-id" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --node-id".to_string());
                }
                node_id = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --node-id: {e}"))?;
            }
            "--udp-addr" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --udp-addr".to_string());
                }
                udp_addr = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --udp-addr: {e}"))?;
            }
            "--tcp-addr" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --tcp-addr".to_string());
                }
                tcp_addr = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --tcp-addr: {e}"))?;
            }
            "--grpc-addr" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --grpc-addr".to_string());
                }
                grpc_addr = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --grpc-addr: {e}"))?;
            }
            "--peer" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --peer".to_string());
                }
                let peer = parse_peer_entry(&args[i]).ok_or_else(|| {
                    format!("Invalid peer format: '{}', expected ID=IP:PORT", args[i])
                })?;
                peers.push(peer);
            }
            "--peers" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --peers".to_string());
                }
                peers.extend(parse_peers_list(&args[i]));
            }
            "--election-timeout-ticks" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --election-timeout-ticks".to_string());
                }
                election_timeout_ticks = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --election-timeout-ticks: {e}"))?;
            }
            "--heartbeat-interval-ticks" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --heartbeat-interval-ticks".to_string());
                }
                heartbeat_interval_ticks = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --heartbeat-interval-ticks: {e}"))?;
            }
            "--tick-ms" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --tick-ms".to_string());
                }
                tick_interval_ms = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --tick-ms: {e}"))?;
            }
            "--metrics-addr" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --metrics-addr".to_string());
                }
                let addr: SocketAddr = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --metrics-addr: {e}"))?;
                metrics_addr = Some(addr);
            }
            "--metrics-interval-secs" => {
                i += 1;
                if i >= args.len() {
                    return Err("Missing value for --metrics-interval-secs".to_string());
                }
                metrics_interval_secs = args[i]
                    .parse()
                    .map_err(|e| format!("Invalid --metrics-interval-secs: {e}"))?;
            }
            unknown => {
                return Err(format!("Unknown argument: {unknown}"));
            }
        }
        i += 1;
    }

    Ok(Some(ServerSettings {
        node_id: NodeId(node_id),
        udp_addr,
        tcp_addr,
        grpc_addr,
        peers,
        election_timeout_ticks,
        heartbeat_interval_ticks,
        tick_interval_ms,
        metrics_addr,
        metrics_interval_secs,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = env::args().collect();
    let settings = match parse_server_settings_from_args(&args) {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(()),
        Err(err) => {
            eprintln!("Error: {err}");
            std::process::exit(1);
        }
    };

    println!("============================================================");
    println!("Flotilla Consensus Node Starting");
    println!("  Node ID:                  {}", settings.node_id.0);
    println!("  UDP Listen Address:       {}", settings.udp_addr);
    println!("  TCP Listen Address:       {}", settings.tcp_addr);
    println!("  gRPC Listen Address:      {}", settings.grpc_addr);
    println!("  Registered Peers:         {:?}", settings.peers);
    println!(
        "  Election Timeout (ticks): {}",
        settings.election_timeout_ticks
    );
    println!(
        "  Heartbeat Interval:       {}",
        settings.heartbeat_interval_ticks
    );
    println!(
        "  Tick Interval:            {} ms",
        settings.tick_interval_ms
    );
    if let Some(metrics_addr) = settings.metrics_addr {
        println!("  Metrics Prometheus HTTP:  http://{metrics_addr}/metrics");
    }
    if settings.metrics_interval_secs > 0 {
        println!(
            "  Metrics Log Interval:     {} s",
            settings.metrics_interval_secs
        );
    }
    println!("============================================================");

    let mut router = UdpClusterRouter::new();
    let peer_ids: Vec<NodeId> = settings
        .peers
        .iter()
        .map(|(id, addr)| {
            router.register_peer(*id, *addr);
            *id
        })
        .collect();

    let raft_config = RaftConfig {
        node_id: settings.node_id,
        peers: peer_ids,
        election_timeout_ticks: settings.election_timeout_ticks,
        heartbeat_interval_ticks: settings.heartbeat_interval_ticks,
    };

    let node = Arc::new(Mutex::new(RaftNode::<1024, 1024>::new(raft_config)));

    // 1. Initialize UDP Listener
    let udp_listener = Arc::new(UdpListener::bind(settings.udp_addr, router)?);
    let _ = udp_listener.driver.set_nonblocking(true);
    let udp_driver_clone = Arc::clone(&udp_listener);
    let node_udp = Arc::clone(&node);

    tokio::spawn(async move {
        let mut buf = [0u8; 1500];
        loop {
            let res = {
                let mut locked = node_udp.lock();
                udp_driver_clone.poll_and_step(&mut *locked, &mut buf)
            };
            match res {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    });
    println!("✓ UDP transport listening on {}", settings.udp_addr);

    // 2. Initialize TCP Listener
    let _tcp = TcpListener::bind(settings.tcp_addr, Arc::clone(&node)).await?;
    println!("✓ TCP transport listening on {}", settings.tcp_addr);

    // 3. Initialize gRPC Service
    let grpc_service = GrpcService::new(Arc::clone(&node));
    let grpc_addr = settings.grpc_addr;
    tokio::spawn(async move {
        if let Err(e) = grpc_service.serve(grpc_addr).await {
            eprintln!("gRPC service error: {e}");
        }
    });
    println!("✓ gRPC transport listening on {}", settings.grpc_addr);

    // 4. Start Logical Tick Driver Loop
    let node_ticker = Arc::clone(&node);
    let udp_ticker_driver = Arc::clone(&udp_listener);
    let tick_interval = Duration::from_millis(settings.tick_interval_ms);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        loop {
            interval.tick().await;
            flotilla_raft::telemetry::metrics().ticks_total.inc();
            let _timer = flotilla_raft::telemetry::DurationTimer::start(
                &flotilla_raft::telemetry::metrics().tick_duration,
            );

            let outbound = {
                let mut locked = node_ticker.lock();
                locked.tick()
            };
            for msg in outbound {
                if let OutboundMessage::SendPacket { to, packet } = msg
                    && let Some(dest_addr) = udp_ticker_driver.router.peer_addr(to)
                {
                    flotilla_raft::telemetry::metrics().udp_sent.inc();
                    let _ = udp_ticker_driver.driver.send_to(&packet, dest_addr);
                }
            }
        }
    });
    println!(
        "✓ Logical consensus ticker loop active (every {} ms)",
        settings.tick_interval_ms
    );

    // 5. Start Telemetry Periodic Logger Task
    if settings.metrics_interval_secs > 0 {
        let interval_secs = settings.metrics_interval_secs;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                let snap = flotilla_raft::telemetry::metrics().snapshot();
                tracing::info!("\n{}", snap.to_summary_report());
            }
        });
        println!(
            "✓ Telemetry metrics logger active (every {} s)",
            settings.metrics_interval_secs
        );
    }

    // 6. Start Prometheus HTTP Metrics Server if configured
    if let Some(metrics_addr) = settings.metrics_addr {
        tokio::spawn(async move {
            if let Ok(listener) = tokio::net::TcpListener::bind(metrics_addr).await {
                tracing::info!(
                    "✓ Metrics HTTP endpoint listening on http://{metrics_addr}/metrics"
                );
                loop {
                    if let Ok((mut stream, _)) = listener.accept().await {
                        tokio::spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut req_buf = [0u8; 1024];
                            if let Ok(n) = stream.read(&mut req_buf).await {
                                let req_str = String::from_utf8_lossy(&req_buf[..n]);
                                if req_str.starts_with("GET /metrics")
                                    || req_str.starts_with("GET / ")
                                {
                                    let body = flotilla_raft::telemetry::metrics()
                                        .snapshot()
                                        .to_prometheus_text();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                        body.len(),
                                        body
                                    );
                                    let _ = stream.write_all(resp.as_bytes()).await;
                                } else {
                                    let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                    let _ = stream.write_all(resp.as_bytes()).await;
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    println!("Flotilla node initialization complete. Press Ctrl+C to terminate.");

    // 7. Await termination signal
    tokio::signal::ctrl_c().await?;
    println!("\nShutdown signal received. Flotilla server shutting down.");

    Ok(())
}
