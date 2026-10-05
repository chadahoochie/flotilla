use super::commit_broadcaster::CommitBroadcaster;
use super::server_error::ServerError;
use super::udp_listener::UdpListener;
use crate::client::tcp::framing::{read_packet_frame, write_packet_frame};
use crate::engine::{OutboundMessage, RaftNode};
use crate::types::NodeId;
use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

/// Standalone worker loop streaming committed log entry frames to a TCP subscriber.
pub(crate) async fn run_subscriber_stream(
    mut stream: TcpStream,
    mut rx: tokio::sync::broadcast::Receiver<Arc<[u8]>>,
) {
    loop {
        match rx.recv().await {
            Ok(frame) => {
                crate::telemetry::metrics().tcp_sent.inc();
                if write_packet_frame(&mut stream, &frame).await.is_err() {
                    crate::telemetry::metrics().transport_errors.inc();
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Standalone pure worker function handling incoming TCP stream packets.
pub(crate) async fn handle_tcp_stream<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
    mut stream: TcpStream,
    node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
    broadcaster: Arc<CommitBroadcaster>,
    udp_listener: Option<Arc<UdpListener>>,
) {
    let mut buf = vec![0u8; MAX_PAYLOAD + 1024];

    // Read the initial frame to determine if this is a subscriber stream handshake
    let initial_len = match read_packet_frame(&mut stream, &mut buf).await {
        Ok(len) if len > 0 => len,
        _ => return,
    };

    if let Ok((header, _)) = crate::codec::decode_packet(&buf[..initial_len]) {
        if header.msg_type == crate::message::MsgType::HeartbeatArgs as u16 {
            let rx = broadcaster.subscribe();
            run_subscriber_stream(stream, rx).await;
            return;
        }
    }

    // Otherwise, this is a proposal / RPC connection: process initial packet then loop
    let mut current_len = initial_len;
    loop {
        crate::telemetry::metrics().tcp_received.inc();
        let _timer = crate::telemetry::DurationTimer::start(
            &crate::telemetry::metrics().tcp_request_duration,
        );

        // If packet contains a TraceEnvelope, extract trace context for the tracing span
        let _span = if current_len > crate::codec::HEADER_SIZE
            && crate::telemetry::TraceEnvelope::is_enveloped(
                &buf[crate::codec::HEADER_SIZE..current_len],
            ) {
            if let Ok((trace, _)) = crate::telemetry::TraceEnvelope::unwrap(
                &buf[crate::codec::HEADER_SIZE..current_len],
            ) {
                let span = tracing::info_span!("tcp_stream_step", trace_id = %trace.trace_id, span_id = %trace.span_id);
                #[cfg(feature = "otel")]
                {
                    use tracing_opentelemetry::OpenTelemetrySpanExt;
                    span.set_parent(crate::telemetry::trace_context_to_otel_context(
                        &trace,
                    ));
                }
                span
            } else {
                tracing::info_span!("tcp_stream_step")
            }
        } else {
            tracing::info_span!("tcp_stream_step")
        };
        let _enter = _span.enter();

        let actions = {
            let mut locked = node.lock();
            let res = locked.step(NodeId(0), &buf[..current_len]);
            if let Ok(ref acts) = res {
                for act in acts {
                    if let OutboundMessage::ApplyEntries { from_index, to_index } = *act {
                        crate::server::commit_broadcaster::broadcast_applied_entries(
                            &*locked,
                            &broadcaster,
                            from_index,
                            to_index,
                        );
                    }
                }
            }
            res
        };

        if let Ok(acts) = actions {
            for act in acts {
                match act {
                    OutboundMessage::SendPacket { to, packet } => {
                        if to == NodeId(0) {
                            let mut forwarded = false;
                            if let Ok((header, payload)) = crate::codec::decode_packet(&packet) {
                                if header.msg_type == crate::message::MsgType::ClientProposalReply as u16 {
                                    use zerocopy::FromBytes;
                                    if let Ok((reply, _)) = crate::message::ClientProposalReply::read_from_prefix(payload) {
                                        if !reply.is_success() && reply.leader_id != NodeId(0) {
                                            if let Some(ref udp) = udp_listener {
                                                if let Some(dest_addr) = udp.router.peer_addr(reply.leader_id) {
                                                    let leader_tcp_addr = SocketAddr::new(dest_addr.ip(), 9100);
                                                    if let Ok(mut leader_stream) = TcpStream::connect(leader_tcp_addr).await {
                                                        if write_packet_frame(&mut leader_stream, &buf[..current_len]).await.is_ok() {
                                                            let mut fwd_buf = vec![0u8; MAX_PAYLOAD + 1024];
                                                            if let Ok(fwd_len) = read_packet_frame(&mut leader_stream, &mut fwd_buf).await {
                                                                if write_packet_frame(&mut stream, &fwd_buf[..fwd_len]).await.is_ok() {
                                                                    crate::telemetry::metrics().tcp_sent.inc();
                                                                    forwarded = true;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if !forwarded {
                                crate::telemetry::metrics().tcp_sent.inc();
                                if write_packet_frame(&mut stream, &packet).await.is_err() {
                                    crate::telemetry::metrics().transport_errors.inc();
                                    return;
                                }
                            }
                        } else if let Some(ref udp) = udp_listener {
                            if let Some(dest_addr) = udp.router.peer_addr(to) {
                                crate::telemetry::metrics().udp_sent.inc();
                                let _ = udp.driver.send_to(&packet, dest_addr);
                            }
                        }
                    }
                    OutboundMessage::ApplyEntries { .. } => {}
                }
            }
        }

        match read_packet_frame(&mut stream, &mut buf).await {
            Ok(len) if len > 0 => current_len = len,
            _ => break,
        }
    }
}

/// Standalone worker loop accepting TCP connections.
pub(crate) async fn run_tcp_accept_loop<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
    listener: TokioTcpListener,
    node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
    broadcaster: Arc<CommitBroadcaster>,
    udp_listener: Option<Arc<UdpListener>>,
) {
    while let Ok((stream, _)) = listener.accept().await {
        crate::telemetry::metrics().tcp_accepted.inc();
        let node_clone = Arc::clone(&node);
        let b_clone = Arc::clone(&broadcaster);
        let udp_clone = udp_listener.clone();
        tokio::spawn(async move {
            handle_tcp_stream(stream, node_clone, b_clone, udp_clone).await;
        });
    }
}

/// Connection listener accepting TCP streams and stepping a shared Flotilla node.
pub struct TcpListener {
    pub local_addr: SocketAddr,
    pub broadcaster: Arc<CommitBroadcaster>,
}

impl TcpListener {
    /// Bind a new TCP listener and spawn an accept loop driving the shared RaftNode.
    pub async fn bind<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
        addr: SocketAddr,
        node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
    ) -> Result<Self, ServerError> {
        let broadcaster = Arc::new(CommitBroadcaster::default());
        Self::bind_with_options(addr, node, broadcaster, None).await
    }

    /// Bind a new TCP listener with an explicit broadcaster and optional UDP listener for peer forwarding.
    pub async fn bind_with_options<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
        addr: SocketAddr,
        node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
        broadcaster: Arc<CommitBroadcaster>,
        udp_listener: Option<Arc<UdpListener>>,
    ) -> Result<Self, ServerError> {
        let listener = TokioTcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let b_clone = Arc::clone(&broadcaster);

        tokio::spawn(async move {
            run_tcp_accept_loop(listener, node, b_clone, udp_listener).await;
        });

        Ok(Self {
            local_addr,
            broadcaster,
        })
    }

    /// Return the local bound socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
