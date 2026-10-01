use super::server_error::ServerError;
use crate::client::tcp::framing::{read_packet_frame, write_packet_frame};
use crate::engine::{OutboundMessage, RaftNode};
use crate::types::NodeId;
use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

/// Standalone pure worker function handling incoming TCP stream packets.
pub(crate) async fn handle_tcp_stream<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
    mut stream: TcpStream,
    node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
) {
    let mut buf = [0u8; 1500];
    loop {
        match read_packet_frame(&mut stream, &mut buf).await {
            Ok(len) if len > 0 => {
                let actions = {
                    let mut locked = node.lock();
                    locked.step(NodeId(0), &buf[..len])
                };
                if let Ok(acts) = actions {
                    for act in acts {
                        if let OutboundMessage::SendPacket { packet, .. } = act
                            && write_packet_frame(&mut stream, &packet).await.is_err()
                        {
                            return;
                        }
                    }
                }
            }
            _ => break,
        }
    }
}

/// Standalone worker loop accepting TCP connections.
pub(crate) async fn run_tcp_accept_loop<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
    listener: TokioTcpListener,
    node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
) {
    while let Ok((stream, _)) = listener.accept().await {
        let node_clone = Arc::clone(&node);
        tokio::spawn(async move {
            handle_tcp_stream(stream, node_clone).await;
        });
    }
}

/// Connection listener accepting TCP streams and stepping a shared Flotilla node.
pub struct TcpListener {
    pub local_addr: SocketAddr,
}

impl TcpListener {
    /// Bind a new TCP listener and spawn an accept loop driving the shared RaftNode.
    pub async fn bind<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
        addr: SocketAddr,
        node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
    ) -> Result<Self, ServerError> {
        let listener = TokioTcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        tokio::spawn(async move {
            run_tcp_accept_loop(listener, node).await;
        });

        Ok(Self { local_addr })
    }

    /// Return the local bound socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
