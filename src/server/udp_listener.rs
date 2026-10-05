use crate::engine::{OutboundMessage, RaftNode};
use crate::types::NodeId;
use crate::udp::{UdpClusterRouter, UdpDriver};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

/// Network listener for receiving and dispatching UDP consensus packets.
pub struct UdpListener {
    pub driver: UdpDriver,
    pub router: UdpClusterRouter,
    pub broadcaster: Option<std::sync::Arc<crate::server::CommitBroadcaster>>,
}

impl UdpListener {
    /// Bind a new UDP listener to the designated address.
    pub fn bind<A: ToSocketAddrs>(addr: A, router: UdpClusterRouter) -> io::Result<Self> {
        let driver = UdpDriver::bind(addr)?;
        Ok(Self {
            driver,
            router,
            broadcaster: None,
        })
    }

    /// Set an optional commit broadcaster to distribute applied entries to subscribers.
    pub fn with_broadcaster(mut self, broadcaster: std::sync::Arc<crate::server::CommitBroadcaster>) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    /// Return the local bound socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.driver.local_addr()
    }

    /// Poll and process incoming datagrams, stepping the consensus node and sending outbound packets.
    pub fn poll_and_step<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
        &self,
        node: &mut RaftNode<CAPACITY, MAX_PAYLOAD>,
        buf: &mut [u8],
    ) -> io::Result<usize> {
        match self.driver.recv_from(buf) {
            Ok((len, src_addr)) => {
                crate::telemetry::metrics().udp_received.inc();
                let _timer = crate::telemetry::DurationTimer::start(
                    &crate::telemetry::metrics().udp_step_duration,
                );

                let sender_id = self.router.peer_id(&src_addr).unwrap_or(NodeId(0));
                if let Ok(actions) = node.step(sender_id, &buf[..len]) {
                    for act in actions {
                        match act {
                            OutboundMessage::SendPacket { to, packet } => {
                                crate::telemetry::metrics().udp_sent.inc();
                                if let Some(dest_addr) = self.router.peer_addr(to) {
                                    let _ = self.driver.send_to(&packet, dest_addr);
                                } else if to == sender_id {
                                    let _ = self.driver.send_to(&packet, src_addr);
                                }
                            }
                            OutboundMessage::ApplyEntries { from_index, to_index } => {
                                if let Some(ref broadcaster) = self.broadcaster {
                                    crate::server::commit_broadcaster::broadcast_applied_entries(
                                        node,
                                        broadcaster,
                                        from_index,
                                        to_index,
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(len)
            }
            Err(e) => Err(e),
        }
    }
}
