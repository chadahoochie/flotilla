//! UDP transport driver and peer routing for Flotilla consensus packets.

pub mod framing;

pub use framing::fits_in_mtu;

use crate::types::NodeId;
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

/// Thin socket driver wrapper around `std::net::UdpSocket`.
pub struct UdpDriver {
    pub socket: UdpSocket,
}

impl UdpDriver {
    /// Bind a UDP socket to the given local address.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        Ok(Self { socket })
    }

    /// Set non-blocking mode on the underlying UDP socket.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }

    /// Transmit a framed packet to the destination address.
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(buf, addr)
    }

    /// Receive an inbound datagram from the network.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(buf)
    }

    /// Return the local socket address this driver is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

/// Bidirectional map between cluster `NodeId` and network `SocketAddr`.
#[derive(Debug, Default, Clone)]
pub struct UdpClusterRouter {
    pub id_to_addr: HashMap<NodeId, SocketAddr>,
    pub addr_to_id: HashMap<SocketAddr, NodeId>,
}

impl UdpClusterRouter {
    /// Create a new empty cluster address router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Associate a node ID with its network socket address.
    pub fn register_peer(&mut self, id: NodeId, addr: SocketAddr) {
        self.id_to_addr.insert(id, addr);
        self.addr_to_id.insert(addr, id);
    }

    /// Look up the network address for a given node ID.
    pub fn peer_addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.id_to_addr.get(&id).copied()
    }

    /// Look up the node ID for a given network address.
    pub fn peer_id(&self, addr: &SocketAddr) -> Option<NodeId> {
        self.addr_to_id.get(addr).copied()
    }
}
