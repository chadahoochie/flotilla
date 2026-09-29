use crate::types::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;

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
