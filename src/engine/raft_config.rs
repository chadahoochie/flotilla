use crate::types::NodeId;

/// Configuration options for a Raft node instance.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_timeout_ticks: u32,
    pub heartbeat_interval_ticks: u32,
}
