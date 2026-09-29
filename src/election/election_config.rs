use crate::types::NodeId;

/// Configuration parameters for the election state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionConfig {
    pub node_id: NodeId,
    pub cluster_size: usize,
    pub election_timeout_ticks: u32,
    pub heartbeat_interval_ticks: u32,
}
