use crate::types::{LogIndex, NodeId};

/// Outbound action emitted by the sans-I/O consensus state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundMessage {
    SendPacket { to: NodeId, packet: Vec<u8> },
    ApplyEntries { from_index: LogIndex, to_index: LogIndex },
}
