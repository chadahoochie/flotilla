use crate::types::{LogIndex, NodeId, Term};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Client proposal RPC response.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct ClientProposalReply {
    pub success: u8,
    pub _pad: [u8; 7],
    pub index: LogIndex,
    pub term: Term,
    pub leader_id: NodeId,
}

impl ClientProposalReply {
    pub const fn is_success(&self) -> bool {
        self.success == 1
    }
}
