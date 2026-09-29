use crate::types::{LogIndex, NodeId, Term};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// AppendEntries RPC response.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: u8, // 1 = true, 0 = false
    pub _pad: [u8; 7],
    pub match_index: LogIndex,
}

impl AppendEntriesReply {
    pub const fn is_success(&self) -> bool {
        self.success == 1
    }
}
