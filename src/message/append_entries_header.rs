use crate::types::{LogIndex, NodeId, Term};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Fixed header for AppendEntries RPC arguments.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct AppendEntriesHeader {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub leader_commit: LogIndex,
    pub entries_count: u32,
    pub _pad: [u8; 4],
}
