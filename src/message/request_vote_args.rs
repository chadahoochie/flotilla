use crate::types::{LogIndex, NodeId, Term};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// RequestVote RPC arguments.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct RequestVoteArgs {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}
