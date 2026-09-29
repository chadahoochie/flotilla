use crate::types::Term;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// RequestVote RPC response.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: u8, // 1 = true, 0 = false
    pub _pad: [u8; 7],
}

impl RequestVoteReply {
    pub const fn is_granted(&self) -> bool {
        self.vote_granted == 1
    }
}
