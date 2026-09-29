use crate::types::{LogIndex, NodeId, Term};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// RPC message discriminant.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MsgType {
    #[default]
    Unknown = 0,
    RequestVoteArgs = 1,
    RequestVoteReply = 2,
    AppendEntriesArgs = 3,
    AppendEntriesReply = 4,
    HeartbeatArgs = 5,
    HeartbeatReply = 6,
}

impl MsgType {
    /// Convert a raw `u16` into a `MsgType` if valid.
    pub const fn from_u16(val: u16) -> Option<Self> {
        match val {
            1 => Some(Self::RequestVoteArgs),
            2 => Some(Self::RequestVoteReply),
            3 => Some(Self::AppendEntriesArgs),
            4 => Some(Self::AppendEntriesReply),
            5 => Some(Self::HeartbeatArgs),
            6 => Some(Self::HeartbeatReply),
            _ => None,
        }
    }

    /// Return the discriminant as `u16`.
    pub const fn to_u16(self) -> u16 {
        self as u16
    }
}

/// RequestVote RPC arguments.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct RequestVoteArgs {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

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

/// High-level strongly-typed Raft message for the state engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteResponse(RequestVoteReply),
    AppendEntries {
        header: AppendEntriesHeader,
        entries_payload: Vec<u8>,
    },
    AppendEntriesResponse(AppendEntriesReply),
    Heartbeat {
        term: Term,
        leader_id: NodeId,
        leader_commit: LogIndex,
    },
    HeartbeatResponse {
        term: Term,
        follower_id: NodeId,
        success: bool,
    },
}
