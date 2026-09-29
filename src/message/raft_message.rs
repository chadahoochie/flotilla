use super::append_entries_header::AppendEntriesHeader;
use super::append_entries_reply::AppendEntriesReply;
use super::request_vote_args::RequestVoteArgs;
use super::request_vote_reply::RequestVoteReply;
use crate::types::{LogIndex, NodeId, Term};

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
