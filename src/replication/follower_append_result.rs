use crate::types::LogIndex;

/// Result of evaluating an AppendEntries RPC on a follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerAppendResult {
    Success { match_index: LogIndex },
    Rejected,
}
