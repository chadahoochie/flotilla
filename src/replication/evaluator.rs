use super::follower_append_result::FollowerAppendResult;
use crate::message::AppendEntriesHeader;
use crate::storage::ring_buffer::RingBufferLogStorage;
use crate::types::LogIndex;

/// Evaluate an AppendEntries request on a follower and update local storage & commit index.
pub fn evaluate_follower_append_entries<const C: usize, const M: usize>(
    header: &AppendEntriesHeader,
    entries_raw: &[u8],
    storage: &mut RingBufferLogStorage<C, M>,
    current_commit: &mut LogIndex,
) -> FollowerAppendResult {
    // 1. Reply false if log doesn't contain an entry at prev_log_index matching prev_log_term (Raft §5.3)
    if header.prev_log_index.0 > 0 {
        if storage.last_index().0 < header.prev_log_index.0 {
            return FollowerAppendResult::Rejected;
        }
        if storage.term_at(header.prev_log_index) != Some(header.prev_log_term) {
            return FollowerAppendResult::Rejected;
        }
    }

    // 2. If an existing entry conflicts with a new one, delete existing entry and all that follow it
    if header.entries_count > 0 {
        let first_new_idx = header.prev_log_index.next();
        if storage.last_index().0 >= first_new_idx.0 {
            storage.truncate_suffix(first_new_idx);
        }
        // Append new entries payload
        if !entries_raw.is_empty() {
            let _ = storage.append_entry(header.term, entries_raw);
        }
    }

    // 3. Update follower's commit index if leader_commit > current_commit
    if header.leader_commit.0 > current_commit.0 {
        let min_commit = if header.leader_commit.0 < storage.last_index().0 {
            header.leader_commit
        } else {
            storage.last_index()
        };
        if min_commit.0 > current_commit.0 {
            *current_commit = min_commit;
        }
    }

    FollowerAppendResult::Success {
        match_index: storage.last_index(),
    }
}
