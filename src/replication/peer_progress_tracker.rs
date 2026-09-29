use super::peer_progress::PeerProgress;
use crate::types::{LogIndex, NodeId};

/// Tracking structure for all peer replication states on the leader.
#[derive(Debug, Clone)]
pub struct PeerProgressTracker {
    pub peers: Vec<PeerProgress>,
}

impl PeerProgressTracker {
    /// Initialize tracking for a set of peer nodes.
    pub fn new(peer_ids: &[NodeId], last_log_index: LogIndex) -> Self {
        let peers = peer_ids
            .iter()
            .copied()
            .map(|id| PeerProgress {
                id,
                match_index: LogIndex::ZERO,
                next_index: last_log_index.next(),
            })
            .collect();
        Self { peers }
    }

    /// Retrieve the match index for a given peer.
    pub fn get_match_index(&self, id: NodeId) -> LogIndex {
        self.peers
            .iter()
            .find(|p| p.id == id)
            .map(|p| p.match_index)
            .unwrap_or(LogIndex::ZERO)
    }

    /// Retrieve the next index to send to a given peer.
    pub fn get_next_index(&self, id: NodeId) -> LogIndex {
        self.peers
            .iter()
            .find(|p| p.id == id)
            .map(|p| p.next_index)
            .unwrap_or(LogIndex(1))
    }

    /// Record a successful AppendEntries response from a peer.
    pub fn record_success(&mut self, id: NodeId, match_idx: LogIndex) {
        if let Some(peer) = self.peers.iter_mut().find(|p| p.id == id) {
            peer.match_index = match_idx;
            peer.next_index = match_idx.next();
        }
    }

    /// Record an AppendEntries rejection from a peer, decrementing next_index.
    pub fn record_rejection(&mut self, id: NodeId) -> LogIndex {
        if let Some(peer) = self.peers.iter_mut().find(|p| p.id == id) {
            if peer.next_index.0 > 1 {
                peer.next_index = LogIndex(peer.next_index.0 - 1);
            }
            peer.next_index
        } else {
            LogIndex(1)
        }
    }

    /// Reset all peers' next_index to `last_log_index + 1` (upon becoming leader).
    pub fn reset_all_next_indices(&mut self, last_log_index: LogIndex) {
        for peer in &mut self.peers {
            peer.match_index = LogIndex::ZERO;
            peer.next_index = last_log_index.next();
        }
    }

    /// Collect all peer match indices plus the leader's own last index into a fixed stack slice.
    pub fn collect_all_match_indices(
        &self,
        leader_last_index: LogIndex,
        dest: &mut [LogIndex],
    ) -> usize {
        if dest.is_empty() {
            return 0;
        }
        dest[0] = leader_last_index;
        let mut count = 1;
        for peer in &self.peers {
            if count < dest.len() {
                dest[count] = peer.match_index;
                count += 1;
            }
        }
        count
    }
}
