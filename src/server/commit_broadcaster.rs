use std::sync::Arc;
use tokio::sync::broadcast::{self, Receiver, Sender};

/// Zero-copy broadcast channel distributing committed Raft log entry frames to connected subscribers.
pub struct CommitBroadcaster {
    tx: Sender<Arc<[u8]>>,
}

impl Default for CommitBroadcaster {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl CommitBroadcaster {
    /// Initialize a new broadcaster with the designated ring buffer capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe to the stream of committed frames.
    pub fn subscribe(&self) -> Receiver<Arc<[u8]>> {
        self.tx.subscribe()
    }

    /// Distribute a newly committed wire frame to all active subscribers.
    pub fn broadcast(&self, frame: Arc<[u8]>) {
        let _ = self.tx.send(frame);
    }

    /// Return the number of currently active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// Standalone pure helper function broadcasting applied log entries from a RaftNode to a CommitBroadcaster.
pub fn broadcast_applied_entries<const CAPACITY: usize, const MAX_PAYLOAD: usize>(
    node: &crate::engine::RaftNode<CAPACITY, MAX_PAYLOAD>,
    broadcaster: &CommitBroadcaster,
    from_index: crate::types::LogIndex,
    to_index: crate::types::LogIndex,
) {
    for idx in from_index.0..=to_index.0 {
        if let Some(entry) = node.storage.entry_at(crate::types::LogIndex(idx)) {
            if let Ok(pkt) = crate::engine::packets::create_subscriber_commit_packet(
                node.election.config.node_id,
                entry.term,
                entry.index,
                entry.payload_bytes(),
            ) {
                broadcaster.broadcast(pkt.into());
            }
        }
    }
}
