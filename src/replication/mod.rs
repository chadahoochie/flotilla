//! Peer log replication progress tracking and consistency invariants for Flotilla Raft.

pub mod evaluator;
pub mod follower_append_result;
pub mod peer_progress;
pub mod peer_progress_tracker;

pub use evaluator::evaluate_follower_append_entries;
pub use follower_append_result::FollowerAppendResult;
pub use peer_progress::PeerProgress;
pub use peer_progress_tracker::PeerProgressTracker;
