//! Leader election state machine and voting safety rules for Flotilla Raft.

pub mod election_action;
pub mod election_config;
pub mod election_state;
pub mod rules;

pub use election_action::ElectionAction;
pub use election_config::ElectionConfig;
pub use election_state::ElectionState;
pub use rules::{is_log_up_to_date, is_quorum_reached, is_vote_eligible, quorum_size};
