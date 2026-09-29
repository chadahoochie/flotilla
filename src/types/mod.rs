//! Core domain primitive types for Flotilla Raft consensus.

pub mod hard_state;
pub mod log_index;
pub mod node_id;
pub mod role;
pub mod term;

pub use hard_state::HardState;
pub use log_index::LogIndex;
pub use node_id::NodeId;
pub use role::Role;
pub use term::Term;
