use super::node_id::NodeId;
use super::term::Term;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Persisted hard state for crash recovery.
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: NodeId,
}
