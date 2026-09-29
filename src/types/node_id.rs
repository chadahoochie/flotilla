use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Identifier for a node in the Raft cluster.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Sentinel value representing no node or an unknown node.
    pub const NONE: Self = Self(0);

    /// Construct a new `NodeId`.
    #[inline(always)]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Return the inner `u64` id.
    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0
    }
}
