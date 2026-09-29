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

/// Raft election term counter.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
pub struct Term(pub u64);

impl Term {
    /// Initial term (0).
    pub const ZERO: Self = Self(0);

    /// Construct a new `Term`.
    #[inline(always)]
    pub const fn new(val: u64) -> Self {
        Self(val)
    }

    /// Return the inner `u64` term.
    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Increment term by one.
    #[inline(always)]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// 1-based log index in the Raft replicated log.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// Zero index (represents uninitialized or empty log).
    pub const ZERO: Self = Self(0);

    /// Construct a new `LogIndex`.
    #[inline(always)]
    pub const fn new(val: u64) -> Self {
        Self(val)
    }

    /// Return the inner `u64` index.
    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advance index by one.
    #[inline(always)]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Role of a Raft node.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Role {
    #[default]
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

/// Persisted hard state for crash recovery.
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: NodeId,
}
