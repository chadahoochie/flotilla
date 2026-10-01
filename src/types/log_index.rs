use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 1-based log index in the Raft replicated log.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    FromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
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
