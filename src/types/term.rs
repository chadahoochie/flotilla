use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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
