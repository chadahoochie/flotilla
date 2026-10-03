use std::fmt;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 8-bit distributed trace flags (W3C Trace Context recommendation).
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct TraceFlags(pub u8);

impl TraceFlags {
    /// No flags set (unrecorded/unsampled).
    pub const NONE: Self = Self(0x00);

    /// Trace sampled flag bit (0x01).
    pub const SAMPLED: Self = Self(0x01);

    /// Check if the sampled flag bit is enabled.
    pub const fn is_sampled(&self) -> bool {
        (self.0 & Self::SAMPLED.0) != 0
    }

    /// Format to standard 2-character hexadecimal string.
    pub fn to_hex(&self) -> String {
        format!("{:02x}", self.0)
    }

    /// Parse a 2-character hexadecimal string into TraceFlags.
    pub fn from_hex(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed.len() != 2 {
            return None;
        }
        let val = u8::from_str_radix(trimmed, 16).ok()?;
        Some(Self(val))
    }
}

impl fmt::Debug for TraceFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceFlags(0x{:02x})", self.0)
    }
}

impl fmt::Display for TraceFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02x}", self.0)
    }
}
