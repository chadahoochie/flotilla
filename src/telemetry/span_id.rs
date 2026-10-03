use super::trace_id::generate_entropy_u64;
use std::fmt;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 64-bit unique span identifier within a distributed trace.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct SpanId(pub [u8; 8]);

impl SpanId {
    /// Nil (all zeros) span identifier.
    pub const NIL: Self = Self([0; 8]);

    /// Generate a new non-zero 64-bit span ID.
    pub fn generate() -> Self {
        let val = generate_entropy_u64();
        Self(val.to_be_bytes())
    }

    /// Construct a SpanId directly from 8 raw bytes.
    pub const fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    /// Return reference to raw 8-byte slice.
    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    /// Check if the span ID is nil (all zeros).
    pub fn is_nil(&self) -> bool {
        self.0 == [0u8; 8]
    }

    /// Format to standard 16-character lowercase hexadecimal string.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(16);
        for byte in self.0 {
            use std::fmt::Write;
            let _ = write!(s, "{byte:02x}");
        }
        s
    }

    /// Parse a 16-character hexadecimal string into a SpanId.
    pub fn from_hex(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed.len() != 16 {
            return None;
        }
        let mut bytes = [0u8; 8];
        for i in 0..8 {
            let chunk = &trimmed[i * 2..i * 2 + 2];
            bytes[i] = u8::from_str_radix(chunk, 16).ok()?;
        }
        Some(Self(bytes))
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpanId({})", self.to_hex())
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}
