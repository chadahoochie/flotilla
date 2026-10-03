use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

static TRACE_SEED_COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

/// Fast pseudo-random 64-bit entropy generator without heap allocation or external crates.
pub fn generate_entropy_u64() -> u64 {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let count = TRACE_SEED_COUNTER.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let mut z = now.wrapping_add(count);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    let out = z ^ (z >> 31);
    out | ((out == 0) as u64)
}

/// 128-bit globally unique distributed trace identifier.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct TraceId(pub [u8; 16]);

impl TraceId {
    /// Nil (all zeros) trace identifier.
    pub const NIL: Self = Self([0; 16]);

    /// Generate a new non-zero 128-bit distributed trace ID.
    pub fn generate() -> Self {
        let hi = generate_entropy_u64();
        let lo = generate_entropy_u64();
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&hi.to_be_bytes());
        bytes[8..].copy_from_slice(&lo.to_be_bytes());
        Self(bytes)
    }

    /// Construct a TraceId directly from 16 raw bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return reference to raw 16-byte slice.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Check if the trace ID is nil (all zeros).
    pub fn is_nil(&self) -> bool {
        self.0 == [0u8; 16]
    }

    /// Format to standard 32-character lowercase hexadecimal string.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(32);
        for byte in self.0 {
            use std::fmt::Write;
            let _ = write!(s, "{byte:02x}");
        }
        s
    }

    /// Parse a 32-character hexadecimal string into a TraceId.
    pub fn from_hex(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed.len() != 32 {
            return None;
        }
        let mut bytes = [0u8; 16];
        for i in 0..16 {
            let chunk = &trimmed[i * 2..i * 2 + 2];
            bytes[i] = u8::from_str_radix(chunk, 16).ok()?;
        }
        Some(Self(bytes))
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({})", self.to_hex())
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}
