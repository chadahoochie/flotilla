use super::span_id::SpanId;
use super::trace_flags::TraceFlags;
use super::trace_id::TraceId;
use std::fmt;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Fixed 32-byte binary trace context propagating across process, transport, and wire boundaries.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct TraceContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub flags: TraceFlags,
    pub _pad: [u8; 7],
}

impl TraceContext {
    /// Nil trace context.
    pub const NIL: Self = Self {
        trace_id: TraceId::NIL,
        span_id: SpanId::NIL,
        flags: TraceFlags::NONE,
        _pad: [0; 7],
    };

    /// Create an explicit trace context.
    pub const fn new(trace_id: TraceId, span_id: SpanId, flags: TraceFlags) -> Self {
        Self {
            trace_id,
            span_id,
            flags,
            _pad: [0; 7],
        }
    }

    /// Generate a brand new root trace context with fresh trace_id and span_id, marked as sampled.
    pub fn new_root() -> Self {
        Self {
            trace_id: TraceId::generate(),
            span_id: SpanId::generate(),
            flags: TraceFlags::SAMPLED,
            _pad: [0; 7],
        }
    }

    /// Derive a child trace context retaining the distributed trace_id and flags, but generating a new span_id.
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::generate(),
            flags: self.flags,
            _pad: [0; 7],
        }
    }

    /// Format as standard W3C `traceparent` header string: `00-{trace_id}-{span_id}-{flags}`.
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{}",
            self.trace_id.to_hex(),
            self.span_id.to_hex(),
            self.flags.to_hex()
        )
    }

    /// Parse a standard W3C `traceparent` header string into a `TraceContext`.
    pub fn from_traceparent(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.trim().split('-').collect();
        if parts.len() < 4 {
            return None;
        }

        // Version check: only version "00" supported
        if parts[0] != "00" {
            return None;
        }

        let trace_id = TraceId::from_hex(parts[1])?;
        if trace_id.is_nil() {
            return None;
        }

        let span_id = SpanId::from_hex(parts[2])?;
        if span_id.is_nil() {
            return None;
        }

        let flags = TraceFlags::from_hex(parts[3])?;

        Some(Self::new(trace_id, span_id, flags))
    }

    /// Serialize to 32 fixed bytes.
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        let _ = self.write_to_prefix(&mut out);
        out
    }

    /// Deserialize from 32 fixed bytes.
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self::read_from_prefix(bytes)
            .map(|(ctx, _)| ctx)
            .unwrap_or(Self::NIL)
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new_root()
    }
}

impl fmt::Debug for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TraceContext(trace_id: {}, span_id: {}, flags: 0x{:02x})",
            self.trace_id, self.span_id, self.flags.0
        )
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_traceparent())
    }
}
