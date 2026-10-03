use std::error::Error;
use std::fmt;

/// Errors produced during telemetry operations, framing, and context propagation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryError {
    /// Provided destination buffer is too small for serialization.
    BufferTooSmall,
    /// Packet or envelope magic bytes do not match expected signature.
    InvalidEnvelopeMagic,
    /// Envelope payload length does not match provided byte slice.
    PayloadLengthMismatch,
    /// Invalid or malformed W3C traceparent string.
    InvalidTraceparent,
}

impl fmt::Display for TelemetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall => write!(f, "Telemetry buffer too small"),
            Self::InvalidEnvelopeMagic => write!(f, "Invalid telemetry envelope magic bytes"),
            Self::PayloadLengthMismatch => write!(f, "Telemetry envelope payload length mismatch"),
            Self::InvalidTraceparent => write!(f, "Malformed or invalid W3C traceparent"),
        }
    }
}

impl Error for TelemetryError {}
