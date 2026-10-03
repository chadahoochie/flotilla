use super::telemetry_error::TelemetryError;
use super::trace_context::TraceContext;
use zerocopy::{FromBytes, IntoBytes};

/// Header size of the trace envelope: 2 magic + 2 version + 32 trace context + 4 payload length = 40 bytes.
pub const ENVELOPE_HEADER_SIZE: usize = 40;

/// Zero-copy binary envelope wrapping an application payload with a distributed trace context.
pub struct TraceEnvelope;

impl TraceEnvelope {
    /// Magic signature bytes: 'T', 'E' (0x54, 0x45).
    pub const MAGIC: [u8; 2] = [0x54, 0x45];

    /// Envelope version: 1.
    pub const VERSION: u16 = 1;

    /// Check if a byte buffer starts with a valid trace envelope header.
    pub fn is_enveloped(buf: &[u8]) -> bool {
        if buf.len() < ENVELOPE_HEADER_SIZE {
            return false;
        }
        buf[0] == Self::MAGIC[0] && buf[1] == Self::MAGIC[1]
    }

    /// Wrap a payload with the given trace context into the destination buffer.
    pub fn wrap(
        trace: &TraceContext,
        payload: &[u8],
        out: &mut [u8],
    ) -> Result<usize, TelemetryError> {
        let total_len = ENVELOPE_HEADER_SIZE + payload.len();
        if out.len() < total_len {
            return Err(TelemetryError::BufferTooSmall);
        }

        out[0] = Self::MAGIC[0];
        out[1] = Self::MAGIC[1];
        out[2..4].copy_from_slice(&Self::VERSION.to_be_bytes());

        let trace_bytes = trace.as_bytes();
        out[4..36].copy_from_slice(trace_bytes);

        let len_u32 = payload.len() as u32;
        out[36..40].copy_from_slice(&len_u32.to_be_bytes());
        out[40..total_len].copy_from_slice(payload);

        Ok(total_len)
    }

    /// Unwrap an enveloped buffer, extracting the trace context and the inner payload slice.
    pub fn unwrap(buf: &[u8]) -> Result<(TraceContext, &[u8]), TelemetryError> {
        if buf.len() < ENVELOPE_HEADER_SIZE {
            return Err(TelemetryError::BufferTooSmall);
        }

        if buf[0] != Self::MAGIC[0] || buf[1] != Self::MAGIC[1] {
            return Err(TelemetryError::InvalidEnvelopeMagic);
        }

        let mut ver_bytes = [0u8; 2];
        ver_bytes.copy_from_slice(&buf[2..4]);
        let version = u16::from_be_bytes(ver_bytes);
        if version != Self::VERSION {
            return Err(TelemetryError::InvalidEnvelopeMagic);
        }

        let (ctx, _) = TraceContext::read_from_prefix(&buf[4..36])
            .map_err(|_| TelemetryError::InvalidEnvelopeMagic)?;

        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&buf[36..40]);
        let payload_len = u32::from_be_bytes(len_bytes) as usize;

        let total_len = ENVELOPE_HEADER_SIZE + payload_len;
        if buf.len() < total_len {
            return Err(TelemetryError::PayloadLengthMismatch);
        }

        let payload = &buf[ENVELOPE_HEADER_SIZE..total_len];
        Ok((ctx, payload))
    }
}
