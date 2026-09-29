use crate::types::{LogIndex, Term};

/// Pre-allocated cacheline-aligned storage slot for a single log entry.
#[repr(align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogSlot<const MAX_PAYLOAD: usize> {
    pub term: Term,
    pub index: LogIndex,
    pub payload_len: u32,
    pub payload: [u8; MAX_PAYLOAD],
}

impl<const MAX_PAYLOAD: usize> Default for LogSlot<MAX_PAYLOAD> {
    fn default() -> Self {
        Self {
            term: Term::ZERO,
            index: LogIndex::ZERO,
            payload_len: 0,
            payload: [0u8; MAX_PAYLOAD],
        }
    }
}

impl<const MAX_PAYLOAD: usize> LogSlot<MAX_PAYLOAD> {
    /// Return the active payload slice for this entry.
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload[..self.payload_len as usize]
    }
}
