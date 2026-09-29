/// Errors encountered during packet encoding or decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    BufferTooSmall,
    InvalidMagic(u32),
    UnsupportedVersion(u16),
    PayloadLengthMismatch { expected: u32, actual: usize },
    ChecksumMismatch { header_crc: u32, computed_crc: u32 },
    InvalidMessageType(u16),
    SerializationError,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BufferTooSmall => write!(f, "destination buffer is too small"),
            Self::InvalidMagic(m) => write!(f, "invalid magic: 0x{m:08X}"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported protocol version: {v}"),
            Self::PayloadLengthMismatch { expected, actual } => {
                write!(f, "payload length mismatch: header={expected}, actual={actual}")
            }
            Self::ChecksumMismatch { header_crc, computed_crc } => {
                write!(f, "checksum mismatch: expected 0x{header_crc:08X}, got 0x{computed_crc:08X}")
            }
            Self::InvalidMessageType(t) => write!(f, "invalid message type code: {t}"),
            Self::SerializationError => write!(f, "failed to serialize message payload"),
        }
    }
}

impl std::error::Error for CodecError {}
