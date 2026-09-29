use crate::message::{
    AppendEntriesHeader, AppendEntriesReply, MsgType, RequestVoteArgs, RequestVoteReply,
};
use crate::types::{NodeId, Term};
use crc32fast::Hasher;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// ASCII constant "FLT1" (0x464C5431).
pub const MAGIC: u32 = 0x464C_5431;

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u16 = 1;

/// Total size of the fixed binary packet header in bytes.
pub const HEADER_SIZE: usize = 40;

/// Standard maximum Ethernet UDP payload size to prevent fragmentation.
pub const MAX_DATAGRAM_SIZE: usize = 1472;

/// Fixed-size binary wire header for all Flotilla UDP datagrams.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct PacketHeader {
    pub magic: u32,
    pub version: u16,
    pub msg_type: u16,
    pub sender_id: NodeId,
    pub receiver_id: NodeId,
    pub term: Term,
    pub checksum: u32,
    pub payload_len: u32,
}

const _: () = {
    assert!(std::mem::size_of::<PacketHeader>() == HEADER_SIZE);
    assert!(std::mem::align_of::<PacketHeader>() == 8);
};

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

/// Calculate CRC32 checksum over the provided byte slice.
#[inline]
pub fn calculate_crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Verify whether the payload matches the header's checksum.
#[inline]
pub fn verify_checksum(header: &PacketHeader, payload: &[u8]) -> bool {
    calculate_crc32(payload) == header.checksum
}

/// Decode a packet header and extract its payload slice without heap allocation.
pub fn decode_packet(buf: &[u8]) -> Result<(PacketHeader, &[u8]), CodecError> {
    if buf.len() < HEADER_SIZE {
        return Err(CodecError::BufferTooSmall);
    }

    let (header, rest) = PacketHeader::read_from_prefix(buf)
        .map_err(|_| CodecError::SerializationError)?;

    if header.magic != MAGIC {
        return Err(CodecError::InvalidMagic(header.magic));
    }
    if header.version != PROTOCOL_VERSION {
        return Err(CodecError::UnsupportedVersion(header.version));
    }
    if rest.len() < header.payload_len as usize {
        return Err(CodecError::PayloadLengthMismatch {
            expected: header.payload_len,
            actual: rest.len(),
        });
    }

    let payload = &rest[..header.payload_len as usize];
    Ok((header, payload))
}

/// Encode a raw packet header into the destination buffer.
pub fn encode_packet_header(
    buf: &mut [u8],
    header: &PacketHeader,
) -> Result<usize, CodecError> {
    if buf.len() < HEADER_SIZE {
        return Err(CodecError::BufferTooSmall);
    }
    header
        .write_to_prefix(&mut buf[..HEADER_SIZE])
        .map_err(|_| CodecError::SerializationError)?;
    Ok(HEADER_SIZE)
}

/// Encode a RequestVoteArgs message into a UDP datagram buffer.
pub fn encode_request_vote_args(
    buf: &mut [u8],
    sender: NodeId,
    receiver: NodeId,
    term: Term,
    args: &RequestVoteArgs,
) -> Result<usize, CodecError> {
    let payload_bytes = args.as_bytes();
    let total_len = HEADER_SIZE + payload_bytes.len();
    if buf.len() < total_len {
        return Err(CodecError::BufferTooSmall);
    }

    let checksum = calculate_crc32(payload_bytes);
    let header = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: MsgType::RequestVoteArgs as u16,
        sender_id: sender,
        receiver_id: receiver,
        term,
        checksum,
        payload_len: payload_bytes.len() as u32,
    };

    encode_packet_header(buf, &header)?;
    buf[HEADER_SIZE..total_len].copy_from_slice(payload_bytes);
    Ok(total_len)
}

/// Encode a RequestVoteReply message into a UDP datagram buffer.
pub fn encode_request_vote_reply(
    buf: &mut [u8],
    sender: NodeId,
    receiver: NodeId,
    term: Term,
    reply: &RequestVoteReply,
) -> Result<usize, CodecError> {
    let payload_bytes = reply.as_bytes();
    let total_len = HEADER_SIZE + payload_bytes.len();
    if buf.len() < total_len {
        return Err(CodecError::BufferTooSmall);
    }

    let checksum = calculate_crc32(payload_bytes);
    let header = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: MsgType::RequestVoteReply as u16,
        sender_id: sender,
        receiver_id: receiver,
        term,
        checksum,
        payload_len: payload_bytes.len() as u32,
    };

    encode_packet_header(buf, &header)?;
    buf[HEADER_SIZE..total_len].copy_from_slice(payload_bytes);
    Ok(total_len)
}

/// Encode an AppendEntriesReply message into a UDP datagram buffer.
pub fn encode_append_entries_reply(
    buf: &mut [u8],
    sender: NodeId,
    receiver: NodeId,
    term: Term,
    reply: &AppendEntriesReply,
) -> Result<usize, CodecError> {
    let payload_bytes = reply.as_bytes();
    let total_len = HEADER_SIZE + payload_bytes.len();
    if buf.len() < total_len {
        return Err(CodecError::BufferTooSmall);
    }

    let checksum = calculate_crc32(payload_bytes);
    let header = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: MsgType::AppendEntriesReply as u16,
        sender_id: sender,
        receiver_id: receiver,
        term,
        checksum,
        payload_len: payload_bytes.len() as u32,
    };

    encode_packet_header(buf, &header)?;
    buf[HEADER_SIZE..total_len].copy_from_slice(payload_bytes);
    Ok(total_len)
}

/// Encode an AppendEntries message with a fixed header and pre-serialized entries payload.
pub fn encode_append_entries(
    buf: &mut [u8],
    sender: NodeId,
    receiver: NodeId,
    term: Term,
    header_args: &AppendEntriesHeader,
    entries_data: &[u8],
) -> Result<usize, CodecError> {
    let header_bytes = header_args.as_bytes();
    let payload_len = header_bytes.len() + entries_data.len();
    let total_len = HEADER_SIZE + payload_len;
    if buf.len() < total_len {
        return Err(CodecError::BufferTooSmall);
    }

    let mut hasher = Hasher::new();
    hasher.update(header_bytes);
    hasher.update(entries_data);
    let checksum = hasher.finalize();

    let header = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: MsgType::AppendEntriesArgs as u16,
        sender_id: sender,
        receiver_id: receiver,
        term,
        checksum,
        payload_len: payload_len as u32,
    };

    encode_packet_header(buf, &header)?;
    let header_end = HEADER_SIZE + header_bytes.len();
    buf[HEADER_SIZE..header_end].copy_from_slice(header_bytes);
    buf[header_end..total_len].copy_from_slice(entries_data);
    Ok(total_len)
}
