//! Zero-copy wire protocol binary framing, serialization, and CRC32 verification.

pub mod codec_error;
pub mod packet_header;

pub use codec_error::CodecError;
pub use packet_header::{PacketHeader, HEADER_SIZE};

use crate::message::{
    AppendEntriesHeader, AppendEntriesReply, MsgType, RequestVoteArgs, RequestVoteReply,
};
use crate::types::{NodeId, Term};
use crc32fast::Hasher;
use zerocopy::{FromBytes, IntoBytes};

/// ASCII constant "FLT1" (0x464C5431).
pub const MAGIC: u32 = 0x464C_5431;

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u16 = 1;

/// Standard maximum Ethernet UDP payload size to prevent fragmentation.
pub const MAX_DATAGRAM_SIZE: usize = 1472;

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
