use crate::types::{NodeId, Term};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Total size of the fixed binary packet header in bytes.
pub const HEADER_SIZE: usize = 40;

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
