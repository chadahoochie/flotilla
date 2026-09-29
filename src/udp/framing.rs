use crate::codec::MAX_DATAGRAM_SIZE;

/// Default maximum transmission unit for Ethernet network interfaces.
pub const ETHERNET_MTU: usize = 1500;

/// Standard IPv4 + UDP header size (20 bytes IP + 8 bytes UDP).
pub const IP_UDP_OVERHEAD: usize = 28;

/// Enforce that a datagram fits within standard MTU constraints without IP fragmentation.
#[inline(always)]
pub fn fits_in_mtu(packet_len: usize) -> bool {
    packet_len <= MAX_DATAGRAM_SIZE
}
