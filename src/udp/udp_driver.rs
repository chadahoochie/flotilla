use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

/// Thin socket driver wrapper around `std::net::UdpSocket`.
pub struct UdpDriver {
    pub socket: UdpSocket,
}

impl UdpDriver {
    /// Bind a UDP socket to the given local address.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        Ok(Self { socket })
    }

    /// Set non-blocking mode on the underlying UDP socket.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }

    /// Transmit a framed packet to the destination address.
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(buf, addr)
    }

    /// Receive an inbound datagram from the network.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(buf)
    }

    /// Return the local socket address this driver is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}
