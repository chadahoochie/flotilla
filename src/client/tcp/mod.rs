//! TCP client implementation and stream framing for Flotilla consensus.

pub mod framing;
pub mod tcp_client;

pub use tcp_client::TcpClient;
