//! Pluggable client abstractions and transport implementations for Flotilla consensus.

pub mod client_config;
pub mod client_error;
pub mod flotilla_client;
pub mod proposal_result;

pub use client_config::ClientConfig;
pub use client_error::ClientError;
pub use flotilla_client::FlotillaClient;
pub use proposal_result::ProposalResult;

#[cfg(feature = "client-udp")]
pub mod udp;
#[cfg(feature = "client-udp")]
pub use udp::UdpClient;

#[cfg(feature = "client-tcp")]
pub mod tcp;
#[cfg(feature = "client-tcp")]
pub use tcp::TcpClient;

#[cfg(feature = "client-grpc")]
pub mod grpc;
#[cfg(feature = "client-grpc")]
pub use grpc::GrpcClient;
