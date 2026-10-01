//! Server transport listeners and protocol services for Flotilla nodes.

pub mod server_config;
pub mod server_error;

pub use server_config::ServerConfig;
pub use server_error::ServerError;

pub mod udp_listener;
pub use udp_listener::UdpListener;

#[cfg(feature = "server-tcp")]
pub mod tcp_listener;
#[cfg(feature = "server-tcp")]
pub use tcp_listener::TcpListener;

#[cfg(feature = "server-grpc")]
#[allow(clippy::all)]
pub mod mod_proto {
    tonic::include_proto!("flotilla");
}

#[cfg(feature = "server-grpc")]
pub mod grpc_service;
#[cfg(feature = "server-grpc")]
pub use grpc_service::GrpcService;
#[cfg(feature = "server-grpc")]
pub use mod_proto as proto;
