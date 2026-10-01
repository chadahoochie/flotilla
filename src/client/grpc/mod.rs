//! gRPC client implementation for Flotilla consensus.

#[allow(clippy::all)]
pub mod mod_proto {
    tonic::include_proto!("flotilla");
}

pub mod grpc_client;

pub use grpc_client::GrpcClient;
pub use mod_proto as proto;
