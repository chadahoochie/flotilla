//! # Flotilla
//!
//! High-Throughput, Low-Latency Sans-I/O Raft Consensus in Safe Rust (Rust 1.98+ / Edition 2024).
//!
//! Flotilla is a deterministic, constant-memory distributed consensus engine engineered for
//! mission-critical systems where garbage collection pauses, memory allocator lock contention,
//! and asynchronous runtime stalls are unacceptable. By decoupling consensus logic from operating
//! system side effects (networking, clocks, and disk drives), Flotilla achieves microsecond-level
//! latency predictability and 100% reproducible simulation.
//!
//! ## Core Architectural Pillars
//!
//! 1. **Sans-I/O State Machine**: The consensus state engine ([`engine::RaftNode`]) contains no
//!    sockets, timers, or threads. State transitions occur strictly through deterministic functions:
//!    [`engine::RaftNode::step`], [`engine::RaftNode::tick`], and [`engine::RaftNode::propose`].
//! 2. **Zero-Allocation Hot Path**: Constant-memory execution along all steady-state consensus paths
//!    backed by compile-time power-of-two circular buffers ([`storage::RingBufferLogStorage`]),
//!    `zerocopy 0.8` transmutation, and cacheline alignment (`#[repr(align(64))]`).
//! 3. **Pluggable Transports**: Unified client proposal interface ([`client::FlotillaClient`]) and
//!    server transport listeners supporting UDP datagrams, async TCP streams, and HTTP/2 gRPC.
//! 4. **Type-per-File Modularity**: Every struct, enum, and trait resides in its own dedicated source file.
//! 5. **No Private Helper Methods**: Inherent `impl` blocks strictly prohibit private methods; all algorithms
//!    are decomposed into crate-visible standalone pure functions.
//!
//! ## Quickstart
//!
//! ```rust
//! use flotilla_raft::engine::{RaftConfig, RaftNode};
//! use flotilla_raft::types::NodeId;
//!
//! // Configure node 1 in a 3-node cluster
//! let config = RaftConfig {
//!     node_id: NodeId(1),
//!     peers: vec![NodeId(2), NodeId(3)],
//!     election_timeout_ticks: 10,
//!     heartbeat_interval_ticks: 3,
//! };
//!
//! // Instantiate node with 1024 slots and 1024-byte payload capacity
//! let mut node = RaftNode::<1024, 1024>::new(config);
//!
//! // Advance logical time by one tick
//! let outbound_messages = node.tick();
//! ```
//!
//! ## Pluggable Transports
//!
//! Under the `client-udp`, `tcp`, or `grpc` feature flags, Flotilla provides production-ready client
//! and server implementations:
//!
//! ```rust,no_run
//! use flotilla_raft::client::{FlotillaClient, UdpClient};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = UdpClient::connect("127.0.0.1:9001")?;
//! let result = client.propose(b"command_payload").await?;
//! if result.is_success() {
//!     println!("Accepted at index: {}", result.index.0);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature Flags
//!
//! - `client-udp` *(default)*: UDP datagram client ([`client::UdpClient`]).
//! - `client-tcp`: Async length-prefixed TCP client ([`client::TcpClient`]).
//! - `client-grpc`: HTTP/2 gRPC client ([`client::GrpcClient`]).
//! - `server-tcp`: Multi-client TCP server listener ([`server::TcpListener`]).
//! - `server-grpc`: HTTP/2 gRPC server service ([`server::GrpcService`]).
//! - `tcp`: Composite flag enabling `client-tcp` and `server-tcp`.
//! - `grpc`: Composite flag enabling `client-grpc` and `server-grpc`.
//! - `cosmos`: Azure Cosmos DB asynchronous archival offloader.
//! - `full`: Enables all transports and archival backends.

pub mod archive;
pub mod client;
pub mod codec;
pub mod commit;
pub mod election;
pub mod engine;
pub mod message;
pub mod replication;
pub mod server;
pub mod storage;
pub mod types;
pub mod udp;
