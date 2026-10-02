# Flotilla Developer and User Guide

Flotilla is a zero-allocation, sans-I/O Raft consensus library implemented in Safe Rust (Rust 1.98+ / Edition 2024).

For the top-level repository overview, see [**`README.md`**](../README.md).  
For the complete engineering and code quality specification, see [**`CODING_STANDARDS.md`**](../CODING_STANDARDS.md).

---

## Non-Negotiable Coding Standards

1. **TDD (Red -> Green -> Refactor)**: Write failing tests first. Heavily prioritize the **Refactor** step to eliminate allocations, verify cacheline alignment, enforce type-per-file, and eliminate clippy warnings.
2. **Zero Allocation**: Constant-memory execution along all steady-state consensus hot paths using compile-time power-of-two circular buffers, `zerocopy 0.8`, and cacheline alignment (`#[repr(align(64))]`).
3. **Type-per-File Decomposition**: Every struct, enum, and trait resides in its own dedicated, snake_case source file.
4. **No Private Helper Methods**: Inherent `impl` blocks prohibit private methods. All logic is decomposed into crate-visible standalone pure functions or dedicated types for 100% isolated unit testability.

---

## Key Features

- **Sans-I/O Architecture**: The consensus state engine has no socket handles, file descriptors, or timer threads. It is stepped purely via in-memory events (`step`), proposals (`propose`), and manual ticks (`tick`).
- **Zero-Allocation Hot Path**: Constant-memory execution along hot paths using static circular arrays and borrowed slices.
- **Pluggable Client Transports**: Unified [`FlotillaClient`](file:///home/chad/source/rust/flotilla/src/client/flotilla_client.rs) interface supporting:
  - UDP datagrams (`UdpClient`)
  - Async length-prefixed TCP streams (`TcpClient`)
  - High-performance HTTP/2 gRPC (`GrpcClient`)
- **Server Transport Listeners**: Pluggable protocol server listeners driving shared consensus nodes:
  - [`UdpListener`](file:///home/chad/source/rust/flotilla/src/server/udp_listener.rs): Polled non-blocking UDP datagram engine driver.
  - [`TcpListener`](file:///home/chad/source/rust/flotilla/src/server/tcp_listener.rs): Async multi-client TCP connection pool listener.
  - [`GrpcService`](file:///home/chad/source/rust/flotilla/src/server/grpc_service.rs): HTTP/2 gRPC service implementing `Propose`, `Step`, and `ClusterStatus`.
- **Client Proposals & Leader Redirection**: Inbound proposals automatically append to the leader or reject with a leader hint for transparent client redirection.
- **Strictly Modular (Type-per-File)**: Every struct, enum, and trait is decomposed into its own dedicated file without hidden private helper routines.
- **Pluggable Archival**: Stream committed entries to disk WALs or Azure Cosmos DB asynchronously without blocking consensus replication.
- **Safe Zero-Copy Wire Protocol**: Binary datagram framing built on `zerocopy 0.8` without `unsafe`.

---

## Codebase Organization (Type-per-File)

All domain primitives, message models, protocol frames, and consensus logic are separated into dedicated, single-type files within submodules, with top-level re-exports:

- **`types/`**:
  - [`NodeId`](file:///home/chad/source/rust/flotilla/src/types/node_id.rs): Cluster node identifier.
  - [`Term`](file:///home/chad/source/rust/flotilla/src/types/term.rs): Monotonic Raft term counter.
  - [`LogIndex`](file:///home/chad/source/rust/flotilla/src/types/log_index.rs): 1-based replicated log index.
  - [`Role`](file:///home/chad/source/rust/flotilla/src/types/role.rs): Node role (`Follower`, `Candidate`, `Leader`).
  - [`HardState`](file:///home/chad/source/rust/flotilla/src/types/hard_state.rs): Persisted crash-recovery state.
- **`message/`**:
  - [`MsgType`](file:///home/chad/source/rust/flotilla/src/message/msg_type.rs): Wire message discriminant.
  - [`RequestVoteArgs`](file:///home/chad/source/rust/flotilla/src/message/request_vote_args.rs): Vote request parameters.
  - [`RequestVoteReply`](file:///home/chad/source/rust/flotilla/src/message/request_vote_reply.rs): Vote response payload.
  - [`AppendEntriesHeader`](file:///home/chad/source/rust/flotilla/src/message/append_entries_header.rs): Fixed replication RPC header.
  - [`AppendEntriesReply`](file:///home/chad/source/rust/flotilla/src/message/append_entries_reply.rs): Replication acknowledgment.
  - [`ClientProposalReply`](file:///home/chad/source/rust/flotilla/src/message/client_proposal_reply.rs): Client proposal acknowledgment with leader hint.
  - [`RaftMessage`](file:///home/chad/source/rust/flotilla/src/message/raft_message.rs): High-level strongly-typed Raft message enum.
- **`codec/`**:
  - [`PacketHeader`](file:///home/chad/source/rust/flotilla/src/codec/packet_header.rs): 40-byte fixed zero-copy packet header.
  - [`CodecError`](file:///home/chad/source/rust/flotilla/src/codec/codec_error.rs): Encoding/decoding error types.
  - [`mod.rs`](file:///home/chad/source/rust/flotilla/src/codec/mod.rs): Wire framing, CRC32 checksums, and zero-copy packet encoders/parsers.
- **`storage/`**:
  - [`LogSlot`](file:///home/chad/source/rust/flotilla/src/storage/log_slot.rs): Cacheline-aligned (64-byte) log entry slot.
  - [`StorageError`](file:///home/chad/source/rust/flotilla/src/storage/storage_error.rs): Ring buffer capacity and bounds errors.
  - [`RingBufferLogStorage`](file:///home/chad/source/rust/flotilla/src/storage/ring_buffer_log_storage.rs): Power-of-two circular buffer log storage.
- **`election/`**:
  - [`ElectionAction`](file:///home/chad/source/rust/flotilla/src/election/election_action.rs): Actions emitted by the election subsystem.
  - [`ElectionConfig`](file:///home/chad/source/rust/flotilla/src/election/election_config.rs): Node and cluster election parameters.
  - [`ElectionState`](file:///home/chad/source/rust/flotilla/src/election/election_state.rs): Election timer and vote tracking state machine.
  - [`rules.rs`](file:///home/chad/source/rust/flotilla/src/election/rules.rs): Quorum size, vote eligibility, and up-to-date checks.
- **`replication/`**:
  - [`PeerProgress`](file:///home/chad/source/rust/flotilla/src/replication/peer_progress.rs): Replication indices for a peer node.
  - [`PeerProgressTracker`](file:///home/chad/source/rust/flotilla/src/replication/peer_progress_tracker.rs): Peer match/next progress manager.
  - [`FollowerAppendResult`](file:///home/chad/source/rust/flotilla/src/replication/follower_append_result.rs): Follower AppendEntries evaluation outcome.
  - [`evaluator.rs`](file:///home/chad/source/rust/flotilla/src/replication/evaluator.rs): Standalone follower log append evaluator.
- **`commit.rs`**: Quorum median calculations and commit advancement evaluation.
- **`engine/`**:
  - [`EngineError`](file:///home/chad/source/rust/flotilla/src/engine/engine_error.rs): State machine error definitions.
  - [`OutboundMessage`](file:///home/chad/source/rust/flotilla/src/engine/outbound_message.rs): Actions emitted by the engine (`SendPacket`, `ApplyEntries`).
  - [`RaftConfig`](file:///home/chad/source/rust/flotilla/src/engine/raft_config.rs): Consensus node configuration options.
  - [`RaftNode`](file:///home/chad/source/rust/flotilla/src/engine/raft_node.rs): Top-level sans-I/O Raft consensus engine.
  - [`packets.rs`](file:///home/chad/source/rust/flotilla/src/engine/packets.rs): Standalone packet and proposal construction pure functions.
- **`client/`**:
  - [`FlotillaClient`](file:///home/chad/source/rust/flotilla/src/client/flotilla_client.rs): Unified async client proposal trait (`propose`, `ping`).
  - [`ClientConfig`](file:///home/chad/source/rust/flotilla/src/client/client_config.rs): Client connection endpoints, timeout, and retry settings.
  - [`ClientError`](file:///home/chad/source/rust/flotilla/src/client/client_error.rs): Transport errors, timeouts, and codec errors.
  - [`ProposalResult`](file:///home/chad/source/rust/flotilla/src/client/proposal_result.rs): Proposal outcome (`success`, `index`, `term`, `leader_id`).
  - [`UdpClient`](file:///home/chad/source/rust/flotilla/src/client/udp/udp_client.rs): Datagram client driver.
  - [`TcpClient`](file:///home/chad/source/rust/flotilla/src/client/tcp/tcp_client.rs): Length-prefixed TCP client driver.
  - [`GrpcClient`](file:///home/chad/source/rust/flotilla/src/client/grpc/grpc_client.rs): HTTP/2 gRPC client driver.
  - [`framing.rs`](file:///home/chad/source/rust/flotilla/src/client/tcp/framing.rs): Async TCP frame reader and writer.
- **`server/`**:
  - [`ServerConfig`](file:///home/chad/source/rust/flotilla/src/server/server_config.rs): Server bind address and connection limits.
  - [`ServerError`](file:///home/chad/source/rust/flotilla/src/server/server_error.rs): Network I/O and protocol server error types.
  - [`UdpListener`](file:///home/chad/source/rust/flotilla/src/server/udp_listener.rs): Polled non-blocking UDP datagram engine driver.
  - [`TcpListener`](file:///home/chad/source/rust/flotilla/src/server/tcp_listener.rs): Async multi-client TCP connection pool listener.
  - [`GrpcService`](file:///home/chad/source/rust/flotilla/src/server/grpc_service.rs): HTTP/2 gRPC server service wrapping `RaftNode`.
- **`archive/`**:
  - [`ArchivedEntry`](file:///home/chad/source/rust/flotilla/src/archive/archived_entry.rs): Committed entry payload for offloading.
  - [`AsyncArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/async_archive_sink.rs): Storage target trait (`write_entries`, `flush`).
  - [`PipelineError`](file:///home/chad/source/rust/flotilla/src/archive/pipeline_error.rs): Pipeline queue error types.
  - [`ArchivePipeline`](file:///home/chad/source/rust/flotilla/src/archive/archive_pipeline.rs): Buffered asynchronous archival pipeline.
  - [`NullArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/null_archive_sink.rs): In-memory discarding sink.
  - [`FileArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/file_archive_sink.rs): Append-only WAL file sink with CRC verification.
  - `cosmos/`: Optional Azure Cosmos DB archival offloader subsystem.
- **`udp/`**:
  - [`UdpDriver`](file:///home/chad/source/rust/flotilla/src/udp/udp_driver.rs): Lightweight non-blocking socket driver wrapper.
  - [`UdpClusterRouter`](file:///home/chad/source/rust/flotilla/src/udp/udp_cluster_router.rs): Bidirectional `NodeId` to `SocketAddr` address mapping.
  - [`framing.rs`](file:///home/chad/source/rust/flotilla/src/udp/framing.rs): UDP MTU bounds checks.

---

## Cargo Feature Flags

Flotilla supports modular compilation flags:

| Feature Flag | Default | Included Capabilities |
| :--- | :---: | :--- |
| `default` | Yes | Includes `client-udp` |
| `client-udp` | Yes | UDP datagram client (`UdpClient`) |
| `client-tcp` | No | Async TCP client (`TcpClient`, `tokio`) |
| `client-grpc` | No | HTTP/2 gRPC client (`GrpcClient`, `tonic`, `prost`) |
| `server-tcp` | No | Async TCP server listener (`TcpListener`, `tokio`) |
| `server-grpc` | No | HTTP/2 gRPC server service (`GrpcService`, `tonic`, `prost`) |
| `udp` | No | Composite flag enabling `client-udp` |
| `tcp` | No | Composite flag enabling `client-tcp` and `server-tcp` |
| `grpc` | No | Composite flag enabling `client-grpc` and `server-grpc` |
| `cosmos` | No | Azure Cosmos DB archival offloader |
| `full` | No | All features: `udp`, `tcp`, `grpc`, and `cosmos` |

---

## Usage Guide

### 1. Sans-I/O Engine Instantiation

```rust
use flotilla_raft::engine::{RaftConfig, RaftNode};
use flotilla_raft::types::NodeId;

// Configure node 1 in a 3-node cluster
let config = RaftConfig {
    node_id: NodeId(1),
    peers: vec![NodeId(2), NodeId(3)],
    election_timeout_ticks: 10,
    heartbeat_interval_ticks: 3,
};

// Instantiate node with 1024 slots and 1024-byte maximum payload
let mut node = RaftNode::<1024, 1024>::new(config);

// Trigger a logical timer tick
let outbound_messages = node.tick();
```

### 2. Client Proposal Submission

Submitting a proposal using the [`FlotillaClient`](file:///home/chad/source/rust/flotilla/src/client/flotilla_client.rs) interface:

```rust,ignore
use flotilla_raft::client::{FlotillaClient, UdpClient, TcpClient, GrpcClient};

// Connect via UDP
let udp_client = UdpClient::connect("127.0.0.1:9001")?;
let result = udp_client.propose(b"set_key:value").await?;

if result.is_success() {
    println!("Committed at log index {} in term {}", result.index.0, result.term.0);
} else if let Some(leader) = result.leader_id {
    println!("Not leader. Redirect to node {}", leader.0);
}
```

### 3. Server Transport Listeners

#### Async TCP Server Listener
```rust,ignore
use flotilla_raft::engine::{RaftConfig, RaftNode};
use flotilla_raft::server::TcpListener;
use parking_lot::Mutex;
use std::sync::Arc;

let config = RaftConfig::default();
let node = Arc::new(Mutex::new(RaftNode::<1024, 1024>::new(config)));
let listener = TcpListener::bind("0.0.0.0:9001".parse()?, Arc::clone(&node)).await?;
```

#### HTTP/2 gRPC Service
```rust,ignore
use flotilla_raft::engine::{RaftConfig, RaftNode};
use flotilla_raft::server::GrpcService;
use parking_lot::Mutex;
use std::sync::Arc;

let node = Arc::new(Mutex::new(RaftNode::<1024, 1024>::new(RaftConfig::default())));
let service = GrpcService::new(node);
service.serve("0.0.0.0:50051".parse()?).await?;
```

---

## Running Tests & Quality Gates

```bash
# Run all unit and integration tests (default features)
cargo test --all-targets

# Run tests across all feature flags (UDP, TCP, gRPC, Cosmos)
cargo test --all-targets --all-features

# Run zero-allocation hot path verification test suite
cargo test --test zero_alloc_tests

# Enforce coding standards (no-private-helpers, type-per-file)
python3 .github/scripts/check_coding_standards.py
cargo test --test coding_standards_tests

# Check for clippy warnings across all features
cargo clippy --all-targets --all-features -- -D warnings

# Verify documentation generation
cargo doc --all-features --no-deps

# Verify crate packaging
cargo package --no-verify

# Build release server binary and Docker container
cargo build --release --bin flotilla-server --features full
docker build -t flotilla:latest .

# Run performance benchmarks
cargo bench
```
