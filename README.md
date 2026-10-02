# Flotilla

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.98%2B%20(Edition%202024)-orange.svg)](Cargo.toml)
[![Sans-I/O](https://img.shields.io/badge/architecture-sans--I%2FO-brightgreen.svg)](CODING_STANDARDS.md#5-sans-io-consensus-model)
[![Zero-Allocation](https://img.shields.io/badge/hot--path-zero--allocation-success.svg)](CODING_STANDARDS.md#2-zero-allocation-hot-path-standards)
[![Coverage Gate](https://img.shields.io/badge/coverage%20gate-85%25%20line%20%7C%2090%25%20branch-blueviolet.svg)](.github/workflows/coverage.yml)

> **High-Throughput, Low-Latency Sans-I/O Raft Consensus in Safe Rust (Rust 1.98+ / Edition 2024).**

Flotilla is a deterministic, constant-memory distributed consensus engine engineered for mission-critical systems where garbage collection pauses, memory allocator lock contention, and asynchronous runtime stalls are unacceptable. By decoupling consensus logic from operating system side effects (networking, clocks, and disk drives), Flotilla achieves microsecond-level latency predictability and 100% reproducible simulation.

---

## 🏛️ Core Architectural Pillars

```mermaid
graph TD
    subgraph Network ["Pluggable Transport Layer"]
        UDP[UDP Driver / UdpListener / UdpClient]
        TCP[TCP Listener / TcpClient / Length Framing]
        GRPC[gRPC Service / GrpcClient / HTTP/2 Protobuf]
    end

    subgraph CodecLayer ["Wire & Codec Layer"]
        Codec[Zero-Copy Codec zerocopy 0.8]
        Framing[Stream & Datagram Framing]
    end

    UDP <--> Codec
    TCP <--> Framing
    Framing <--> Codec

    subgraph Core ["Deterministic Sans-I/O Core"]
        Engine[RaftNode Engine]
        Tick[Deterministic Ticker] -->|Manual Ticks| Engine
        Client[Client Proposals] -->|Raw Bytes & Redirection| Engine
        Codec -->|Decoded Packets| Engine
        Engine -->|Outbound Envelopes / ApplyEntries| Codec
    end

    GRPC <--> Engine

    subgraph Storage ["Zero-Alloc Storage Engine"]
        Engine -->|Append / Truncate| RingBuf[RingBufferLogStorage]
        RingBuf -->|Committed Entries| Pipe[ArchivePipeline]
    end

    subgraph Archival ["Async Archival Sinks"]
        Pipe -->|Non-blocking Channel| Disk[FileArchiveSink / WAL]
        Pipe -->|Optional Feature| Cosmos[CosmosArchiveSink]
        Disk -->|Compaction Watermark| Engine
        Cosmos -->|Compaction Watermark| Engine
    end
```

### 1. Sans-I/O State Machine
The core consensus engine imports no socket primitives (`std::net`), no system clocks (`Instant::now`), and no thread-spawning runtimes (`std::thread`, `tokio`). All state transitions occur through deterministic memory functions:
- `RaftNode::step(sender, packet_bytes) -> Result<Vec<OutboundMessage>, EngineError>`
- `RaftNode::tick() -> Vec<OutboundMessage>`
- `RaftNode::propose(payload) -> Result<LogIndex, EngineError>`

### 2. Zero-Allocation Hot Path Execution
Steady-state log appending, follower AppendEntries validation, packet framing, and election timer updates execute with **zero dynamic heap allocations**. Memory is backed by pre-allocated, power-of-two static circular buffers and borrowed slices.

### 3. Pluggable Client & Server Transports
Consensus nodes easily integrate across heterogeneous networking layers through modular client drivers and server listeners:
- **UDP**: Lightweight datagram client (`UdpClient`), polled listener (`UdpListener`), and socket driver (`UdpDriver`).
- **TCP**: Connection-oriented client (`TcpClient`), async connection listener (`TcpListener`), and length-prefixed stream framing.
- **gRPC**: Multi-language HTTP/2 client (`GrpcClient`) and service (`GrpcService`) powered by Tonic and Prost.

### 4. Type-per-File Modular Decomposition
Every `struct`, `enum`, and `trait` is strictly isolated into its own dedicated source file matching its snake_case name. No sprawling god-files or tangled multi-type definitions.

### 5. Prohibition of Private Helper Methods
Inherent `impl` blocks strictly prohibit private helper methods (`fn helper(&self)`). Algorithmic evaluations, rules, and packet builders are decomposed into standalone, crate-visible (`pub` or `pub(crate)`) pure functions. This enables 100% isolated unit testability without mocking.

### 6. Test-Driven Development with Heavy Focus on Refactoring
Every feature follows the **Red -> Green -> Refactor** cycle. The **Refactor** phase is the primary engineering driver: eliminating allocations, verifying cacheline alignment, decomposing into single types, and enforcing zero compiler warnings.

---

## 📋 Best Practices & Engineering Standards

Flotilla adheres to strict, non-negotiable coding standards. For the complete specification and guidelines, see [**`CODING_STANDARDS.md`**](file:///home/chad/source/rust/flotilla/CODING_STANDARDS.md).

### 1. Test-Driven Development (TDD: Red / Green / Refactor)

```
🔴 RED      ──> Write a failing unit / invariant test specifying exact requirements.
🟢 GREEN    ──> Implement minimal code necessary to make the test pass.
🔵 REFACTOR ──> Deep engineering audit: eliminate allocations, align cachelines,
                decompose into single types, eliminate private helpers, fix clippy.
```

- **Red**: Tests are authored *before* implementation logic.
- **Green**: Minimal, unbloated code to satisfy the test.
- **Refactor (The Priority)**: Refactoring is where architectural excellence is achieved. Code is audited for zero allocations, cacheline alignment, modularity, and zero clippy warnings before submitting.
- **Coverage Standard**: Strict CI enforcement of **$\ge 85\%$ line coverage** and **$\ge 90\%$ branch coverage**.

### 2. Zero Steady-State Allocation Standard

- **No Heap Calls on Hot Path**: Never call `Vec::new`, `Vec::push`, `Box::new`, `format!`, `String`, or `.to_vec()` on consensus hot paths.
- **Power-of-Two Ring Buffers**: Static capacity verified at compile-time via:
  ```rust
  const { assert!(N > 0 && (N & (N - 1)) == 0, "Capacity must be a power of two") }
  ```
  Enables branchless $O(1)$ bitwise masking (`(index - 1) & (CAPACITY - 1)`).
- **Cacheline Alignment**: High-contention slots derive `#[repr(align(64))]` to prevent CPU cacheline bouncing.
- **Safe Transmutation**: Wire packets derive `zerocopy 0.8` (`FromBytes`, `IntoBytes`, `Immutable`, `KnownLayout`). `unsafe std::mem::transmute` is forbidden.
- **Automated Verification**: The test suite includes a custom `#[global_allocator]` tracking harness in [`tests/zero_alloc_tests.rs`](file:///home/chad/source/rust/flotilla/tests/zero_alloc_tests.rs) asserting 0 allocations across all hot paths.

### 3. Type-per-File Decomposition Standard

- Exactly **one primary type per file** named in `snake_case` matching the type's `PascalCase` name.
- Domain modules under `src/` organize types cleanly:
  - Primitives: [`NodeId`](file:///home/chad/source/rust/flotilla/src/types/node_id.rs), [`Term`](file:///home/chad/source/rust/flotilla/src/types/term.rs), [`LogIndex`](file:///home/chad/source/rust/flotilla/src/types/log_index.rs), [`Role`](file:///home/chad/source/rust/flotilla/src/types/role.rs), [`HardState`](file:///home/chad/source/rust/flotilla/src/types/hard_state.rs)
  - Messages: [`MsgType`](file:///home/chad/source/rust/flotilla/src/message/msg_type.rs), [`RequestVoteArgs`](file:///home/chad/source/rust/flotilla/src/message/request_vote_args.rs), [`RequestVoteReply`](file:///home/chad/source/rust/flotilla/src/message/request_vote_reply.rs), [`AppendEntriesHeader`](file:///home/chad/source/rust/flotilla/src/message/append_entries_header.rs), [`AppendEntriesReply`](file:///home/chad/source/rust/flotilla/src/message/append_entries_reply.rs), [`ClientProposalReply`](file:///home/chad/source/rust/flotilla/src/message/client_proposal_reply.rs), [`RaftMessage`](file:///home/chad/source/rust/flotilla/src/message/raft_message.rs)
  - Codec: [`PacketHeader`](file:///home/chad/source/rust/flotilla/src/codec/packet_header.rs), [`CodecError`](file:///home/chad/source/rust/flotilla/src/codec/codec_error.rs)
  - Storage: [`LogSlot`](file:///home/chad/source/rust/flotilla/src/storage/log_slot.rs), [`StorageError`](file:///home/chad/source/rust/flotilla/src/storage/storage_error.rs), [`RingBufferLogStorage`](file:///home/chad/source/rust/flotilla/src/storage/ring_buffer_log_storage.rs)
  - Election: [`ElectionAction`](file:///home/chad/source/rust/flotilla/src/election/election_action.rs), [`ElectionConfig`](file:///home/chad/source/rust/flotilla/src/election/election_config.rs), [`ElectionState`](file:///home/chad/source/rust/flotilla/src/election/election_state.rs)
  - Replication: [`PeerProgress`](file:///home/chad/source/rust/flotilla/src/replication/peer_progress.rs), [`PeerProgressTracker`](file:///home/chad/source/rust/flotilla/src/replication/peer_progress_tracker.rs), [`FollowerAppendResult`](file:///home/chad/source/rust/flotilla/src/replication/follower_append_result.rs)
  - Engine: [`RaftConfig`](file:///home/chad/source/rust/flotilla/src/engine/raft_config.rs), [`EngineError`](file:///home/chad/source/rust/flotilla/src/engine/engine_error.rs), [`OutboundMessage`](file:///home/chad/source/rust/flotilla/src/engine/outbound_message.rs), [`RaftNode`](file:///home/chad/source/rust/flotilla/src/engine/raft_node.rs)
  - Client: [`FlotillaClient`](file:///home/chad/source/rust/flotilla/src/client/flotilla_client.rs), [`ClientConfig`](file:///home/chad/source/rust/flotilla/src/client/client_config.rs), [`ClientError`](file:///home/chad/source/rust/flotilla/src/client/client_error.rs), [`ProposalResult`](file:///home/chad/source/rust/flotilla/src/client/proposal_result.rs), [`UdpClient`](file:///home/chad/source/rust/flotilla/src/client/udp/udp_client.rs), [`TcpClient`](file:///home/chad/source/rust/flotilla/src/client/tcp/tcp_client.rs), [`GrpcClient`](file:///home/chad/source/rust/flotilla/src/client/grpc/grpc_client.rs)
  - Server: [`ServerConfig`](file:///home/chad/source/rust/flotilla/src/server/server_config.rs), [`ServerError`](file:///home/chad/source/rust/flotilla/src/server/server_error.rs), [`UdpListener`](file:///home/chad/source/rust/flotilla/src/server/udp_listener.rs), [`TcpListener`](file:///home/chad/source/rust/flotilla/src/server/tcp_listener.rs), [`GrpcService`](file:///home/chad/source/rust/flotilla/src/server/grpc_service.rs)
  - Archival: [`ArchivedEntry`](file:///home/chad/source/rust/flotilla/src/archive/archived_entry.rs), [`AsyncArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/async_archive_sink.rs), [`PipelineError`](file:///home/chad/source/rust/flotilla/src/archive/pipeline_error.rs), [`ArchivePipeline`](file:///home/chad/source/rust/flotilla/src/archive/archive_pipeline.rs), [`FileArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/file_archive_sink.rs), [`NullArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/null_archive_sink.rs)
  - UDP: [`UdpDriver`](file:///home/chad/source/rust/flotilla/src/udp/udp_driver.rs), [`UdpClusterRouter`](file:///home/chad/source/rust/flotilla/src/udp/udp_cluster_router.rs)

### 4. Prohibition of Private Helper Methods Standard

- **No Private Methods in `impl`**: Every method in an inherent `impl Struct` block is crate-visible (`pub` or `pub(crate)`).
- **Standalone Pure Functions**: Internal algorithms are extracted to standalone pure functions in dedicated files:
  - [`rules.rs`](file:///home/chad/source/rust/flotilla/src/election/rules.rs): Quorum calculation and log freshness checks.
  - [`evaluator.rs`](file:///home/chad/source/rust/flotilla/src/replication/evaluator.rs): Follower AppendEntries verification.
  - [`commit.rs`](file:///home/chad/source/rust/flotilla/src/commit.rs): Quorum median calculations and commit advancement.
  - [`packets.rs`](file:///home/chad/source/rust/flotilla/src/engine/packets.rs): Outbound datagram packet construction.
  - [`udp/framing.rs`](file:///home/chad/source/rust/flotilla/src/udp/framing.rs): UDP MTU bounds checks.
  - [`client/tcp/framing.rs`](file:///home/chad/source/rust/flotilla/src/client/tcp/framing.rs): Async TCP length-prefixed frame encoding and decoding.
- **Granular Testability**: Because functions are pure and public, every edge case can be tested with 100% isolation.

---

## 📦 Cargo Feature Flags

Flotilla provides modular feature flags so applications only compile the transports and integrations they require:

| Feature Flag | Default | Description | Key Dependencies |
| :--- | :---: | :--- | :--- |
| `default` | Yes | Standard installation | `client-udp` |
| `client-udp` | Yes | UDP datagram client (`UdpClient`) | Standard library only |
| `client-tcp` | No | Async TCP client (`TcpClient`) | `tokio` |
| `client-grpc` | No | HTTP/2 gRPC client (`GrpcClient`) | `tonic`, `prost` |
| `server-tcp` | No | Async TCP server listener (`TcpListener`) | `tokio` |
| `server-grpc` | No | HTTP/2 gRPC server service (`GrpcService`) | `tonic`, `prost` |
| `udp` | No | Full UDP support | `client-udp` |
| `tcp` | No | Full TCP client and server support | `client-tcp`, `server-tcp`, `tokio` |
| `grpc` | No | Full gRPC client and server support | `client-grpc`, `server-grpc`, `tonic`, `prost` |
| `cosmos` | No | Azure Cosmos DB archival offloader | `reqwest`, `tokio`, `serde`, `hmac`, `sha2` |
| `full` | No | All transports and archival sinks | `udp`, `tcp`, `grpc`, `cosmos` |

---

## 🚀 Quickstart

### 1. Add Flotilla to `Cargo.toml`

```toml
[dependencies]
# Default: UDP client
flotilla = "0.1"

# Enable TCP client and server
# flotilla = { version = "0.1", features = ["tcp"] }

# Enable full features (UDP, TCP, gRPC, Cosmos DB offloader)
# flotilla = { version = "0.1", features = ["full"] }
```

### 2. Basic Engine Instantiation

```rust
use flotilla::engine::{RaftConfig, RaftNode};
use flotilla::types::NodeId;

// Configure node 1 in a 3-node cluster
let config = RaftConfig {
    node_id: NodeId(1),
    peers: vec![NodeId(2), NodeId(3)],
    election_timeout_ticks: 10,
    heartbeat_interval_ticks: 3,
};

// Instantiate node with 1024 slots and 1024-byte payload capacity
let mut node = RaftNode::<1024, 1024>::new(config);

// Trigger a logical timer tick
let outbound_messages = node.tick();
```

### 3. Client Proposal Submission

```rust,ignore
use flotilla::client::{FlotillaClient, UdpClient, TcpClient, GrpcClient};

// Submit a proposal over UDP, TCP, or gRPC
let client = UdpClient::connect("127.0.0.1:9001")?;
let res = client.propose(b"payload_data").await?;

if res.is_success() {
    println!("Accepted at index: {}, term: {}", res.index.0, res.term.0);
} else if let Some(leader) = res.leader_id {
    println!("Redirect proposal to leader node {}", leader.0);
}
```

### 4. Running a Server Transport Listener

```rust,ignore
use flotilla::engine::{RaftConfig, RaftNode};
use flotilla::server::TcpListener;
use parking_lot::Mutex;
use std::sync::Arc;

let config = RaftConfig::default();
let node = Arc::new(Mutex::new(RaftNode::<1024, 1024>::new(config)));
let listener = TcpListener::bind("0.0.0.0:9001".parse()?, Arc::clone(&node)).await?;
```

---

## 🧪 Testing, Benchmarks & Quality Gates

Flotilla enforces comprehensive quality checks across the entire codebase.

### Running the Test Suite
```bash
# Run all unit, integration, and default transport tests
cargo test --all-targets

# Run tests across all feature flags (UDP, TCP, gRPC, Cosmos)
cargo test --all-targets --all-features
```

### Zero-Allocation Verification Gate
```bash
# Verifies 0 heap allocations across all consensus hot paths via TrackingAllocator
cargo test --test zero_alloc_tests
```

### Coding Standards Enforcement Gate
```bash
# Programmatic structural verification of no-private-helpers and type-per-file
python3 .github/scripts/check_coding_standards.py

# Rust test enforcing structural invariants
cargo test --test coding_standards_tests
```

### Coverage Gate Enforcement (85% Line / 90% Branch)
```bash
cargo llvm-cov --branch --html --output-dir target/llvm-cov/html
cargo llvm-cov report --branch --json --summary-only --output-path coverage.json
python3 .github/scripts/check_coverage.py coverage.json --min-line 85.0 --min-branch 90.0
```

### Clippy Hygiene & Documentation
```bash
# Enforce zero clippy warnings across all features
cargo clippy --all-targets --all-features -- -D warnings

# Verify documentation builds cleanly
cargo doc --all-features --no-deps
```

### Performance Benchmarks
```bash
cargo bench
```

---

## 🐳 Deployment & Containerization

### Running Flotilla Server Locally
The `flotilla-server` executable runs an integrated consensus daemon exposing UDP, TCP, and gRPC transports:

```bash
# Build release binary
cargo build --release --bin flotilla-server --features full

# Run node 1 with default transports
./target/release/flotilla-server \
  --node-id 1 \
  --udp-addr 0.0.0.0:9000 \
  --tcp-addr 0.0.0.0:9001 \
  --grpc-addr 0.0.0.0:50051 \
  --peers 2=10.0.0.2:9000,3=10.0.0.3:9000
```

### Running with Docker
A multi-stage, non-root container image is published automatically to GitHub Container Registry (`ghcr.io/chadahoochie/flotilla`):

```bash
# Pull and run container
docker run -d \
  --name flotilla-node-1 \
  -p 9000:9000/udp \
  -p 9001:9001/tcp \
  -p 50051:50051/tcp \
  -e FLOTILLA_NODE_ID=1 \
  -e FLOTILLA_PEERS="2=10.0.0.2:9000,3=10.0.0.3:9000" \
  ghcr.io/chadahoochie/flotilla:latest
```

---

## 🚀 CI/CD Pipelines

GitHub Actions workflows are located in [`.github/workflows/`](file:///home/chad/source/rust/flotilla/.github/workflows/):

- [`coverage.yml`](file:///home/chad/source/rust/flotilla/.github/workflows/coverage.yml): Enforces 85% line and 90% branch coverage gates, zero-allocation tests, and architectural standards on PRs and `main`.
- [`crates.yml`](file:///home/chad/source/rust/flotilla/.github/workflows/crates.yml): Matrix compilation and test suites across all feature modules (`--no-default-features`, `client-udp`, `tcp`, `grpc`, `cosmos`, `full`), clippy linting, rustfmt checks, and `cargo package` dry-run verification.
- [`docker-publish.yml`](file:///home/chad/source/rust/flotilla/.github/workflows/docker-publish.yml): Builds multi-arch container image with Docker Buildx and publishes signed images to GitHub Container Registry on pushes to `main` and semantic version tags (`v*`).

---

## 📁 Repository Map

```
flotilla/
├── CODING_STANDARDS.md         # Comprehensive standards & architectural invariants
├── README.md                   # Primary developer guide & best practices
├── Cargo.toml                  # Dependencies, feature flags, profile settings
├── Dockerfile                  # Multi-stage release container specification
├── .dockerignore               # Docker build context exclusions
├── LICENSE                     # MIT License
├── proto/
│   └── flotilla.proto          # Protocol Buffers definition for gRPC service
├── src/
│   ├── lib.rs                  # Library entrypoint and public module exports
│   ├── bin/
│   │   └── flotilla-server.rs  # Integrated server binary daemon (UDP/TCP/gRPC)
│   ├── commit.rs               # Standalone quorum median commit evaluator
│   ├── types/                  # Single-type scalar wrappers (NodeId, Term, etc.)
│   ├── message/                # Wire message structures (RequestVoteArgs, ClientProposalReply, etc.)
│   ├── codec/                  # PacketHeader (zerocopy 0.8), CodecError, framing
│   ├── storage/                # Power-of-two RingBufferLogStorage, LogSlot
│   ├── election/               # ElectionState, ElectionConfig, rules.rs
│   ├── replication/            # PeerProgressTracker, evaluator.rs
│   ├── engine/                 # RaftNode, RaftConfig, packets.rs, OutboundMessage
│   ├── client/                 # FlotillaClient trait, UdpClient, TcpClient, GrpcClient
│   │   ├── tcp/                # TCP length-prefixed framing and client driver
│   │   ├── udp/                # UDP datagram client driver
│   │   └── grpc/               # Tonic HTTP/2 gRPC client driver
│   ├── server/                 # Transport server listeners (UdpListener, TcpListener, GrpcService)
│   ├── archive/                # Async archival pipeline, FileArchiveSink, NullArchiveSink
│   │   └── cosmos/             # Optional Cosmos DB offloader subsystem
│   └── udp/                    # Non-blocking UdpDriver, UdpClusterRouter, framing.rs
├── tests/
│   ├── zero_alloc_tests.rs     # Tracking allocator hot path verification
│   ├── coding_standards_tests.rs # Automated structural standards tests
│   ├── client_udp_tests.rs     # UDP client & listener integration tests
│   ├── client_tcp_tests.rs     # TCP client & listener integration tests
│   ├── client_grpc_tests.rs    # gRPC client & service integration tests
│   ├── archive_tests.rs        # Pipeline and WAL sink tests
│   ├── cluster_tests.rs        # Multi-node loopback cluster tests
│   ├── codec_tests.rs          # Zero-copy serialization roundtrip tests
│   ├── commit_tests.rs         # Quorum median math tests
│   ├── election_tests.rs       # Election timeouts and voting rules tests
│   ├── engine_tests.rs         # Sans-I/O node stepping and proposal tests
│   ├── replication_tests.rs    # Follower log matching evaluator tests
│   ├── storage_tests.rs        # Ring buffer wrap-around and compaction tests
│   └── coverage_expansion_tests.rs # Edge cases ensuring 85%/90% coverage
└── .github/
    ├── scripts/
    │   ├── check_coding_standards.py # Automated standards verification script
    │   └── check_coverage.py         # Coverage gate evaluation script
    └── workflows/
        ├── coverage.yml        # GitHub Actions coverage & standards workflow
        ├── crates.yml          # GitHub Actions crate builds & module matrix
        └── docker-publish.yml  # GitHub Actions Docker publish to GHCR
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2026 Chad Bauers.
