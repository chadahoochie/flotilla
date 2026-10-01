# Flotilla Developer and User Guide

Flotilla is a zero-allocation, sans-I/O Raft consensus library implemented in Safe Rust (Rust 1.98+ / Edition 2024).

For the top-level repository overview, see [**`README.md`**](../README.md).  
For the complete engineering and code quality specification, see [**`CODING_STANDARDS.md`**](../CODING_STANDARDS.md).

## Non-Negotiable Coding Standards
1. **TDD (Red -> Green -> Refactor)**: Write failing tests first. Heavily prioritize the **Refactor** step to eliminate allocations, verify cacheline alignment, enforce type-per-file, and eliminate clippy warnings.
2. **Zero Allocation**: Constant-memory execution along all steady-state consensus hot paths using compile-time power-of-two circular buffers, `zerocopy 0.8`, and cacheline alignment (`#[repr(align(64))]`).
3. **Type-per-File Decomposition**: Every struct, enum, and trait resides in its own dedicated, snake_case source file.
4. **No Private Helper Methods**: Inherent `impl` blocks prohibit private methods. All logic is decomposed into crate-visible standalone pure functions or dedicated types for 100% isolated unit testability.

## Key Features
- **Sans-I/O Architecture**: The consensus state engine has no socket handles, file descriptors, or timer threads. It is stepped purely via in-memory events and manual ticks.
- **Zero-Allocation Execution**: Constant-memory execution along hot paths using static circular arrays and borrowing slices.
- **Strictly Modular (Type-per-File)**: Every struct, enum, and trait is decomposed into its own dedicated file without hidden private helper routines.
- **Pluggable Archival**: Seamlessly stream committed entries to disk WALs or databases asynchronously without blocking consensus replication.
- **Safe Zero-Copy Wire Protocol**: Binary datagram framing built on `zerocopy 0.8` without `unsafe`.

## Codebase Organization (Type-per-File)

All domain primitives, message models, protocol frames, and consensus logic are separated into dedicated, single-type files within submodules, with top-level re-exports for backwards compatibility:

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
  - [`RaftMessage`](file:///home/chad/source/rust/flotilla/src/message/raft_message.rs): High-level strongly-typed Raft message enum.
- **`codec/`**:
  - [`PacketHeader`](file:///home/chad/source/rust/flotilla/src/codec/packet_header.rs): 40-byte fixed zero-copy packet header.
  - [`CodecError`](file:///home/chad/source/rust/flotilla/src/codec/codec_error.rs): Encoding/decoding error types.
  - [`mod.rs`](file:///home/chad/source/rust/flotilla/src/codec/mod.rs): Wire framing, CRC32 checksums, and packet parsers.
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
- **`archive/`**:
  - [`ArchivedEntry`](file:///home/chad/source/rust/flotilla/src/archive/archived_entry.rs): Committed entry payload for offloading.
  - [`AsyncArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/async_archive_sink.rs): Storage target trait (`write_entries`, `flush`).
  - [`PipelineError`](file:///home/chad/source/rust/flotilla/src/archive/pipeline_error.rs): Pipeline queue error types.
  - [`ArchivePipeline`](file:///home/chad/source/rust/flotilla/src/archive/archive_pipeline.rs): Buffered asynchronous archival pipeline.
  - [`NullArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/null_archive_sink.rs): In-memory discarding sink.
  - [`FileArchiveSink`](file:///home/chad/source/rust/flotilla/src/archive/file_archive_sink.rs): Append-only WAL file sink with CRC verification.
- **`engine/`**:
  - [`EngineError`](file:///home/chad/source/rust/flotilla/src/engine/engine_error.rs): State machine error definitions.
  - [`OutboundMessage`](file:///home/chad/source/rust/flotilla/src/engine/outbound_message.rs): Actions emitted by the engine (`SendPacket`, `ApplyEntries`).
  - [`RaftConfig`](file:///home/chad/source/rust/flotilla/src/engine/raft_config.rs): Consensus node configuration options.
  - [`RaftNode`](file:///home/chad/source/rust/flotilla/src/engine/raft_node.rs): Top-level sans-I/O Raft consensus engine.
  - [`packets.rs`](file:///home/chad/source/rust/flotilla/src/engine/packets.rs): Outbound packet construction functions.
- **`udp/`**:
  - [`UdpDriver`](file:///home/chad/source/rust/flotilla/src/udp/udp_driver.rs): Lightweight non-blocking socket driver wrapper.
  - [`UdpClusterRouter`](file:///home/chad/source/rust/flotilla/src/udp/udp_cluster_router.rs): Bidirectional `NodeId` to `SocketAddr` address mapping.
  - [`framing.rs`](file:///home/chad/source/rust/flotilla/src/udp/framing.rs): MTU bounds checks.

## Quickstart

Add `flotilla` to your `Cargo.toml`:
```toml
[dependencies]
flotilla = "0.1"
```

### Basic Engine Instantiation

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

let mut node = RaftNode::<1024>::new(config);

// Trigger a tick
node.tick();
```

## Running Tests & Quality Gates

```bash
# Run all unit and integration tests
cargo test --all-targets

# Run zero-allocation hot path verification test suite
cargo test --test zero_alloc_tests

# Enforce coding standards (no-private-helpers, type-per-file)
python3 .github/scripts/check_coding_standards.py
cargo test --test coding_standards_tests

# Run performance benchmarks
cargo bench

# Check for clippy warnings
cargo clippy --all-targets -- -D warnings
```
