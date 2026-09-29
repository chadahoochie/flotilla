# Flotilla Developer and User Guide

Flotilla is a zero-allocation, sans-I/O Raft consensus library implemented in Safe Rust (Rust 1.98+ / Edition 2024).

## Key Features
- **Sans-I/O Architecture**: The consensus state engine has no socket handles, file descriptors, or timer threads. It is stepped purely via in-memory events and manual ticks.
- **Zero-Allocation Execution**: Constant-memory execution along hot paths using static circular arrays and borrowing slices.
- **Strictly Modular**: All algorithms, election checks, and commit rules are exposed as crate-public modular types without hidden private helper routines.
- **Pluggable Archival**: Seamlessly stream committed entries to disk WALs or databases asynchronously without blocking consensus replication.
- **Safe Zero-Copy Wire Protocol**: Binary datagram framing built on `zerocopy 0.8` without `unsafe`.

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

## Running Tests & Benchmarks

```bash
cargo test --all-targets
cargo bench
cargo clippy --all-targets -- -D warnings
```
