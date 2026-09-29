---
name: rust-sans-io-consensus
description: Architectural standards for Sans-I/O deterministic state machines in consensus systems, completely decoupled from sockets, system clocks, and OS threads.
---

# Rust Sans-I/O Consensus Standards

## Invariants
1. **Decoupled I/O**: The consensus engine never imports `std::net`, `std::time::Instant::now()`, or spawning primitives (`std::thread`, `tokio`).
2. **Deterministic Inputs**: All inputs pass through:
   - `step(event: InboundMessage) -> Actions`
   - `tick() -> Actions`
3. **Reproducible Simulation**: Tests run synchronously in pure memory, enabling deterministic step-through debugging, permutation fuzzing, and instantaneous discrete-event simulation.
