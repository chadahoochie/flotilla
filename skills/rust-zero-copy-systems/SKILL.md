---
name: rust-zero-copy-systems
description: Enforces safe zero-copy systems programming standards in Rust 2024, including zerocopy 0.8, const assertions, cacheline alignment, and zero runtime heap allocations along steady-state paths.
---

# Rust Zero-Copy Systems Standards

## Invariants
1. **Zero Steady-State Allocations**: No calls to `Vec::push`, `Box::new`, `format!`, or `String::from` on consensus hot paths.
2. **Safe Transmutation**: Exclusively use `zerocopy 0.8` with `FromBytes`, `IntoBytes`, and `Immutable` for packet wire envelopes. Do not use `unsafe std::mem::transmute`.
3. **Power-of-Two Ring Buffers**: Enforce power-of-two capacity at compile time via:
   ```rust
   const { assert!(N > 0 && (N & (N - 1)) == 0, "Capacity must be a power of two") }
   ```
4. **Cacheline False Sharing Mitigation**: Pad and align high-contention structures with `#[repr(align(64))]`.
5. **Safe Chunking**: Use safe slice methods (`split_first_chunk::<N>()`, `as_chunks::<N>()`) instead of manual index offsets.
