# Zero-Allocation Storage Specification (L2)

## Memory Layout
Each log entry in the ring buffer occupies a fixed slot:
- `term`: `Term` (u64)
- `index`: `LogIndex` (u64)
- `payload_len`: `u32`
- `payload`: `[u8; MAX_ENTRY_PAYLOAD]` (e.g. 1024 bytes)

```rust
#[repr(align(64))]
pub struct LogSlot<const MAX_PAYLOAD: usize> {
    pub term: u64,
    pub index: u64,
    pub payload_len: u32,
    pub payload: [u8; MAX_PAYLOAD],
}
```

## Circular Index Invariants
1. `first_index`: The lowest active log index present in the buffer.
2. `last_index`: The highest log index appended to the buffer.
3. `capacity`: Fixed compile-time capacity $N = 2^k$.
4. Invariant: `last_index - first_index < capacity`. If this invariant is breached due to slow archiving, backpressure is applied to proposals.
