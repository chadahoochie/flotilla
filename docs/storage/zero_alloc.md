# Zero-Allocation Storage Specification (L2)

## Memory Layout
Each log entry in the ring buffer occupies a fixed slot defined in [`src/storage/log_slot.rs`](file:///home/chad/source/rust/flotilla/src/storage/log_slot.rs):
- `term`: [`Term`](file:///home/chad/source/rust/flotilla/src/types/term.rs)
- `index`: [`LogIndex`](file:///home/chad/source/rust/flotilla/src/types/log_index.rs)
- `payload_len`: `u32`
- `payload`: `[u8; MAX_PAYLOAD]` (e.g. 1024 bytes)

```rust
#[repr(align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogSlot<const MAX_PAYLOAD: usize> {
    pub term: Term,
    pub index: LogIndex,
    pub payload_len: u32,
    pub payload: [u8; MAX_PAYLOAD],
}
```

## Circular Index Invariants
Defined in [`src/storage/ring_buffer_log_storage.rs`](file:///home/chad/source/rust/flotilla/src/storage/ring_buffer_log_storage.rs):
1. `first_index`: The lowest active log index present in the buffer.
2. `last_index`: The highest log index appended to the buffer.
3. `capacity`: Fixed compile-time capacity $N = 2^k$.
4. Invariant: `last_index - first_index < capacity`. If this invariant is breached due to slow archiving, [`StorageError::BufferFull`](file:///home/chad/source/rust/flotilla/src/storage/storage_error.rs) is returned to apply backpressure to proposals.
