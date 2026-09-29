//! Storage subsystem for Flotilla Raft consensus.

pub mod ring_buffer;

pub use ring_buffer::{LogSlot, RingBufferLogStorage, StorageError};
