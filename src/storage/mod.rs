//! Storage subsystem for Flotilla Raft consensus.

pub mod log_slot;
pub mod ring_buffer;
pub mod ring_buffer_log_storage;
pub mod storage_error;

pub use log_slot::LogSlot;
pub use ring_buffer_log_storage::{
    RingBufferLogStorage, compute_slot_index, is_buffer_full, is_index_in_retained_range,
};
pub use storage_error::StorageError;
