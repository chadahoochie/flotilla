//! Ring buffer storage module re-exports.

pub use super::log_slot::LogSlot;
pub use super::ring_buffer_log_storage::{
    RingBufferLogStorage, compute_slot_index, is_buffer_full, is_index_in_retained_range,
};
pub use super::storage_error::StorageError;
