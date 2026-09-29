use super::log_slot::LogSlot;
use super::storage_error::StorageError;
use crate::types::{LogIndex, Term};

/// Compute the slot index in the circular buffer using bitwise AND.
#[inline(always)]
pub fn compute_slot_index(index: LogIndex, capacity_mask: usize) -> usize {
    (index.0.wrapping_sub(1) as usize) & capacity_mask
}

/// Check if a log index falls within the active retained range [first, last].
#[inline(always)]
pub fn is_index_in_retained_range(index: LogIndex, first: LogIndex, last: LogIndex) -> bool {
    index.0 >= first.0 && index.0 <= last.0
}

/// Check whether the circular buffer is full.
#[inline(always)]
pub fn is_buffer_full(first: LogIndex, last: LogIndex, capacity: usize) -> bool {
    if last.0 < first.0 {
        false
    } else {
        (last.0 - first.0 + 1) as usize >= capacity
    }
}

/// Circular ring buffer log storage with power-of-two capacity and constant-memory hot path.
pub struct RingBufferLogStorage<const CAPACITY: usize, const MAX_PAYLOAD: usize> {
    pub first: LogIndex,
    pub last: LogIndex,
    pub slots: Box<[LogSlot<MAX_PAYLOAD>]>,
}

impl<const CAPACITY: usize, const MAX_PAYLOAD: usize> RingBufferLogStorage<CAPACITY, MAX_PAYLOAD> {
    const CAPACITY_MASK: usize = CAPACITY - 1;

    /// Initialize a new pre-allocated ring buffer storage.
    pub fn new() -> Self {
        // Compile-time assertions for capacity power of two and valid payload size
        const {
            assert!(
                CAPACITY > 0 && (CAPACITY & (CAPACITY - 1)) == 0,
                "Ring buffer capacity must be a power of two"
            );
            assert!(
                MAX_PAYLOAD > 0,
                "Maximum entry payload size must be greater than zero"
            );
        }

        let slots = vec![LogSlot::default(); CAPACITY].into_boxed_slice();
        Self {
            first: LogIndex(1),
            last: LogIndex(0),
            slots,
        }
    }

    /// Return the lowest retained log index.
    #[inline(always)]
    pub fn first_index(&self) -> LogIndex {
        self.first
    }

    /// Return the highest appended log index.
    #[inline(always)]
    pub fn last_index(&self) -> LogIndex {
        self.last
    }

    /// Number of active retained entries.
    #[inline(always)]
    pub fn len(&self) -> usize {
        if self.last.0 < self.first.0 {
            0
        } else {
            (self.last.0 - self.first.0 + 1) as usize
        }
    }

    /// Check if the storage contains any active entries.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the term of the entry at `index`, if retained.
    #[inline]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if is_index_in_retained_range(index, self.first, self.last) {
            let slot = compute_slot_index(index, Self::CAPACITY_MASK);
            Some(self.slots[slot].term)
        } else {
            None
        }
    }

    /// Retrieve a reference to the entry slot at `index`, if retained.
    #[inline]
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogSlot<MAX_PAYLOAD>> {
        if is_index_in_retained_range(index, self.first, self.last) {
            let slot = compute_slot_index(index, Self::CAPACITY_MASK);
            Some(&self.slots[slot])
        } else {
            None
        }
    }

    /// Append a new log entry to the buffer.
    pub fn append_entry(
        &mut self,
        term: Term,
        payload: &[u8],
    ) -> Result<LogIndex, StorageError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(StorageError::PayloadTooLarge {
                max: MAX_PAYLOAD,
                actual: payload.len(),
            });
        }

        if is_buffer_full(self.first, self.last, CAPACITY) {
            return Err(StorageError::BufferFull);
        }

        let new_index = self.last.next();
        let slot_idx = compute_slot_index(new_index, Self::CAPACITY_MASK);
        let slot = &mut self.slots[slot_idx];

        slot.term = term;
        slot.index = new_index;
        slot.payload_len = payload.len() as u32;
        slot.payload[..payload.len()].copy_from_slice(payload);

        self.last = new_index;
        Ok(new_index)
    }

    /// Truncate any conflicting entries from `from_index` through `last_index`.
    pub fn truncate_suffix(&mut self, from_index: LogIndex) {
        if from_index.0 <= self.first.0 {
            self.last = LogIndex(self.first.0.saturating_sub(1));
        } else if from_index.0 <= self.last.0 {
            self.last = LogIndex(from_index.0 - 1);
        }
    }

    /// Compact entries up to `watermark`, advancing `first_index`.
    pub fn compact_prefix(&mut self, watermark: LogIndex) -> Result<(), StorageError> {
        if watermark.0 > self.last.0 {
            return Err(StorageError::CompactionIndexTooHigh {
                watermark,
                last: self.last,
            });
        }
        if watermark.0 >= self.first.0 {
            self.first = watermark.next();
        }
        Ok(())
    }
}

impl<const CAPACITY: usize, const MAX_PAYLOAD: usize> Default
    for RingBufferLogStorage<CAPACITY, MAX_PAYLOAD>
{
    fn default() -> Self {
        Self::new()
    }
}
