use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe, lock-free atomic monotonic 64-bit telemetry counter.
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    /// Create a new counter with an initial value.
    pub const fn new(init: u64) -> Self {
        Self {
            value: AtomicU64::new(init),
        }
    }

    /// Monotonically increment the counter by 1, returning the previous value.
    #[inline(always)]
    pub fn inc(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed)
    }

    /// Monotonically increment the counter by a specific delta, returning previous value.
    #[inline(always)]
    pub fn inc_by(&self, delta: u64) -> u64 {
        self.value.fetch_add(delta, Ordering::Relaxed)
    }

    /// Read the current counter value.
    #[inline(always)]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset counter value back to zero.
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new(0)
    }
}

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counter({})", self.get())
    }
}
