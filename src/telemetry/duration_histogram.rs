use super::histogram_snapshot::HistogramSnapshot;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Fixed duration bucket upper bounds in microseconds.
pub const DURATION_BUCKETS: [u64; 16] = [
    10,        // <= 10 µs
    50,        // <= 50 µs
    100,       // <= 100 µs
    250,       // <= 250 µs
    500,       // <= 500 µs
    1_000,     // <= 1 ms
    2_500,     // <= 2.5 ms
    5_000,     // <= 5 ms
    10_000,    // <= 10 ms
    25_000,    // <= 25 ms
    50_000,    // <= 50 ms
    100_000,   // <= 100 ms
    250_000,   // <= 250 ms
    500_000,   // <= 500 ms
    1_000_000, // <= 1 s
    u64::MAX,  // +Inf
];

/// Lock-free, zero-allocation duration histogram with cumulative logarithmic buckets.
pub struct DurationHistogram {
    count: AtomicU64,
    sum_micros: AtomicU64,
    min_micros: AtomicU64,
    max_micros: AtomicU64,
    buckets: [AtomicU64; 16],
}

impl DurationHistogram {
    /// Initialize a new duration histogram.
    pub const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
            min_micros: AtomicU64::new(u64::MAX),
            max_micros: AtomicU64::new(0),
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    /// Record a duration measurement in microsecond precision without allocations or locks.
    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        self.record_micros(micros);
    }

    /// Record a duration measurement in microseconds.
    pub fn record_micros(&self, micros: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);

        // Atomic min update
        let mut current_min = self.min_micros.load(Ordering::Relaxed);
        while micros < current_min {
            match self.min_micros.compare_exchange_weak(
                current_min,
                micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }

        // Atomic max update
        let mut current_max = self.max_micros.load(Ordering::Relaxed);
        while micros > current_max {
            match self.max_micros.compare_exchange_weak(
                current_max,
                micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }

        // Increment appropriate bucket
        for (i, &upper) in DURATION_BUCKETS.iter().enumerate() {
            if micros <= upper {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    /// Return total observation count.
    #[inline(always)]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Return cumulative sum in microseconds.
    #[inline(always)]
    pub fn sum_micros(&self) -> u64 {
        self.sum_micros.load(Ordering::Relaxed)
    }

    /// Return minimum duration recorded in microseconds (or 0 if empty).
    pub fn min_micros(&self) -> u64 {
        let val = self.min_micros.load(Ordering::Relaxed);
        if val == u64::MAX { 0 } else { val }
    }

    /// Return maximum duration recorded in microseconds.
    pub fn max_micros(&self) -> u64 {
        self.max_micros.load(Ordering::Relaxed)
    }

    /// Return arithmetic mean duration in microseconds.
    pub fn mean_micros(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            self.sum_micros() as f64 / count as f64
        }
    }

    /// Estimate duration at the given percentile (0.0 to 1.0).
    pub fn estimate_percentile(&self, percentile: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }

        let target_rank = (total as f64 * percentile.clamp(0.0, 1.0)).ceil() as u64;
        let mut accumulated = 0u64;

        for (i, &upper) in DURATION_BUCKETS.iter().enumerate() {
            accumulated += self.buckets[i].load(Ordering::Relaxed);
            if accumulated >= target_rank {
                return upper;
            }
        }

        self.max_micros()
    }

    /// Return estimated 50th percentile (median) duration in microseconds.
    pub fn p50_micros(&self) -> u64 {
        self.estimate_percentile(0.50)
    }

    /// Return estimated 90th percentile duration in microseconds.
    pub fn p90_micros(&self) -> u64 {
        self.estimate_percentile(0.90)
    }

    /// Return estimated 99th percentile duration in microseconds.
    pub fn p99_micros(&self) -> u64 {
        self.estimate_percentile(0.99)
    }

    /// Return cumulative bucket counts as pairs of `(upper_bound_micros, cumulative_count)`.
    pub fn bucket_counts(&self) -> Vec<(u64, u64)> {
        let mut result = Vec::with_capacity(DURATION_BUCKETS.len());
        let mut cumulative = 0u64;
        for (i, &upper) in DURATION_BUCKETS.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            result.push((upper, cumulative));
        }
        result
    }

    /// Take a point-in-time statistical snapshot.
    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            count: self.count(),
            sum_micros: self.sum_micros(),
            min_micros: self.min_micros(),
            max_micros: self.max_micros(),
            mean_micros: self.mean_micros(),
            p50_micros: self.p50_micros(),
            p90_micros: self.p90_micros(),
            p99_micros: self.p99_micros(),
            buckets: self.bucket_counts(),
        }
    }

    /// Reset all counters and statistics back to initial state.
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.sum_micros.store(0, Ordering::Relaxed);
        self.min_micros.store(u64::MAX, Ordering::Relaxed);
        self.max_micros.store(0, Ordering::Relaxed);
        for b in &self.buckets {
            b.store(0, Ordering::Relaxed);
        }
    }
}

impl Default for DurationHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for DurationHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DurationHistogram(count={}, mean={:.2}µs, p50={}µs, p99={}µs)",
            self.count(),
            self.mean_micros(),
            self.p50_micros(),
            self.p99_micros()
        )
    }
}
