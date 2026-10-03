/// Point-in-time snapshot of duration histogram measurements and statistical percentiles.
#[derive(Clone, Debug, PartialEq)]
pub struct HistogramSnapshot {
    /// Total count of duration observations recorded.
    pub count: u64,
    /// Cumulative sum of observed durations in microseconds.
    pub sum_micros: u64,
    /// Minimum duration observed in microseconds (0 if count == 0).
    pub min_micros: u64,
    /// Maximum duration observed in microseconds (0 if count == 0).
    pub max_micros: u64,
    /// Arithmetic mean of observed durations in microseconds.
    pub mean_micros: f64,
    /// Estimated 50th percentile (median) duration in microseconds.
    pub p50_micros: u64,
    /// Estimated 90th percentile duration in microseconds.
    pub p90_micros: u64,
    /// Estimated 99th percentile duration in microseconds.
    pub p99_micros: u64,
    /// Histogram bucket upper bounds and cumulative counts: (upper_bound_micros, count).
    pub buckets: Vec<(u64, u64)>,
}

impl HistogramSnapshot {
    /// Create an empty snapshot.
    pub fn empty() -> Self {
        Self {
            count: 0,
            sum_micros: 0,
            min_micros: 0,
            max_micros: 0,
            mean_micros: 0.0,
            p50_micros: 0,
            p90_micros: 0,
            p99_micros: 0,
            buckets: Vec::new(),
        }
    }
}

impl Default for HistogramSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}
