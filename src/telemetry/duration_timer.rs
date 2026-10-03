use super::duration_histogram::DurationHistogram;
use std::time::{Duration, Instant};

/// RAII execution timer measuring wall-clock duration and recording to a `DurationHistogram` upon drop.
pub struct DurationTimer<'a> {
    histogram: &'a DurationHistogram,
    start_instant: Instant,
    stopped: bool,
}

impl<'a> DurationTimer<'a> {
    /// Start measuring execution duration for the designated histogram.
    #[inline(always)]
    pub fn start(histogram: &'a DurationHistogram) -> Self {
        Self {
            histogram,
            start_instant: Instant::now(),
            stopped: false,
        }
    }

    /// Stop the timer early and record the elapsed duration, returning the recorded duration.
    pub fn stop(&mut self) -> Duration {
        if !self.stopped {
            self.stopped = true;
            let elapsed = self.start_instant.elapsed();
            self.histogram.record(elapsed);
            elapsed
        } else {
            Duration::ZERO
        }
    }

    /// Read the currently elapsed duration without stopping the timer.
    #[inline(always)]
    pub fn elapsed(&self) -> Duration {
        self.start_instant.elapsed()
    }
}

impl Drop for DurationTimer<'_> {
    fn drop(&mut self) {
        if !self.stopped {
            let elapsed = self.start_instant.elapsed();
            self.histogram.record(elapsed);
        }
    }
}
