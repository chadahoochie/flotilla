use super::metrics_snapshot::MetricsSnapshot;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Meter;
use parking_lot::Mutex;

/// OpenTelemetry metrics publisher that maps periodic snapshots to OTel instruments.
pub struct OtelMetricsPublisher {
    /// Interval in seconds for publishing snapshots.
    pub interval_secs: u64,
    prev_snapshot: Mutex<Option<MetricsSnapshot>>,
}

impl OtelMetricsPublisher {
    /// Create a new publisher with specified tick interval.
    pub const fn new(interval_secs: u64) -> Self {
        Self {
            interval_secs,
            prev_snapshot: parking_lot::const_mutex(None),
        }
    }

    /// Record snapshot metrics computing deltas against the previously recorded snapshot.
    pub fn publish(&self, snapshot: &MetricsSnapshot, meter: &Meter) {
        let mut prev_guard = self.prev_snapshot.lock();
        let prev = prev_guard.clone();
        *prev_guard = Some(snapshot.clone());
        drop(prev_guard);

        Self::record_snapshot_diff(snapshot, prev.as_ref(), meter);
    }

    /// Record metric deltas between current snapshot and optional previous snapshot.
    pub fn record_snapshot_diff(
        snapshot: &MetricsSnapshot,
        prev: Option<&MetricsSnapshot>,
        meter: &Meter,
    ) {
        let empty_attrs: &[KeyValue] = &[];

        let prev_val = |f: fn(&MetricsSnapshot) -> u64| -> u64 {
            prev.map(f).unwrap_or(0)
        };

        let add_delta = |name: &'static str, curr: u64, prev: u64| {
            let diff = curr.saturating_sub(prev);
            if diff > 0 {
                meter.u64_counter(name).build().add(diff, empty_attrs);
            }
        };

        // 1. Proposal counters
        add_delta(
            "flotilla.proposals.total",
            snapshot.proposals_total,
            prev_val(|s| s.proposals_total),
        );
        add_delta(
            "flotilla.proposals.committed",
            snapshot.proposals_committed,
            prev_val(|s| s.proposals_committed),
        );
        add_delta(
            "flotilla.proposals.failed",
            snapshot.proposals_failed,
            prev_val(|s| s.proposals_failed),
        );

        // 2. Consensus message counters
        add_delta(
            "flotilla.messages.received",
            snapshot.messages_received_total,
            prev_val(|s| s.messages_received_total),
        );
        add_delta(
            "flotilla.messages.sent",
            snapshot.messages_sent_total,
            prev_val(|s| s.messages_sent_total),
        );
        add_delta(
            "flotilla.request_votes.sent",
            snapshot.request_votes_sent,
            prev_val(|s| s.request_votes_sent),
        );
        add_delta(
            "flotilla.request_votes.received",
            snapshot.request_votes_received,
            prev_val(|s| s.request_votes_received),
        );
        add_delta(
            "flotilla.append_entries.sent",
            snapshot.append_entries_sent,
            prev_val(|s| s.append_entries_sent),
        );
        add_delta(
            "flotilla.append_entries.received",
            snapshot.append_entries_received,
            prev_val(|s| s.append_entries_received),
        );
        add_delta(
            "flotilla.heartbeats.sent",
            snapshot.heartbeats_sent,
            prev_val(|s| s.heartbeats_sent),
        );
        add_delta(
            "flotilla.heartbeats.received",
            snapshot.heartbeats_received,
            prev_val(|s| s.heartbeats_received),
        );

        // 3. Election & state transition counters
        add_delta(
            "flotilla.elections.started",
            snapshot.elections_started,
            prev_val(|s| s.elections_started),
        );
        add_delta(
            "flotilla.elections.won",
            snapshot.elections_won,
            prev_val(|s| s.elections_won),
        );
        add_delta(
            "flotilla.terms.advanced",
            snapshot.terms_advanced,
            prev_val(|s| s.terms_advanced),
        );
        add_delta(
            "flotilla.step_downs",
            snapshot.step_downs,
            prev_val(|s| s.step_downs),
        );

        // 4. Execution counters
        add_delta(
            "flotilla.ticks.total",
            snapshot.ticks_total,
            prev_val(|s| s.ticks_total),
        );
        add_delta(
            "flotilla.entries.applied",
            snapshot.entries_applied_total,
            prev_val(|s| s.entries_applied_total),
        );
        add_delta(
            "flotilla.entries.appended",
            snapshot.entries_appended_total,
            prev_val(|s| s.entries_appended_total),
        );

        // 5. Transport counters
        add_delta(
            "flotilla.udp.received",
            snapshot.udp_received,
            prev_val(|s| s.udp_received),
        );
        add_delta(
            "flotilla.udp.sent",
            snapshot.udp_sent,
            prev_val(|s| s.udp_sent),
        );
        add_delta(
            "flotilla.tcp.received",
            snapshot.tcp_received,
            prev_val(|s| s.tcp_received),
        );
        add_delta(
            "flotilla.tcp.sent",
            snapshot.tcp_sent,
            prev_val(|s| s.tcp_sent),
        );
        add_delta(
            "flotilla.grpc.proposals",
            snapshot.grpc_proposals,
            prev_val(|s| s.grpc_proposals),
        );

        // 6. Duration histograms (recorded in seconds)
        let proposal_prev_count = prev.map(|s| s.proposal_duration.count).unwrap_or(0);
        if snapshot.proposal_duration.count > proposal_prev_count {
            meter
                .f64_histogram("flotilla.duration.proposal")
                .build()
                .record(
                    snapshot.proposal_duration.mean_micros / 1_000_000.0,
                    empty_attrs,
                );
        }

        let step_prev_count = prev.map(|s| s.step_duration.count).unwrap_or(0);
        if snapshot.step_duration.count > step_prev_count {
            meter
                .f64_histogram("flotilla.duration.step")
                .build()
                .record(
                    snapshot.step_duration.mean_micros / 1_000_000.0,
                    empty_attrs,
                );
        }

        let tick_prev_count = prev.map(|s| s.tick_duration.count).unwrap_or(0);
        if snapshot.tick_duration.count > tick_prev_count {
            meter
                .f64_histogram("flotilla.duration.tick")
                .build()
                .record(
                    snapshot.tick_duration.mean_micros / 1_000_000.0,
                    empty_attrs,
                );
        }
    }
}

static GLOBAL_PUBLISHER: OtelMetricsPublisher = OtelMetricsPublisher::new(10);

/// Standalone pure function to map and record an immutable `MetricsSnapshot` to OpenTelemetry instruments.
pub fn publish_snapshot_to_otel(snapshot: &MetricsSnapshot, meter: &Meter) {
    GLOBAL_PUBLISHER.publish(snapshot, meter);
}
