use super::metrics_snapshot::MetricsSnapshot;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Meter;

/// OpenTelemetry metrics publisher that maps periodic snapshots to OTel instruments.
pub struct OtelMetricsPublisher {
    /// Interval in seconds for publishing snapshots.
    pub interval_secs: u64,
}

impl OtelMetricsPublisher {
    /// Create a new publisher with specified tick interval.
    pub const fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

/// Standalone pure function to map and record an immutable `MetricsSnapshot` to OpenTelemetry instruments.
pub fn publish_snapshot_to_otel(snapshot: &MetricsSnapshot, meter: &Meter) {
    let empty_attrs: &[KeyValue] = &[];

    // 1. Proposal counters
    let proposals_total = meter.u64_counter("flotilla.proposals.total").build();
    proposals_total.add(snapshot.proposals_total, empty_attrs);

    let proposals_committed = meter.u64_counter("flotilla.proposals.committed").build();
    proposals_committed.add(snapshot.proposals_committed, empty_attrs);

    let proposals_failed = meter.u64_counter("flotilla.proposals.failed").build();
    proposals_failed.add(snapshot.proposals_failed, empty_attrs);

    // 2. Consensus message counters
    let msgs_rcv = meter.u64_counter("flotilla.messages.received").build();
    msgs_rcv.add(snapshot.messages_received_total, empty_attrs);

    let msgs_sent = meter.u64_counter("flotilla.messages.sent").build();
    msgs_sent.add(snapshot.messages_sent_total, empty_attrs);

    let votes_sent = meter.u64_counter("flotilla.request_votes.sent").build();
    votes_sent.add(snapshot.request_votes_sent, empty_attrs);

    let votes_rcv = meter.u64_counter("flotilla.request_votes.received").build();
    votes_rcv.add(snapshot.request_votes_received, empty_attrs);

    let appends_sent = meter.u64_counter("flotilla.append_entries.sent").build();
    appends_sent.add(snapshot.append_entries_sent, empty_attrs);

    let appends_rcv = meter
        .u64_counter("flotilla.append_entries.received")
        .build();
    appends_rcv.add(snapshot.append_entries_received, empty_attrs);

    let heartbeats_sent = meter.u64_counter("flotilla.heartbeats.sent").build();
    heartbeats_sent.add(snapshot.heartbeats_sent, empty_attrs);

    let heartbeats_rcv = meter.u64_counter("flotilla.heartbeats.received").build();
    heartbeats_rcv.add(snapshot.heartbeats_received, empty_attrs);

    // 3. Election & state transition counters
    let elections_started = meter.u64_counter("flotilla.elections.started").build();
    elections_started.add(snapshot.elections_started, empty_attrs);

    let elections_won = meter.u64_counter("flotilla.elections.won").build();
    elections_won.add(snapshot.elections_won, empty_attrs);

    let terms_advanced = meter.u64_counter("flotilla.terms.advanced").build();
    terms_advanced.add(snapshot.terms_advanced, empty_attrs);

    let step_downs = meter.u64_counter("flotilla.step_downs").build();
    step_downs.add(snapshot.step_downs, empty_attrs);

    // 4. Execution counters
    let ticks_total = meter.u64_counter("flotilla.ticks.total").build();
    ticks_total.add(snapshot.ticks_total, empty_attrs);

    let entries_applied = meter.u64_counter("flotilla.entries.applied").build();
    entries_applied.add(snapshot.entries_applied_total, empty_attrs);

    let entries_appended = meter.u64_counter("flotilla.entries.appended").build();
    entries_appended.add(snapshot.entries_appended_total, empty_attrs);

    // 5. Transport counters
    let udp_rcv = meter.u64_counter("flotilla.udp.received").build();
    udp_rcv.add(snapshot.udp_received, empty_attrs);

    let udp_sent = meter.u64_counter("flotilla.udp.sent").build();
    udp_sent.add(snapshot.udp_sent, empty_attrs);

    let tcp_rcv = meter.u64_counter("flotilla.tcp.received").build();
    tcp_rcv.add(snapshot.tcp_received, empty_attrs);

    let tcp_sent = meter.u64_counter("flotilla.tcp.sent").build();
    tcp_sent.add(snapshot.tcp_sent, empty_attrs);

    let grpc_proposals = meter.u64_counter("flotilla.grpc.proposals").build();
    grpc_proposals.add(snapshot.grpc_proposals, empty_attrs);

    // 6. Duration histograms (recorded in seconds)
    let proposal_hist = meter.f64_histogram("flotilla.duration.proposal").build();
    if snapshot.proposal_duration.count > 0 {
        proposal_hist.record(
            snapshot.proposal_duration.mean_micros / 1_000_000.0,
            empty_attrs,
        );
    }

    let step_hist = meter.f64_histogram("flotilla.duration.step").build();
    if snapshot.step_duration.count > 0 {
        step_hist.record(
            snapshot.step_duration.mean_micros / 1_000_000.0,
            empty_attrs,
        );
    }

    let tick_hist = meter.f64_histogram("flotilla.duration.tick").build();
    if snapshot.tick_duration.count > 0 {
        tick_hist.record(
            snapshot.tick_duration.mean_micros / 1_000_000.0,
            empty_attrs,
        );
    }
}
