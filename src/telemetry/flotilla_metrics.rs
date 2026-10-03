use super::counter::Counter;
use super::duration_histogram::DurationHistogram;
use super::metrics_snapshot::MetricsSnapshot;
use std::sync::OnceLock;

static GLOBAL_METRICS: OnceLock<FlotillaMetrics> = OnceLock::new();

/// Global telemetry registry holding high-performance lock-free counters and duration histograms.
pub struct FlotillaMetrics {
    // Proposal counts
    pub proposals_total: Counter,
    pub proposals_committed: Counter,
    pub proposals_failed: Counter,

    // Consensus message counts
    pub messages_received_total: Counter,
    pub messages_sent_total: Counter,
    pub request_votes_sent: Counter,
    pub request_votes_received: Counter,
    pub votes_granted: Counter,
    pub votes_denied: Counter,
    pub append_entries_sent: Counter,
    pub append_entries_received: Counter,
    pub heartbeats_sent: Counter,
    pub heartbeats_received: Counter,

    // State transition counts
    pub elections_started: Counter,
    pub elections_won: Counter,
    pub terms_advanced: Counter,
    pub step_downs: Counter,

    // Engine execution counts
    pub ticks_total: Counter,
    pub entries_applied_total: Counter,
    pub entries_appended_total: Counter,

    // Transport counts
    pub udp_received: Counter,
    pub udp_sent: Counter,
    pub tcp_accepted: Counter,
    pub tcp_received: Counter,
    pub tcp_sent: Counter,
    pub grpc_proposals: Counter,
    pub grpc_step_calls: Counter,
    pub grpc_status_calls: Counter,
    pub transport_errors: Counter,

    // Client counts
    pub client_proposals_total: Counter,
    pub client_proposals_succeeded: Counter,
    pub client_proposals_failed: Counter,
    pub client_pings_total: Counter,
    pub client_timeouts_total: Counter,

    // Archival counts
    pub archived_entries_total: Counter,
    pub archival_batches_total: Counter,
    pub archival_errors_total: Counter,

    // Duration histograms
    pub proposal_duration: DurationHistogram,
    pub step_duration: DurationHistogram,
    pub tick_duration: DurationHistogram,
    pub commit_evaluation_duration: DurationHistogram,
    pub storage_append_duration: DurationHistogram,
    pub tcp_request_duration: DurationHistogram,
    pub grpc_proposal_duration: DurationHistogram,
    pub grpc_step_duration: DurationHistogram,
    pub udp_step_duration: DurationHistogram,
    pub archive_flush_duration: DurationHistogram,
}

impl FlotillaMetrics {
    /// Create a new, isolated metrics registry.
    pub const fn new() -> Self {
        Self {
            proposals_total: Counter::new(0),
            proposals_committed: Counter::new(0),
            proposals_failed: Counter::new(0),

            messages_received_total: Counter::new(0),
            messages_sent_total: Counter::new(0),
            request_votes_sent: Counter::new(0),
            request_votes_received: Counter::new(0),
            votes_granted: Counter::new(0),
            votes_denied: Counter::new(0),
            append_entries_sent: Counter::new(0),
            append_entries_received: Counter::new(0),
            heartbeats_sent: Counter::new(0),
            heartbeats_received: Counter::new(0),

            elections_started: Counter::new(0),
            elections_won: Counter::new(0),
            terms_advanced: Counter::new(0),
            step_downs: Counter::new(0),

            ticks_total: Counter::new(0),
            entries_applied_total: Counter::new(0),
            entries_appended_total: Counter::new(0),

            udp_received: Counter::new(0),
            udp_sent: Counter::new(0),
            tcp_accepted: Counter::new(0),
            tcp_received: Counter::new(0),
            tcp_sent: Counter::new(0),
            grpc_proposals: Counter::new(0),
            grpc_step_calls: Counter::new(0),
            grpc_status_calls: Counter::new(0),
            transport_errors: Counter::new(0),

            client_proposals_total: Counter::new(0),
            client_proposals_succeeded: Counter::new(0),
            client_proposals_failed: Counter::new(0),
            client_pings_total: Counter::new(0),
            client_timeouts_total: Counter::new(0),

            archived_entries_total: Counter::new(0),
            archival_batches_total: Counter::new(0),
            archival_errors_total: Counter::new(0),

            proposal_duration: DurationHistogram::new(),
            step_duration: DurationHistogram::new(),
            tick_duration: DurationHistogram::new(),
            commit_evaluation_duration: DurationHistogram::new(),
            storage_append_duration: DurationHistogram::new(),
            tcp_request_duration: DurationHistogram::new(),
            grpc_proposal_duration: DurationHistogram::new(),
            grpc_step_duration: DurationHistogram::new(),
            udp_step_duration: DurationHistogram::new(),
            archive_flush_duration: DurationHistogram::new(),
        }
    }

    /// Access the global singleton metrics instance.
    pub fn global() -> &'static Self {
        GLOBAL_METRICS.get_or_init(Self::new)
    }

    /// Capture an immutable point-in-time snapshot of all metric values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            proposals_total: self.proposals_total.get(),
            proposals_committed: self.proposals_committed.get(),
            proposals_failed: self.proposals_failed.get(),

            messages_received_total: self.messages_received_total.get(),
            messages_sent_total: self.messages_sent_total.get(),
            request_votes_sent: self.request_votes_sent.get(),
            request_votes_received: self.request_votes_received.get(),
            votes_granted: self.votes_granted.get(),
            votes_denied: self.votes_denied.get(),
            append_entries_sent: self.append_entries_sent.get(),
            append_entries_received: self.append_entries_received.get(),
            heartbeats_sent: self.heartbeats_sent.get(),
            heartbeats_received: self.heartbeats_received.get(),

            elections_started: self.elections_started.get(),
            elections_won: self.elections_won.get(),
            terms_advanced: self.terms_advanced.get(),
            step_downs: self.step_downs.get(),

            ticks_total: self.ticks_total.get(),
            entries_applied_total: self.entries_applied_total.get(),
            entries_appended_total: self.entries_appended_total.get(),

            udp_received: self.udp_received.get(),
            udp_sent: self.udp_sent.get(),
            tcp_accepted: self.tcp_accepted.get(),
            tcp_received: self.tcp_received.get(),
            tcp_sent: self.tcp_sent.get(),
            grpc_proposals: self.grpc_proposals.get(),
            grpc_step_calls: self.grpc_step_calls.get(),
            grpc_status_calls: self.grpc_status_calls.get(),
            transport_errors: self.transport_errors.get(),

            client_proposals_total: self.client_proposals_total.get(),
            client_proposals_succeeded: self.client_proposals_succeeded.get(),
            client_proposals_failed: self.client_proposals_failed.get(),
            client_pings_total: self.client_pings_total.get(),
            client_timeouts_total: self.client_timeouts_total.get(),

            archived_entries_total: self.archived_entries_total.get(),
            archival_batches_total: self.archival_batches_total.get(),
            archival_errors_total: self.archival_errors_total.get(),

            proposal_duration: self.proposal_duration.snapshot(),
            step_duration: self.step_duration.snapshot(),
            tick_duration: self.tick_duration.snapshot(),
            commit_evaluation_duration: self.commit_evaluation_duration.snapshot(),
            storage_append_duration: self.storage_append_duration.snapshot(),
            tcp_request_duration: self.tcp_request_duration.snapshot(),
            grpc_proposal_duration: self.grpc_proposal_duration.snapshot(),
            grpc_step_duration: self.grpc_step_duration.snapshot(),
            udp_step_duration: self.udp_step_duration.snapshot(),
            archive_flush_duration: self.archive_flush_duration.snapshot(),
        }
    }

    /// Reset all counters and duration histograms back to zero.
    pub fn reset(&self) {
        self.proposals_total.reset();
        self.proposals_committed.reset();
        self.proposals_failed.reset();

        self.messages_received_total.reset();
        self.messages_sent_total.reset();
        self.request_votes_sent.reset();
        self.request_votes_received.reset();
        self.votes_granted.reset();
        self.votes_denied.reset();
        self.append_entries_sent.reset();
        self.append_entries_received.reset();
        self.heartbeats_sent.reset();
        self.heartbeats_received.reset();

        self.elections_started.reset();
        self.elections_won.reset();
        self.terms_advanced.reset();
        self.step_downs.reset();

        self.ticks_total.reset();
        self.entries_applied_total.reset();
        self.entries_appended_total.reset();

        self.udp_received.reset();
        self.udp_sent.reset();
        self.tcp_accepted.reset();
        self.tcp_received.reset();
        self.tcp_sent.reset();
        self.grpc_proposals.reset();
        self.grpc_step_calls.reset();
        self.grpc_status_calls.reset();
        self.transport_errors.reset();

        self.client_proposals_total.reset();
        self.client_proposals_succeeded.reset();
        self.client_proposals_failed.reset();
        self.client_pings_total.reset();
        self.client_timeouts_total.reset();

        self.archived_entries_total.reset();
        self.archival_batches_total.reset();
        self.archival_errors_total.reset();

        self.proposal_duration.reset();
        self.step_duration.reset();
        self.tick_duration.reset();
        self.commit_evaluation_duration.reset();
        self.storage_append_duration.reset();
        self.tcp_request_duration.reset();
        self.grpc_proposal_duration.reset();
        self.grpc_step_duration.reset();
        self.udp_step_duration.reset();
        self.archive_flush_duration.reset();
    }
}

impl Default for FlotillaMetrics {
    fn default() -> Self {
        Self::new()
    }
}
