use super::histogram_snapshot::HistogramSnapshot;
use std::fmt::Write;

/// Immutable point-in-time snapshot of all Flotilla telemetry counters and duration histograms.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricsSnapshot {
    // Proposal counts
    pub proposals_total: u64,
    pub proposals_committed: u64,
    pub proposals_failed: u64,

    // Consensus message counts
    pub messages_received_total: u64,
    pub messages_sent_total: u64,
    pub request_votes_sent: u64,
    pub request_votes_received: u64,
    pub votes_granted: u64,
    pub votes_denied: u64,
    pub append_entries_sent: u64,
    pub append_entries_received: u64,
    pub heartbeats_sent: u64,
    pub heartbeats_received: u64,

    // State transition counts
    pub elections_started: u64,
    pub elections_won: u64,
    pub terms_advanced: u64,
    pub step_downs: u64,

    // Engine execution counts
    pub ticks_total: u64,
    pub entries_applied_total: u64,
    pub entries_appended_total: u64,

    // Transport counts
    pub udp_received: u64,
    pub udp_sent: u64,
    pub tcp_accepted: u64,
    pub tcp_received: u64,
    pub tcp_sent: u64,
    pub grpc_proposals: u64,
    pub grpc_step_calls: u64,
    pub grpc_status_calls: u64,
    pub transport_errors: u64,

    // Client counts
    pub client_proposals_total: u64,
    pub client_proposals_succeeded: u64,
    pub client_proposals_failed: u64,
    pub client_pings_total: u64,
    pub client_timeouts_total: u64,

    // Archival counts
    pub archived_entries_total: u64,
    pub archival_batches_total: u64,
    pub archival_errors_total: u64,

    // Durations
    pub proposal_duration: HistogramSnapshot,
    pub step_duration: HistogramSnapshot,
    pub tick_duration: HistogramSnapshot,
    pub commit_evaluation_duration: HistogramSnapshot,
    pub storage_append_duration: HistogramSnapshot,
    pub tcp_request_duration: HistogramSnapshot,
    pub grpc_proposal_duration: HistogramSnapshot,
    pub grpc_step_duration: HistogramSnapshot,
    pub udp_step_duration: HistogramSnapshot,
    pub archive_flush_duration: HistogramSnapshot,
}

impl MetricsSnapshot {
    /// Format all metrics into standard Prometheus text exposition format.
    pub fn to_prometheus_text(&self) -> String {
        let mut out = String::with_capacity(4096);

        // Helper macro inside method for formatting Prometheus counters
        macro_rules! emit_counter {
            ($name:expr, $help:expr, $val:expr) => {
                let _ = writeln!(out, "# HELP flotilla_{} {}", $name, $help);
                let _ = writeln!(out, "# TYPE flotilla_{} counter", $name);
                let _ = writeln!(out, "flotilla_{} {}", $name, $val);
            };
        }

        emit_counter!(
            "proposals_total",
            "Total proposals received",
            self.proposals_total
        );
        emit_counter!(
            "proposals_committed_total",
            "Total proposals committed",
            self.proposals_committed
        );
        emit_counter!(
            "proposals_failed_total",
            "Total proposals failed",
            self.proposals_failed
        );

        emit_counter!(
            "messages_received_total",
            "Total Raft protocol messages received",
            self.messages_received_total
        );
        emit_counter!(
            "messages_sent_total",
            "Total Raft protocol messages dispatched",
            self.messages_sent_total
        );
        emit_counter!(
            "request_votes_sent_total",
            "Total RequestVote requests sent",
            self.request_votes_sent
        );
        emit_counter!(
            "request_votes_received_total",
            "Total RequestVote requests received",
            self.request_votes_received
        );
        emit_counter!(
            "votes_granted_total",
            "Total votes granted",
            self.votes_granted
        );
        emit_counter!(
            "votes_denied_total",
            "Total votes denied",
            self.votes_denied
        );
        emit_counter!(
            "append_entries_sent_total",
            "Total AppendEntries sent",
            self.append_entries_sent
        );
        emit_counter!(
            "append_entries_received_total",
            "Total AppendEntries received",
            self.append_entries_received
        );
        emit_counter!(
            "heartbeats_sent_total",
            "Total heartbeat messages sent",
            self.heartbeats_sent
        );
        emit_counter!(
            "heartbeats_received_total",
            "Total heartbeat messages received",
            self.heartbeats_received
        );

        emit_counter!(
            "elections_started_total",
            "Total election campaigns initiated",
            self.elections_started
        );
        emit_counter!(
            "elections_won_total",
            "Total elections won",
            self.elections_won
        );
        emit_counter!(
            "terms_advanced_total",
            "Total term increments",
            self.terms_advanced
        );
        emit_counter!(
            "step_downs_total",
            "Total step downs to follower",
            self.step_downs
        );

        emit_counter!(
            "ticks_total",
            "Total logical ticks executed",
            self.ticks_total
        );
        emit_counter!(
            "entries_applied_total",
            "Total log entries applied",
            self.entries_applied_total
        );
        emit_counter!(
            "entries_appended_total",
            "Total log entries appended",
            self.entries_appended_total
        );

        emit_counter!(
            "udp_received_total",
            "Total UDP datagrams received",
            self.udp_received
        );
        emit_counter!("udp_sent_total", "Total UDP datagrams sent", self.udp_sent);
        emit_counter!(
            "tcp_accepted_total",
            "Total TCP connections accepted",
            self.tcp_accepted
        );
        emit_counter!(
            "tcp_received_total",
            "Total TCP frames received",
            self.tcp_received
        );
        emit_counter!("tcp_sent_total", "Total TCP frames sent", self.tcp_sent);
        emit_counter!(
            "grpc_proposals_total",
            "Total gRPC proposals received",
            self.grpc_proposals
        );
        emit_counter!(
            "grpc_step_calls_total",
            "Total gRPC step RPC calls received",
            self.grpc_step_calls
        );
        emit_counter!(
            "grpc_status_calls_total",
            "Total gRPC status RPC calls received",
            self.grpc_status_calls
        );
        emit_counter!(
            "transport_errors_total",
            "Total transport network and framing errors",
            self.transport_errors
        );

        emit_counter!(
            "client_proposals_total",
            "Total client proposals submitted",
            self.client_proposals_total
        );
        emit_counter!(
            "client_proposals_succeeded_total",
            "Total client proposals succeeded",
            self.client_proposals_succeeded
        );
        emit_counter!(
            "client_proposals_failed_total",
            "Total client proposals failed",
            self.client_proposals_failed
        );
        emit_counter!(
            "client_pings_total",
            "Total client pings submitted",
            self.client_pings_total
        );
        emit_counter!(
            "client_timeouts_total",
            "Total client proposal timeouts",
            self.client_timeouts_total
        );

        emit_counter!(
            "archived_entries_total",
            "Total log entries archived to sink",
            self.archived_entries_total
        );
        emit_counter!(
            "archival_batches_total",
            "Total archive compaction batches",
            self.archival_batches_total
        );
        emit_counter!(
            "archival_errors_total",
            "Total archival errors",
            self.archival_errors_total
        );

        // Helper macro inside method for formatting duration histograms
        macro_rules! emit_histogram {
            ($name:expr, $help:expr, $hist:expr) => {
                let _ = writeln!(out, "# HELP flotilla_{}_seconds {}", $name, $help);
                let _ = writeln!(out, "# TYPE flotilla_{}_seconds histogram", $name);
                for &(upper_micros, cumulative) in &$hist.buckets {
                    if upper_micros == u64::MAX {
                        let _ = writeln!(
                            out,
                            "flotilla_{}_seconds_bucket{{le=\"+Inf\"}} {}",
                            $name, cumulative
                        );
                    } else {
                        let upper_sec = upper_micros as f64 / 1_000_000.0;
                        let _ = writeln!(
                            out,
                            "flotilla_{}_seconds_bucket{{le=\"{:.6}\"}} {}",
                            $name, upper_sec, cumulative
                        );
                    }
                }
                let sum_sec = $hist.sum_micros as f64 / 1_000_000.0;
                let _ = writeln!(out, "flotilla_{}_seconds_sum {:.6}", $name, sum_sec);
                let _ = writeln!(out, "flotilla_{}_seconds_count {}", $name, $hist.count);
            };
        }

        emit_histogram!(
            "proposal_duration",
            "Client proposal duration",
            self.proposal_duration
        );
        emit_histogram!(
            "step_duration",
            "Consensus engine step processing duration",
            self.step_duration
        );
        emit_histogram!(
            "tick_duration",
            "Consensus engine tick duration",
            self.tick_duration
        );
        emit_histogram!(
            "commit_evaluation_duration",
            "Quorum commit evaluation duration",
            self.commit_evaluation_duration
        );
        emit_histogram!(
            "storage_append_duration",
            "Log slot write duration",
            self.storage_append_duration
        );
        emit_histogram!(
            "tcp_request_duration",
            "TCP request duration",
            self.tcp_request_duration
        );
        emit_histogram!(
            "grpc_proposal_duration",
            "gRPC proposal endpoint duration",
            self.grpc_proposal_duration
        );
        emit_histogram!(
            "grpc_step_duration",
            "gRPC step endpoint duration",
            self.grpc_step_duration
        );
        emit_histogram!(
            "udp_step_duration",
            "UDP poll and step duration",
            self.udp_step_duration
        );
        emit_histogram!(
            "archive_flush_duration",
            "Archival flush duration",
            self.archive_flush_duration
        );

        out
    }

    /// Format all metrics into a human-readable diagnostic report.
    pub fn to_summary_report(&self) -> String {
        let mut out = String::with_capacity(2048);
        let _ = writeln!(
            out,
            "============================================================"
        );
        let _ = writeln!(out, "Flotilla Telemetry Metrics Summary");
        let _ = writeln!(
            out,
            "============================================================"
        );
        let _ = writeln!(
            out,
            "Proposals:   proposals_total: {}, committed: {}, failed: {}",
            self.proposals_total, self.proposals_committed, self.proposals_failed
        );
        let _ = writeln!(
            out,
            "Consensus:   recv={}, sent={}, terms_adv={}, step_downs={}",
            self.messages_received_total,
            self.messages_sent_total,
            self.terms_advanced,
            self.step_downs
        );
        let _ = writeln!(
            out,
            "Elections:   started={}, won={}",
            self.elections_started, self.elections_won
        );
        let _ = writeln!(
            out,
            "Engine:      ticks={}, applied={}, appended={}",
            self.ticks_total, self.entries_applied_total, self.entries_appended_total
        );
        let _ = writeln!(
            out,
            "Transports:  udp_rx={}, udp_tx={}, tcp_rx={}, tcp_tx={}, grpc_req={}, err={}",
            self.udp_received,
            self.udp_sent,
            self.tcp_received,
            self.tcp_sent,
            self.grpc_proposals + self.grpc_step_calls,
            self.transport_errors
        );
        let _ = writeln!(
            out,
            "Client:      proposals={}, ok={}, failed={}, pings={}, timeouts={}",
            self.client_proposals_total,
            self.client_proposals_succeeded,
            self.client_proposals_failed,
            self.client_pings_total,
            self.client_timeouts_total
        );
        let _ = writeln!(
            out,
            "Archival:    entries={}, batches={}, err={}",
            self.archived_entries_total, self.archival_batches_total, self.archival_errors_total
        );
        let _ = writeln!(
            out,
            "------------------------------------------------------------"
        );
        let _ = writeln!(out, "Durations (µs):");

        macro_rules! print_dur {
            ($label:expr, $hist:expr) => {
                let _ = writeln!(
                    out,
                    "  {:24} count={:<6} avg={:<8.1} p50={:<6} p90={:<6} p99={:<6} max={}",
                    $label,
                    $hist.count,
                    $hist.mean_micros,
                    $hist.p50_micros,
                    $hist.p90_micros,
                    $hist.p99_micros,
                    $hist.max_micros
                );
            };
        }

        print_dur!("proposal_duration", self.proposal_duration);
        print_dur!("step_duration", self.step_duration);
        print_dur!("tick_duration", self.tick_duration);
        print_dur!("commit_evaluation", self.commit_evaluation_duration);
        print_dur!("storage_append", self.storage_append_duration);
        print_dur!("tcp_request", self.tcp_request_duration);
        print_dur!("grpc_proposal", self.grpc_proposal_duration);
        print_dur!("grpc_step", self.grpc_step_duration);
        print_dur!("udp_step", self.udp_step_duration);
        print_dur!("archive_flush", self.archive_flush_duration);
        let _ = writeln!(
            out,
            "============================================================"
        );

        out
    }
}
