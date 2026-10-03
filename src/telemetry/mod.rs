//! Distributed telemetry, W3C trace context propagation, and high-resolution metrics.
//!
//! Provides lock-free atomic counters, duration histograms, and W3C `traceparent`
//! context propagation across transports (UDP datagrams, TCP streams, and HTTP/2 gRPC).

pub mod counter;
pub mod duration_histogram;
pub mod duration_timer;
pub mod flotilla_metrics;
pub mod histogram_snapshot;
pub mod metrics_snapshot;
pub mod propagation;
pub mod span_id;
pub mod telemetry_error;
pub mod trace_context;
pub mod trace_envelope;
pub mod trace_flags;
pub mod trace_id;

pub use counter::Counter;
pub use duration_histogram::{DURATION_BUCKETS, DurationHistogram};
pub use duration_timer::DurationTimer;
pub use flotilla_metrics::FlotillaMetrics;
pub use histogram_snapshot::HistogramSnapshot;
pub use metrics_snapshot::MetricsSnapshot;
pub use propagation::TRACEPARENT_HEADER_KEY;
#[cfg(any(feature = "client-grpc", feature = "server-grpc"))]
pub use propagation::{extract_grpc_traceparent, inject_grpc_traceparent};
pub use span_id::SpanId;
pub use telemetry_error::TelemetryError;
pub use trace_context::TraceContext;
pub use trace_envelope::{ENVELOPE_HEADER_SIZE, TraceEnvelope};
pub use trace_flags::TraceFlags;
pub use trace_id::TraceId;

/// Return a static reference to the global Flotilla telemetry metrics registry.
#[inline(always)]
pub fn metrics() -> &'static FlotillaMetrics {
    FlotillaMetrics::global()
}
