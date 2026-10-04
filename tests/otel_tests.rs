#![cfg(feature = "otel")]

use flotilla_raft::telemetry::{
    FlotillaMetrics, OtelConfig, SpanId, TraceContext, TraceFlags, TraceId,
    otel_context_to_trace_context, publish_snapshot_to_otel, trace_context_to_otel_context,
    trace_context_to_span_context,
};

#[test]
fn test_otel_config_defaults_and_customization() {
    let default_config = OtelConfig::default();
    assert_eq!(default_config.endpoint, "http://127.0.0.1:4317");
    assert_eq!(default_config.service_name, "flotilla");
    assert!((default_config.sample_ratio - 1.0).abs() < f64::EPSILON);
    assert_eq!(default_config.metrics_export_interval_secs, 10);
    assert!(default_config.enabled);

    let custom = OtelConfig::new(
        "http://collector:4317".to_string(),
        "flotilla-node-3".to_string(),
        0.5,
        5,
        true,
    );
    assert_eq!(custom.endpoint, "http://collector:4317");
    assert_eq!(custom.service_name, "flotilla-node-3");
    assert!((custom.sample_ratio - 0.5).abs() < f64::EPSILON);
    assert_eq!(custom.metrics_export_interval_secs, 5);
    assert!(custom.enabled);
}

#[test]
fn test_trace_context_to_span_context_conversion() {
    let trace_id = TraceId::generate();
    let span_id = SpanId::generate();
    let original = TraceContext::new(trace_id, span_id, TraceFlags::SAMPLED);

    let span_ctx = trace_context_to_span_context(&original);
    assert!(span_ctx.is_valid());
    assert!(span_ctx.is_remote());
    assert_eq!(
        span_ctx.trace_id().to_bytes(),
        *original.trace_id.as_bytes()
    );
    assert_eq!(span_ctx.span_id().to_bytes(), *original.span_id.as_bytes());
    assert!(span_ctx.is_sampled());

    // Roundtrip back to TraceContext through opentelemetry Context
    let cx = trace_context_to_otel_context(&original);
    let reconstructed = otel_context_to_trace_context(&cx).expect("Must reconstruct TraceContext");

    assert_eq!(original.trace_id, reconstructed.trace_id);
    assert_eq!(original.span_id, reconstructed.span_id);
    assert_eq!(original.flags, reconstructed.flags);
}

#[test]
fn test_otel_context_parenting_and_nil_handling() {
    let nil_ctx = TraceContext::NIL;
    let span_ctx = trace_context_to_span_context(&nil_ctx);
    assert!(!span_ctx.is_valid());

    let empty_cx = opentelemetry::Context::new();
    assert!(otel_context_to_trace_context(&empty_cx).is_none());
}

#[test]
fn test_publish_snapshot_to_otel() {
    let metrics = FlotillaMetrics::new();
    metrics.proposals_total.inc();
    metrics.proposals_committed.inc();
    metrics.messages_received_total.inc_by(5);
    metrics.messages_sent_total.inc_by(10);
    metrics.proposal_duration.record_micros(150);

    let snapshot = metrics.snapshot();
    let meter = opentelemetry::global::meter("flotilla_test");

    // Publishing snapshot to OpenTelemetry instruments must succeed cleanly
    publish_snapshot_to_otel(&snapshot, &meter);

    // Also test with step and tick durations populated
    metrics.step_duration.record_micros(250);
    metrics.tick_duration.record_micros(500);
    let snapshot_with_durations = metrics.snapshot();
    publish_snapshot_to_otel(&snapshot_with_durations, &meter);
}

#[test]
fn test_otel_config_from_env() {
    unsafe {
        std::env::set_var("FLOTILLA_OTEL_ENDPOINT", "http://env-host:4317");
        std::env::set_var("FLOTILLA_OTEL_SERVICE_NAME", "flotilla-env-node");
        std::env::set_var("FLOTILLA_OTEL_SAMPLE_RATIO", "0.25");
        std::env::set_var("FLOTILLA_OTEL_METRICS_INTERVAL_SECS", "15");
        std::env::set_var("FLOTILLA_OTEL_ENABLED", "true");
    }

    let config = OtelConfig::from_env();
    assert_eq!(config.endpoint, "http://env-host:4317");
    assert_eq!(config.service_name, "flotilla-env-node");
    assert!((config.sample_ratio - 0.25).abs() < f64::EPSILON);
    assert_eq!(config.metrics_export_interval_secs, 15);
    assert!(config.enabled);

    unsafe {
        std::env::remove_var("FLOTILLA_OTEL_ENDPOINT");
        std::env::remove_var("FLOTILLA_OTEL_SERVICE_NAME");
        std::env::remove_var("FLOTILLA_OTEL_SAMPLE_RATIO");
        std::env::remove_var("FLOTILLA_OTEL_METRICS_INTERVAL_SECS");
        std::env::remove_var("FLOTILLA_OTEL_ENABLED");
    }
}

#[test]
fn test_otel_guard_and_publisher_constructors() {
    use flotilla_raft::telemetry::{OtelGuard, OtelMetricsPublisher};

    let guard = OtelGuard::new();
    let default_guard = OtelGuard::default();
    drop(guard);
    drop(default_guard);

    let publisher = OtelMetricsPublisher::new(10);
    assert_eq!(publisher.interval_secs, 10);
}
