use std::env;

/// Configuration settings for exporting OpenTelemetry traces and metrics.
#[derive(Clone, Debug, PartialEq)]
pub struct OtelConfig {
    /// OTLP gRPC endpoint URL (e.g. `http://127.0.0.1:4317`).
    pub endpoint: String,
    /// Service name identified in distributed traces and metrics resources.
    pub service_name: String,
    /// Sampling ratio for trace collection between 0.0 and 1.0.
    pub sample_ratio: f64,
    /// Interval in seconds for publishing metric snapshots to OpenTelemetry instruments.
    pub metrics_export_interval_secs: u64,
    /// Whether OpenTelemetry export is actively enabled.
    pub enabled: bool,
}

impl OtelConfig {
    /// Create an explicit OpenTelemetry configuration.
    pub fn new(
        endpoint: String,
        service_name: String,
        sample_ratio: f64,
        metrics_export_interval_secs: u64,
        enabled: bool,
    ) -> Self {
        Self {
            endpoint,
            service_name,
            sample_ratio: sample_ratio.clamp(0.0, 1.0),
            metrics_export_interval_secs,
            enabled,
        }
    }

    /// Construct configuration from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        let endpoint = env::var("FLOTILLA_OTEL_ENDPOINT")
            .or_else(|_| env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string());

        let service_name = env::var("FLOTILLA_OTEL_SERVICE_NAME")
            .or_else(|_| env::var("OTEL_SERVICE_NAME"))
            .unwrap_or_else(|_| "flotilla".to_string());

        let sample_ratio = env::var("FLOTILLA_OTEL_SAMPLE_RATIO")
            .or_else(|_| env::var("OTEL_TRACES_SAMPLER_ARG"))
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1.0);

        let metrics_export_interval_secs = env::var("FLOTILLA_OTEL_METRICS_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let enabled = env::var("FLOTILLA_OTEL_ENABLED")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);

        Self::new(
            endpoint,
            service_name,
            sample_ratio,
            metrics_export_interval_secs,
            enabled,
        )
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:4317".to_string(),
            service_name: "flotilla".to_string(),
            sample_ratio: 1.0,
            metrics_export_interval_secs: 10,
            enabled: true,
        }
    }
}
