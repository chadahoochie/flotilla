/// RAII guard ensuring graceful flush and shutdown of OpenTelemetry providers upon daemon termination.
pub struct OtelGuard {
    _meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
}

impl OtelGuard {
    /// Construct a new shutdown guard without a meter provider.
    pub fn new() -> Self {
        Self { _meter_provider: None }
    }

    /// Construct a new shutdown guard with a meter provider.
    pub fn with_meter_provider(meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider) -> Self {
        Self {
            _meter_provider: Some(meter_provider),
        }
    }
}

impl Default for OtelGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}
