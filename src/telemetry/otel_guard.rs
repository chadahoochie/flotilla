/// RAII guard ensuring graceful flush and shutdown of OpenTelemetry providers upon daemon termination.
pub struct OtelGuard {
    _private: (),
}

impl OtelGuard {
    /// Construct a new shutdown guard.
    pub fn new() -> Self {
        Self { _private: () }
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
