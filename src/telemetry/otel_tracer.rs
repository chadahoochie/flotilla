use super::otel_config::OtelConfig;
use super::otel_guard::OtelGuard;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::Sampler;

/// OpenTelemetry tracer pipeline initializer and manager.
pub struct OtelTracer;

impl OtelTracer {
    /// Initialize OTLP tracer and configure the global tracer provider, returning the concrete SDK tracer.
    pub fn init(
        config: &OtelConfig,
    ) -> Result<
        (opentelemetry_sdk::trace::Tracer, OtelGuard),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        if !config.enabled {
            let provider = opentelemetry_sdk::trace::TracerProvider::builder().build();
            let tracer = provider.tracer("flotilla-server");
            return Ok((tracer, OtelGuard::new()));
        }

        let sampler = if config.sample_ratio >= 1.0 {
            Sampler::AlwaysOn
        } else if config.sample_ratio <= 0.0 {
            Sampler::AlwaysOff
        } else {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(config.sample_ratio)))
        };

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .build()?;

        let resource = Resource::new(vec![
            KeyValue::new("service.name", config.service_name.clone()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ]);

        let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_sampler(sampler)
            .with_resource(resource.clone())
            .build();

        let tracer = tracer_provider.tracer("flotilla-server");
        opentelemetry::global::set_tracer_provider(tracer_provider);

        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .build()?;

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
            metric_exporter,
            opentelemetry_sdk::runtime::Tokio,
        )
        .with_interval(std::time::Duration::from_secs(config.metrics_export_interval_secs))
        .build();

        let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();

        opentelemetry::global::set_meter_provider(meter_provider.clone());

        Ok((tracer, OtelGuard::with_meter_provider(meter_provider)))
    }
}

/// Standalone helper function to initialize the OpenTelemetry tracer.
pub fn init_otel_tracer(
    config: &OtelConfig,
) -> Result<(opentelemetry_sdk::trace::Tracer, OtelGuard), Box<dyn std::error::Error + Send + Sync>>
{
    OtelTracer::init(config)
}
