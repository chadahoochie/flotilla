use super::span_id::SpanId;
use super::trace_context::TraceContext;
use super::trace_flags::TraceFlags;
use super::trace_id::TraceId;
use opentelemetry::Context;
use opentelemetry::trace::{
    SpanContext, SpanId as OtelSpanId, TraceContextExt, TraceFlags as OtelTraceFlags,
    TraceId as OtelTraceId,
};

/// Convert a Flotilla zero-copy `TraceContext` into an OpenTelemetry `SpanContext`.
pub fn trace_context_to_span_context(ctx: &TraceContext) -> SpanContext {
    if ctx.trace_id.is_nil() || ctx.span_id.is_nil() {
        return SpanContext::empty_context();
    }

    SpanContext::new(
        OtelTraceId::from_bytes(*ctx.trace_id.as_bytes()),
        OtelSpanId::from_bytes(*ctx.span_id.as_bytes()),
        OtelTraceFlags::new(ctx.flags.0),
        true, // is_remote
        Default::default(),
    )
}

/// Attach a Flotilla `TraceContext` to an OpenTelemetry `Context` as the active remote parent.
pub fn trace_context_to_otel_context(ctx: &TraceContext) -> Context {
    let span_ctx = trace_context_to_span_context(ctx);
    if !span_ctx.is_valid() {
        return Context::new();
    }
    Context::current().with_remote_span_context(span_ctx)
}

/// Extract a Flotilla `TraceContext` from an active OpenTelemetry `Context`.
pub fn otel_context_to_trace_context(cx: &Context) -> Option<TraceContext> {
    let span = cx.span();
    let span_ctx = span.span_context();
    if !span_ctx.is_valid() {
        return None;
    }

    let trace_id = TraceId::from_bytes(span_ctx.trace_id().to_bytes());
    let span_id = SpanId::from_bytes(span_ctx.span_id().to_bytes());
    let flags = TraceFlags(span_ctx.trace_flags().to_u8());

    Some(TraceContext::new(trace_id, span_id, flags))
}
