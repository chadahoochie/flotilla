#[cfg(any(feature = "client-grpc", feature = "server-grpc"))]
use super::trace_context::TraceContext;

/// Standard W3C HTTP/gRPC metadata header key for distributed trace propagation.
pub const TRACEPARENT_HEADER_KEY: &str = "traceparent";

#[cfg(any(feature = "client-grpc", feature = "server-grpc"))]
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};

/// Inject a `TraceContext` into a tonic gRPC `MetadataMap` under the `traceparent` key.
#[cfg(any(feature = "client-grpc", feature = "server-grpc"))]
pub fn inject_grpc_traceparent(metadata: &mut MetadataMap, trace: &TraceContext) {
    let header_val = trace.to_traceparent();
    if let Ok(val) = MetadataValue::try_from(header_val.as_str()) {
        let key = MetadataKey::from_static(TRACEPARENT_HEADER_KEY);
        metadata.insert(key, val);
    }
}

/// Extract a `TraceContext` from a tonic gRPC `MetadataMap` if the `traceparent` key is present.
#[cfg(any(feature = "client-grpc", feature = "server-grpc"))]
pub fn extract_grpc_traceparent(metadata: &MetadataMap) -> Option<TraceContext> {
    let header_val = metadata.get(TRACEPARENT_HEADER_KEY)?;
    let val_str = header_val.to_str().ok()?;
    TraceContext::from_traceparent(val_str)
}
