use super::super::client_config::ClientConfig;
use super::super::client_error::ClientError;
use super::super::flotilla_client::FlotillaClient;
use super::super::proposal_result::ProposalResult;
use super::mod_proto::flotilla_service_client::FlotillaServiceClient;
use super::mod_proto::{ProposalRequest, StatusRequest};
use crate::types::{LogIndex, NodeId, Term};
use std::time::Duration;

/// gRPC client for communicating with Flotilla consensus clusters over HTTP/2.
pub struct GrpcClient {
    pub endpoint: String,
    pub timeout: Duration,
}

impl GrpcClient {
    /// Create a new gRPC client targeting the designated endpoint URI.
    pub fn connect<S: Into<String>>(endpoint: S) -> Self {
        Self {
            endpoint: endpoint.into(),
            timeout: Duration::from_millis(1000),
        }
    }

    /// Create a new gRPC client from a `ClientConfig`.
    pub fn from_config(config: &ClientConfig) -> Result<Self, ClientError> {
        let endpoint = config.endpoints.first().ok_or(ClientError::NoEndpoints)?;
        Ok(Self {
            endpoint: endpoint.clone(),
            timeout: config.timeout,
        })
    }

    /// Set the operation timeout.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }
}

impl FlotillaClient for GrpcClient {
    async fn propose(&self, payload: &[u8]) -> Result<ProposalResult, ClientError> {
        let trace = crate::telemetry::TraceContext::new_root();
        self.propose_with_trace(payload, &trace).await
    }

    async fn propose_with_trace(
        &self,
        payload: &[u8],
        trace: &crate::telemetry::TraceContext,
    ) -> Result<ProposalResult, ClientError> {
        let _timer =
            crate::telemetry::DurationTimer::start(&crate::telemetry::metrics().proposal_duration);
        crate::telemetry::metrics().client_proposals_total.inc();

        let _span = tracing::info_span!(
            "grpc_client_propose",
            trace_id = %trace.trace_id,
            span_id = %trace.span_id,
        );
        #[cfg(feature = "otel")]
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            _span.set_parent(crate::telemetry::trace_context_to_otel_context(trace));
        }
        let _enter = _span.enter();

        let mut client = tokio::time::timeout(
            self.timeout,
            FlotillaServiceClient::connect(self.endpoint.clone()),
        )
        .await
        .map_err(|_| {
            crate::telemetry::metrics().client_timeouts_total.inc();
            ClientError::Timeout
        })?
        .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        let req_proto = ProposalRequest {
            payload: payload.to_vec(),
        };

        let mut request = tonic::Request::new(req_proto);
        crate::telemetry::inject_grpc_traceparent(request.metadata_mut(), trace);

        let response = tokio::time::timeout(self.timeout, client.propose(request))
            .await
            .map_err(|_| {
                crate::telemetry::metrics().client_timeouts_total.inc();
                ClientError::Timeout
            })?
            .map_err(|e| {
                crate::telemetry::metrics().client_proposals_failed.inc();
                ClientError::RpcFailed(e.to_string())
            })?
            .into_inner();

        if response.success {
            crate::telemetry::metrics().client_proposals_succeeded.inc();
            Ok(ProposalResult::success(
                LogIndex(response.index),
                Term(response.term),
                NodeId(response.leader_id),
            ))
        } else {
            crate::telemetry::metrics().client_proposals_failed.inc();
            let leader = if response.leader_id != 0 {
                Some(NodeId(response.leader_id))
            } else {
                None
            };
            Ok(ProposalResult::failure(leader))
        }
    }

    async fn ping(&self) -> Result<bool, ClientError> {
        crate::telemetry::metrics().client_pings_total.inc();
        let mut client = tokio::time::timeout(
            self.timeout,
            FlotillaServiceClient::connect(self.endpoint.clone()),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        let response = tokio::time::timeout(
            self.timeout,
            client.cluster_status(tonic::Request::new(StatusRequest {})),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(|e| ClientError::RpcFailed(e.to_string()))?;

        Ok(!response.into_inner().role.is_empty())
    }
}
