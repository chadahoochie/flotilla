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
        let mut client = tokio::time::timeout(
            self.timeout,
            FlotillaServiceClient::connect(self.endpoint.clone()),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        let req = ProposalRequest {
            payload: payload.to_vec(),
        };

        let response = tokio::time::timeout(self.timeout, client.propose(tonic::Request::new(req)))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|e| ClientError::RpcFailed(e.to_string()))?
            .into_inner();

        if response.success {
            Ok(ProposalResult::success(
                LogIndex(response.index),
                Term(response.term),
                NodeId(response.leader_id),
            ))
        } else {
            let leader = if response.leader_id != 0 {
                Some(NodeId(response.leader_id))
            } else {
                None
            };
            Ok(ProposalResult::failure(leader))
        }
    }

    async fn ping(&self) -> Result<bool, ClientError> {
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
