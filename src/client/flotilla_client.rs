use super::client_error::ClientError;
use super::proposal_result::ProposalResult;
use std::future::Future;

/// Unified client interface for submitting proposals and pinging a Flotilla consensus cluster.
pub trait FlotillaClient: Send + Sync {
    /// Submit a command payload to the cluster leader for quorum replication.
    fn propose(
        &self,
        payload: &[u8],
    ) -> impl Future<Output = Result<ProposalResult, ClientError>> + Send;

    /// Ping cluster node to verify health and connectivity.
    fn ping(&self) -> impl Future<Output = Result<bool, ClientError>> + Send;
}
