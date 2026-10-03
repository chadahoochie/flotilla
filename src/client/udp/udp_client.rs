use super::super::client_config::ClientConfig;
use super::super::client_error::ClientError;
use super::super::flotilla_client::FlotillaClient;
use super::super::proposal_result::ProposalResult;
use crate::codec::{CodecError, decode_packet, encode_client_proposal};
use crate::message::{ClientProposalReply, MsgType};
use crate::types::{NodeId, Term};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::Duration;
use zerocopy::FromBytes;

/// UDP datagram-based client for submitting proposals to Flotilla clusters.
pub struct UdpClient {
    pub socket: UdpSocket,
    pub target_addr: SocketAddr,
    pub timeout: Duration,
}

impl UdpClient {
    /// Connect a new UDP client to the designated cluster target address.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, ClientError> {
        let mut addrs = addr.to_socket_addrs().map_err(ClientError::Io)?;
        let target_addr = addrs.next().ok_or(ClientError::NoEndpoints)?;

        let bind_addr = if target_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(bind_addr).map_err(ClientError::Io)?;
        let timeout = Duration::from_millis(500);
        socket
            .set_read_timeout(Some(timeout))
            .map_err(ClientError::Io)?;
        socket
            .set_write_timeout(Some(timeout))
            .map_err(ClientError::Io)?;

        Ok(Self {
            socket,
            target_addr,
            timeout,
        })
    }

    /// Create a new UDP client using full `ClientConfig`.
    pub fn from_config(config: &ClientConfig) -> Result<Self, ClientError> {
        let endpoint = config.endpoints.first().ok_or(ClientError::NoEndpoints)?;
        let mut client = Self::connect(endpoint)?;
        client.set_timeout(config.timeout)?;
        Ok(client)
    }

    /// Update the socket read/write timeouts.
    pub fn set_timeout(&mut self, timeout: Duration) -> Result<(), ClientError> {
        self.timeout = timeout;
        self.socket
            .set_read_timeout(Some(timeout))
            .map_err(ClientError::Io)?;
        self.socket
            .set_write_timeout(Some(timeout))
            .map_err(ClientError::Io)?;
        Ok(())
    }
}

impl FlotillaClient for UdpClient {
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
            "udp_client_propose",
            trace_id = %trace.trace_id,
            span_id = %trace.span_id,
        );
        let _enter = _span.enter();

        let mut send_buf = [0u8; 1500];
        let len = encode_client_proposal(&mut send_buf, NodeId(0), NodeId(1), Term::ZERO, payload)?;

        self.socket
            .send_to(&send_buf[..len], self.target_addr)
            .map_err(ClientError::Io)?;

        let mut recv_buf = [0u8; 1500];
        let (recv_len, _) = self
            .socket
            .recv_from(&mut recv_buf)
            .map_err(ClientError::Io)?;

        let (header, payload_bytes) = decode_packet(&recv_buf[..recv_len])?;
        let computed = crate::codec::calculate_crc32(payload_bytes);
        if computed != header.checksum {
            crate::telemetry::metrics().client_proposals_failed.inc();
            return Err(ClientError::Codec(CodecError::ChecksumMismatch {
                header_crc: header.checksum,
                computed_crc: computed,
            }));
        }

        if MsgType::from_u16(header.msg_type) != Some(MsgType::ClientProposalReply) {
            crate::telemetry::metrics().client_proposals_failed.inc();
            return Err(ClientError::RpcFailed(
                "Unexpected message type in reply".to_string(),
            ));
        }

        let reply = ClientProposalReply::read_from_prefix(payload_bytes)
            .map_err(|_| ClientError::Codec(CodecError::SerializationError))?
            .0;

        if reply.is_success() {
            crate::telemetry::metrics().client_proposals_succeeded.inc();
            Ok(ProposalResult::success(
                reply.index,
                reply.term,
                reply.leader_id,
            ))
        } else {
            crate::telemetry::metrics().client_proposals_failed.inc();
            let leader = if reply.leader_id != NodeId(0) {
                Some(reply.leader_id)
            } else {
                None
            };
            Ok(ProposalResult::failure(leader))
        }
    }

    async fn ping(&self) -> Result<bool, ClientError> {
        crate::telemetry::metrics().client_pings_total.inc();
        let res = self.propose(b"__ping__").await?;
        Ok(res.is_success())
    }
}
