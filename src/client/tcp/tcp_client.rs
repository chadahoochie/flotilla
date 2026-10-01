use super::super::client_config::ClientConfig;
use super::super::client_error::ClientError;
use super::super::flotilla_client::FlotillaClient;
use super::super::proposal_result::ProposalResult;
use super::framing::{read_packet_frame, write_packet_frame};
use crate::codec::{CodecError, decode_packet, encode_client_proposal};
use crate::message::{ClientProposalReply, MsgType};
use crate::types::{NodeId, Term};
use std::time::Duration;
use tokio::net::TcpStream;
use zerocopy::FromBytes;

/// TCP connection-oriented client for submitting proposals to Flotilla clusters.
pub struct TcpClient {
    pub target_addr: String,
    pub timeout: Duration,
}

impl TcpClient {
    /// Create a new TCP client pointing to the designated target address.
    pub fn connect<S: Into<String>>(addr: S) -> Self {
        Self {
            target_addr: addr.into(),
            timeout: Duration::from_millis(1000),
        }
    }

    /// Create a new TCP client from a `ClientConfig`.
    pub fn from_config(config: &ClientConfig) -> Result<Self, ClientError> {
        let endpoint = config.endpoints.first().ok_or(ClientError::NoEndpoints)?;
        Ok(Self {
            target_addr: endpoint.clone(),
            timeout: config.timeout,
        })
    }

    /// Set the operation timeout.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }
}

impl FlotillaClient for TcpClient {
    async fn propose(&self, payload: &[u8]) -> Result<ProposalResult, ClientError> {
        let mut stream = tokio::time::timeout(self.timeout, TcpStream::connect(&self.target_addr))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(ClientError::Io)?;

        let mut send_buf = [0u8; 1500];
        let len = encode_client_proposal(&mut send_buf, NodeId(0), NodeId(1), Term::ZERO, payload)?;

        tokio::time::timeout(
            self.timeout,
            write_packet_frame(&mut stream, &send_buf[..len]),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(ClientError::Io)?;

        let mut recv_buf = [0u8; 1500];
        let recv_len =
            tokio::time::timeout(self.timeout, read_packet_frame(&mut stream, &mut recv_buf))
                .await
                .map_err(|_| ClientError::Timeout)?
                .map_err(ClientError::Io)?;

        let (header, payload_bytes) = decode_packet(&recv_buf[..recv_len])?;
        let computed = crate::codec::calculate_crc32(payload_bytes);
        if computed != header.checksum {
            return Err(ClientError::Codec(CodecError::ChecksumMismatch {
                header_crc: header.checksum,
                computed_crc: computed,
            }));
        }

        if MsgType::from_u16(header.msg_type) != Some(MsgType::ClientProposalReply) {
            return Err(ClientError::RpcFailed(
                "Unexpected message type in reply".to_string(),
            ));
        }

        let reply = ClientProposalReply::read_from_prefix(payload_bytes)
            .map_err(|_| ClientError::Codec(CodecError::SerializationError))?
            .0;

        if reply.is_success() {
            Ok(ProposalResult::success(
                reply.index,
                reply.term,
                reply.leader_id,
            ))
        } else {
            let leader = if reply.leader_id != NodeId(0) {
                Some(reply.leader_id)
            } else {
                None
            };
            Ok(ProposalResult::failure(leader))
        }
    }

    async fn ping(&self) -> Result<bool, ClientError> {
        let stream =
            tokio::time::timeout(self.timeout, TcpStream::connect(&self.target_addr)).await;
        match stream {
            Ok(Ok(_)) => Ok(true),
            Ok(Err(e)) => Err(ClientError::Io(e)),
            Err(_) => Err(ClientError::Timeout),
        }
    }
}
