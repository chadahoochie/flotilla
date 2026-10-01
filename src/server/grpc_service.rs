use super::mod_proto::flotilla_service_server::{
    FlotillaService as FlotillaRpcService, FlotillaServiceServer,
};
use super::mod_proto::{
    OutboundPacket, ProposalRequest, ProposalResponse, StatusRequest, StatusResponse, StepRequest,
    StepResponse,
};
use crate::engine::{OutboundMessage, RaftNode};
use crate::types::{NodeId, Role};
use parking_lot::Mutex;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// gRPC service exposing Flotilla consensus node endpoints over HTTP/2.
pub struct GrpcService<const CAPACITY: usize = 1024, const MAX_PAYLOAD: usize = 1024> {
    pub node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>,
}

impl<const CAPACITY: usize, const MAX_PAYLOAD: usize> GrpcService<CAPACITY, MAX_PAYLOAD> {
    /// Create a new gRPC service wrapping a shared consensus node.
    pub fn new(node: Arc<Mutex<RaftNode<CAPACITY, MAX_PAYLOAD>>>) -> Self {
        Self { node }
    }

    /// Serve the gRPC service on the designated socket address.
    pub fn serve(
        self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<(), tonic::transport::Error>> {
        tonic::transport::Server::builder()
            .add_service(FlotillaServiceServer::new(self))
            .serve(addr)
    }
}

#[tonic::async_trait]
impl<const CAPACITY: usize, const MAX_PAYLOAD: usize> FlotillaRpcService
    for GrpcService<CAPACITY, MAX_PAYLOAD>
{
    async fn propose(
        &self,
        request: Request<ProposalRequest>,
    ) -> Result<Response<ProposalResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.lock();

        if node.role() == Role::Leader {
            match node.propose(&req.payload) {
                Ok(idx) => Ok(Response::new(ProposalResponse {
                    success: true,
                    index: idx.0,
                    term: node.current_term().0,
                    leader_id: node.election.config.node_id.0,
                    error_message: String::new(),
                })),
                Err(e) => Ok(Response::new(ProposalResponse {
                    success: false,
                    index: 0,
                    term: node.current_term().0,
                    leader_id: node.election.config.node_id.0,
                    error_message: e.to_string(),
                })),
            }
        } else {
            let leader = if node.election.voted_for != NodeId::NONE {
                node.election.voted_for.0
            } else {
                0
            };
            Ok(Response::new(ProposalResponse {
                success: false,
                index: 0,
                term: node.current_term().0,
                leader_id: leader,
                error_message: "Not leader".to_string(),
            }))
        }
    }

    async fn step(&self, request: Request<StepRequest>) -> Result<Response<StepResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.lock();

        match node.step(NodeId(req.sender_id), &req.packet) {
            Ok(actions) => {
                let mut packets = Vec::with_capacity(actions.len());
                for act in actions {
                    if let OutboundMessage::SendPacket { to, packet } = act {
                        packets.push(OutboundPacket {
                            to_node_id: to.0,
                            packet,
                        });
                    }
                }
                Ok(Response::new(StepResponse { packets }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn cluster_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let node = self.node.lock();
        let leader = if node.role() == Role::Leader {
            node.election.config.node_id.0
        } else if node.election.voted_for != NodeId::NONE {
            node.election.voted_for.0
        } else {
            0
        };

        Ok(Response::new(StatusResponse {
            node_id: node.election.config.node_id.0,
            role: format!("{:?}", node.role()),
            term: node.current_term().0,
            commit_index: node.commit_index().0,
            leader_id: leader,
        }))
    }
}
