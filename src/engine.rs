use crate::codec::{
    decode_packet, encode_append_entries, encode_append_entries_reply, encode_request_vote_args,
    encode_request_vote_reply, verify_checksum, CodecError, HEADER_SIZE,
};
use crate::commit::evaluate_commit_advancement;
use crate::election::{ElectionAction, ElectionConfig, ElectionState};
use crate::message::{
    AppendEntriesHeader, AppendEntriesReply, MsgType, RequestVoteArgs, RequestVoteReply,
};
use crate::replication::{
    evaluate_follower_append_entries, FollowerAppendResult, PeerProgressTracker,
};
use crate::storage::ring_buffer::{RingBufferLogStorage, StorageError};
use crate::types::{LogIndex, NodeId, Role, Term};
use zerocopy::FromBytes;

/// Errors produced during Raft consensus stepping and proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineError {
    NotLeader,
    Storage(StorageError),
    Codec(CodecError),
    InvalidPacket,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader => write!(f, "current node is not the elected leader"),
            Self::Storage(e) => write!(f, "storage engine error: {e}"),
            Self::Codec(e) => write!(f, "codec serialization error: {e}"),
            Self::InvalidPacket => write!(f, "inbound packet failed decoding or verification"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<StorageError> for EngineError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

impl From<CodecError> for EngineError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}

/// Outbound action emitted by the sans-I/O consensus state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundMessage {
    SendPacket { to: NodeId, packet: Vec<u8> },
    ApplyEntries { from_index: LogIndex, to_index: LogIndex },
}

/// Configuration options for a Raft node instance.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_timeout_ticks: u32,
    pub heartbeat_interval_ticks: u32,
}

/// Standalone pure constructor for RequestVote packet envelopes.
pub fn create_request_vote_packet(
    sender: NodeId,
    receiver: NodeId,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term,
) -> Result<Vec<u8>, CodecError> {
    let args = RequestVoteArgs {
        term,
        candidate_id: sender,
        last_log_index,
        last_log_term,
    };
    let mut buf = [0u8; 128];
    let len = encode_request_vote_args(&mut buf, sender, receiver, term, &args)?;
    Ok(buf[..len].to_vec())
}

/// Standalone pure constructor for RequestVote reply packet envelopes.
pub fn create_request_vote_reply_packet(
    sender: NodeId,
    receiver: NodeId,
    term: Term,
    granted: bool,
) -> Result<Vec<u8>, CodecError> {
    let reply = RequestVoteReply {
        term,
        vote_granted: if granted { 1 } else { 0 },
        _pad: [0; 7],
    };
    let mut buf = [0u8; 128];
    let len = encode_request_vote_reply(&mut buf, sender, receiver, term, &reply)?;
    Ok(buf[..len].to_vec())
}

/// Standalone pure constructor for AppendEntries packet envelopes.
pub fn create_append_entries_packet(
    receiver: NodeId,
    header: &AppendEntriesHeader,
    entries_raw: &[u8],
) -> Result<Vec<u8>, CodecError> {
    let total_needed = HEADER_SIZE + std::mem::size_of::<AppendEntriesHeader>() + entries_raw.len();
    let mut buf = vec![0u8; total_needed];
    let len = encode_append_entries(
        &mut buf,
        header.leader_id,
        receiver,
        header.term,
        header,
        entries_raw,
    )?;
    buf.truncate(len);
    Ok(buf)
}

/// Standalone pure constructor for AppendEntries reply packet envelopes.
pub fn create_append_entries_reply_packet(
    sender: NodeId,
    receiver: NodeId,
    term: Term,
    success: bool,
    match_index: LogIndex,
) -> Result<Vec<u8>, CodecError> {
    let reply = AppendEntriesReply {
        term,
        follower_id: sender,
        success: if success { 1 } else { 0 },
        _pad: [0; 7],
        match_index,
    };
    let mut buf = [0u8; 128];
    let len = encode_append_entries_reply(&mut buf, sender, receiver, term, &reply)?;
    Ok(buf[..len].to_vec())
}

/// Sans-I/O Raft consensus engine node.
pub struct RaftNode<const CAPACITY: usize = 1024, const MAX_PAYLOAD: usize = 1024> {
    pub election: ElectionState,
    pub storage: RingBufferLogStorage<CAPACITY, MAX_PAYLOAD>,
    pub progress: PeerProgressTracker,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub peers: Vec<NodeId>,
}

impl<const CAPACITY: usize, const MAX_PAYLOAD: usize> RaftNode<CAPACITY, MAX_PAYLOAD> {
    /// Initialize a new Sans-I/O Raft engine node.
    pub fn new(config: RaftConfig) -> Self {
        let cluster_size = config.peers.len() + 1;
        let election_config = ElectionConfig {
            node_id: config.node_id,
            cluster_size,
            election_timeout_ticks: config.election_timeout_ticks,
            heartbeat_interval_ticks: config.heartbeat_interval_ticks,
        };

        let election = ElectionState::new(election_config);
        let storage = RingBufferLogStorage::new();
        let progress = PeerProgressTracker::new(&config.peers, LogIndex::ZERO);

        Self {
            election,
            storage,
            progress,
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            peers: config.peers,
        }
    }

    /// Return the node's current role.
    #[inline(always)]
    pub fn role(&self) -> Role {
        self.election.role
    }

    /// Return the node's current term.
    #[inline(always)]
    pub fn current_term(&self) -> Term {
        self.election.current_term
    }

    /// Return the current commit index.
    #[inline(always)]
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Return the last applied log index.
    #[inline(always)]
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Return the highest appended log index.
    #[inline(always)]
    pub fn last_log_index(&self) -> LogIndex {
        self.storage.last_index()
    }

    /// Return the term of the highest appended log entry.
    pub fn last_log_term(&self) -> Term {
        let last_idx = self.storage.last_index();
        if last_idx.0 == 0 {
            Term::ZERO
        } else {
            self.storage.term_at(last_idx).unwrap_or(Term::ZERO)
        }
    }

    /// Advance the logical timer by one tick.
    pub fn tick(&mut self) -> Vec<OutboundMessage> {
        let action = self.election.handle_tick();
        match action {
            ElectionAction::Campaign => self.broadcast_request_vote(),
            ElectionAction::ElectedLeader => {
                self.progress.reset_all_next_indices(self.storage.last_index());
                self.broadcast_heartbeats()
            }
            ElectionAction::HeartbeatTimeout => self.broadcast_heartbeats(),
            ElectionAction::StepDown(_) | ElectionAction::None => Vec::new(),
        }
    }

    /// Broadcast RequestVote RPCs to all peers.
    pub fn broadcast_request_vote(&self) -> Vec<OutboundMessage> {
        let mut msgs = Vec::with_capacity(self.peers.len());
        let last_idx = self.last_log_index();
        let last_term = self.last_log_term();
        for &peer in &self.peers {
            if let Ok(packet) = create_request_vote_packet(
                self.election.config.node_id,
                peer,
                self.current_term(),
                last_idx,
                last_term,
            ) {
                msgs.push(OutboundMessage::SendPacket { to: peer, packet });
            }
        }
        msgs
    }

    /// Broadcast heartbeat AppendEntries (empty entries) to all peers.
    pub fn broadcast_heartbeats(&mut self) -> Vec<OutboundMessage> {
        let mut msgs = Vec::with_capacity(self.peers.len());
        let sender = self.election.config.node_id;
        let term = self.current_term();
        let leader_commit = self.commit_index;

        for peer in &self.peers {
            let next_idx = self.progress.get_next_index(*peer);
            let prev_idx = LogIndex(next_idx.0.saturating_sub(1));
            let prev_term = if prev_idx.0 == 0 {
                Term::ZERO
            } else {
                self.storage.term_at(prev_idx).unwrap_or(Term::ZERO)
            };

            // If follower is behind, send the entry at next_idx
            let (entries_raw, entries_count) = if next_idx.0 <= self.storage.last_index().0 {
                if let Some(entry) = self.storage.entry_at(next_idx) {
                    (entry.payload_bytes(), 1)
                } else {
                    (&[][..], 0)
                }
            } else {
                (&[][..], 0)
            };

            let header = AppendEntriesHeader {
                term,
                leader_id: sender,
                prev_log_index: prev_idx,
                prev_log_term: prev_term,
                leader_commit,
                entries_count,
                _pad: [0; 4],
            };

            if let Ok(packet) = create_append_entries_packet(
                *peer,
                &header,
                entries_raw,
            ) {
                msgs.push(OutboundMessage::SendPacket { to: *peer, packet });
            }
        }
        msgs
    }

    /// Client proposal: append entry to local log and replicate to peers.
    pub fn propose(&mut self, payload: &[u8]) -> Result<LogIndex, EngineError> {
        if self.role() != Role::Leader {
            return Err(EngineError::NotLeader);
        }

        let new_index = self.storage.append_entry(self.current_term(), payload)?;
        Ok(new_index)
    }

    /// Compact the storage ring buffer up to the given persisted watermark.
    pub fn compact_watermark(&mut self, watermark: LogIndex) -> Result<(), StorageError> {
        self.storage.compact_prefix(watermark)
    }

    /// Step the sans-I/O state engine with an inbound datagram from `sender`.
    pub fn step(
        &mut self,
        sender: NodeId,
        packet_bytes: &[u8],
    ) -> Result<Vec<OutboundMessage>, EngineError> {
        let (header, payload) = decode_packet(packet_bytes)?;
        if !verify_checksum(&header, payload) {
            return Err(EngineError::InvalidPacket);
        }

        let mut actions = Vec::new();

        // 1. Check peer term for step-down
        let step_action = self.election.check_peer_term(header.term);
        if let ElectionAction::StepDown(_) = step_action {
            // Reverted to follower
        }

        let msg_type = MsgType::from_u16(header.msg_type).ok_or(EngineError::InvalidPacket)?;

        match msg_type {
            MsgType::RequestVoteArgs => {
                let args = RequestVoteArgs::read_from_prefix(payload)
                    .map_err(|_| EngineError::InvalidPacket)?
                    .0;
                let reply = self.election.handle_request_vote(
                    &args,
                    self.last_log_term(),
                    self.last_log_index(),
                );
                let packet = create_request_vote_reply_packet(
                    self.election.config.node_id,
                    sender,
                    reply.term,
                    reply.is_granted(),
                )?;
                actions.push(OutboundMessage::SendPacket { to: sender, packet });
            }

            MsgType::RequestVoteReply => {
                let reply = RequestVoteReply::read_from_prefix(payload)
                    .map_err(|_| EngineError::InvalidPacket)?
                    .0;
                let action = self.election.handle_vote_reply(sender, &reply);
                if action == ElectionAction::ElectedLeader {
                    self.progress.reset_all_next_indices(self.storage.last_index());
                    actions.extend(self.broadcast_heartbeats());
                }
            }

            MsgType::AppendEntriesArgs => {
                let (hdr, entries_raw) = AppendEntriesHeader::read_from_prefix(payload)
                    .map_err(|_| EngineError::InvalidPacket)?;

                // Reply false if term < current_term (Raft §5.1)
                if hdr.term.0 < self.current_term().0 {
                    let packet = create_append_entries_reply_packet(
                        self.election.config.node_id,
                        sender,
                        self.current_term(),
                        false,
                        self.storage.last_index(),
                    )?;
                    actions.push(OutboundMessage::SendPacket { to: sender, packet });
                    return Ok(actions);
                }

                // Valid leader recognized: reset election timer
                self.election.election_elapsed = 0;
                if self.role() == Role::Candidate {
                    self.election.role = Role::Follower;
                }

                let prev_commit = self.commit_index;
                let res = evaluate_follower_append_entries(
                    &hdr,
                    entries_raw,
                    &mut self.storage,
                    &mut self.commit_index,
                );

                let (success, match_idx) = match res {
                    FollowerAppendResult::Success { match_index } => (true, match_index),
                    FollowerAppendResult::Rejected => (false, self.storage.last_index()),
                };

                let packet = create_append_entries_reply_packet(
                    self.election.config.node_id,
                    sender,
                    self.current_term(),
                    success,
                    match_idx,
                )?;
                actions.push(OutboundMessage::SendPacket { to: sender, packet });

                // Check if new entries can be applied
                if self.commit_index.0 > prev_commit.0 {
                    actions.push(OutboundMessage::ApplyEntries {
                        from_index: prev_commit.next(),
                        to_index: self.commit_index,
                    });
                    self.last_applied = self.commit_index;
                }
            }

            MsgType::AppendEntriesReply => {
                if self.role() == Role::Leader {
                    let reply = AppendEntriesReply::read_from_prefix(payload)
                        .map_err(|_| EngineError::InvalidPacket)?
                        .0;

                    if reply.is_success() {
                        self.progress.record_success(sender, reply.match_index);

                        // Evaluate commit index advancement
                        let mut match_buf = [LogIndex::ZERO; 16];
                        let count = self.progress.collect_all_match_indices(
                            self.storage.last_index(),
                            &mut match_buf,
                        );

                        if let Some(new_commit) = evaluate_commit_advancement(
                            &match_buf[..count],
                            self.commit_index,
                            self.current_term(),
                            |idx| self.storage.term_at(idx),
                        ) {
                            let old_commit = self.commit_index;
                            self.commit_index = new_commit;
                            actions.push(OutboundMessage::ApplyEntries {
                                from_index: old_commit.next(),
                                to_index: new_commit,
                            });
                            self.last_applied = new_commit;
                        }
                    } else {
                        self.progress.record_rejection(sender);
                    }
                }
            }

            MsgType::Unknown | MsgType::HeartbeatArgs | MsgType::HeartbeatReply => {}
        }

        Ok(actions)
    }
}
