use crate::message::{RequestVoteArgs, RequestVoteReply};
use crate::types::{LogIndex, NodeId, Role, Term};

/// Compute the majority quorum size required for consensus decisions in a cluster of `cluster_size` nodes.
#[inline(always)]
pub fn quorum_size(cluster_size: usize) -> usize {
    (cluster_size / 2) + 1
}

/// Check if the number of affirmative votes is sufficient to reach quorum.
#[inline(always)]
pub fn is_quorum_reached(votes_granted: usize, cluster_size: usize) -> bool {
    votes_granted >= quorum_size(cluster_size)
}

/// Determine if a candidate's log is at least as up-to-date as the receiver's log (Raft §5.4.1).
#[inline]
pub fn is_log_up_to_date(
    candidate_last_term: Term,
    candidate_last_index: LogIndex,
    local_last_term: Term,
    local_last_index: LogIndex,
) -> bool {
    if candidate_last_term != local_last_term {
        candidate_last_term.0 > local_last_term.0
    } else {
        candidate_last_index.0 >= local_last_index.0
    }
}

/// Determine if the receiver node is eligible to cast a vote for `candidate_id` in `candidate_term`.
#[inline]
pub fn is_vote_eligible(
    current_term: Term,
    voted_for: NodeId,
    candidate_term: Term,
    candidate_id: NodeId,
) -> bool {
    if candidate_term.0 < current_term.0 {
        false
    } else if candidate_term.0 > current_term.0 {
        true
    } else {
        voted_for == NodeId::NONE || voted_for == candidate_id
    }
}

/// Actions emitted by the election subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionAction {
    None,
    Campaign,
    ElectedLeader,
    StepDown(Term),
    HeartbeatTimeout,
}

/// Configuration parameters for the election state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionConfig {
    pub node_id: NodeId,
    pub cluster_size: usize,
    pub election_timeout_ticks: u32,
    pub heartbeat_interval_ticks: u32,
}

/// State tracking for Raft leader elections.
#[derive(Debug, Clone)]
pub struct ElectionState {
    pub config: ElectionConfig,
    pub role: Role,
    pub current_term: Term,
    pub voted_for: NodeId,
    pub votes_granted: u32,
    pub election_elapsed: u32,
    pub heartbeat_elapsed: u32,
}

impl ElectionState {
    /// Create a new election state machine in Follower role.
    pub fn new(config: ElectionConfig) -> Self {
        Self {
            config,
            role: Role::Follower,
            current_term: Term::ZERO,
            voted_for: NodeId::NONE,
            votes_granted: 0,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
        }
    }

    /// Advance logical timer by one tick and evaluate election or heartbeat timeouts.
    pub fn handle_tick(&mut self) -> ElectionAction {
        match self.role {
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.config.election_timeout_ticks {
                    self.start_campaign()
                } else {
                    ElectionAction::None
                }
            }
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_interval_ticks {
                    self.heartbeat_elapsed = 0;
                    ElectionAction::HeartbeatTimeout
                } else {
                    ElectionAction::None
                }
            }
        }
    }

    /// Initiate an election campaign: increment term, vote for self, reset election timer.
    pub fn start_campaign(&mut self) -> ElectionAction {
        self.role = Role::Candidate;
        self.current_term = self.current_term.next();
        self.voted_for = self.config.node_id;
        self.votes_granted = 1; // Self-vote
        self.election_elapsed = 0;
        if is_quorum_reached(self.votes_granted as usize, self.config.cluster_size) {
            self.role = Role::Leader;
            self.heartbeat_elapsed = 0;
            ElectionAction::ElectedLeader
        } else {
            ElectionAction::Campaign
        }
    }

    /// Step down to Follower in response to discovering a higher term or valid leader.
    pub fn step_down_to_follower(&mut self, new_term: Term) {
        self.role = Role::Follower;
        self.current_term = new_term;
        self.voted_for = NodeId::NONE;
        self.votes_granted = 0;
        self.election_elapsed = 0;
    }

    /// Check if peer term is higher than current term; step down if so.
    pub fn check_peer_term(&mut self, peer_term: Term) -> ElectionAction {
        if peer_term.0 > self.current_term.0 {
            self.step_down_to_follower(peer_term);
            ElectionAction::StepDown(peer_term)
        } else {
            ElectionAction::None
        }
    }

    /// Record a received vote reply from a peer.
    pub fn handle_vote_reply(
        &mut self,
        _from: NodeId,
        reply: &RequestVoteReply,
    ) -> ElectionAction {
        if reply.term.0 > self.current_term.0 {
            self.step_down_to_follower(reply.term);
            return ElectionAction::StepDown(reply.term);
        }

        if self.role == Role::Candidate && reply.term == self.current_term && reply.is_granted() {
            self.votes_granted += 1;
            if is_quorum_reached(self.votes_granted as usize, self.config.cluster_size) {
                self.role = Role::Leader;
                self.heartbeat_elapsed = 0;
                return ElectionAction::ElectedLeader;
            }
        }

        ElectionAction::None
    }

    /// Evaluate an inbound `RequestVoteArgs` RPC from a candidate node.
    pub fn handle_request_vote(
        &mut self,
        args: &RequestVoteArgs,
        local_last_term: Term,
        local_last_index: LogIndex,
    ) -> RequestVoteReply {
        // Step down if candidate's term is higher
        if args.term.0 > self.current_term.0 {
            self.step_down_to_follower(args.term);
        }

        let can_vote = is_vote_eligible(
            self.current_term,
            self.voted_for,
            args.term,
            args.candidate_id,
        );
        let log_ok = is_log_up_to_date(
            args.last_log_term,
            args.last_log_index,
            local_last_term,
            local_last_index,
        );

        let vote_granted = if can_vote && log_ok {
            self.voted_for = args.candidate_id;
            self.election_elapsed = 0; // Reset timer upon granting vote
            1
        } else {
            0
        };

        RequestVoteReply {
            term: self.current_term,
            vote_granted,
            _pad: [0; 7],
        }
    }
}
