//! Consensus RPC message models and payloads for Flotilla Raft.

pub mod append_entries_header;
pub mod append_entries_reply;
pub mod client_proposal_reply;
pub mod msg_type;
pub mod raft_message;
pub mod request_vote_args;
pub mod request_vote_reply;

pub use append_entries_header::AppendEntriesHeader;
pub use append_entries_reply::AppendEntriesReply;
pub use client_proposal_reply::ClientProposalReply;
pub use msg_type::MsgType;
pub use raft_message::RaftMessage;
pub use request_vote_args::RequestVoteArgs;
pub use request_vote_reply::RequestVoteReply;
