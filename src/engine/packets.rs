use crate::codec::{
    encode_append_entries, encode_append_entries_reply, encode_request_vote_args,
    encode_request_vote_reply, CodecError, HEADER_SIZE,
};
use crate::message::{
    AppendEntriesHeader, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use crate::types::{LogIndex, NodeId, Term};

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
