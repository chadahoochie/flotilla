use flotilla_raft::codec::{
    HEADER_SIZE, MAGIC, PROTOCOL_VERSION, PacketHeader, decode_packet, encode_append_entries,
    encode_append_entries_reply, encode_request_vote_args, encode_request_vote_reply,
    verify_checksum,
};
use flotilla_raft::message::{
    AppendEntriesHeader, AppendEntriesReply, MsgType, RequestVoteArgs, RequestVoteReply,
};
use flotilla_raft::types::{LogIndex, NodeId, Term};
use zerocopy::FromBytes;

#[test]
fn test_packet_header_size_and_alignment() {
    assert_eq!(std::mem::size_of::<PacketHeader>(), HEADER_SIZE);
    assert_eq!(std::mem::size_of::<PacketHeader>(), 40);
    assert_eq!(std::mem::align_of::<PacketHeader>(), 8);
}

#[test]
fn test_request_vote_roundtrip() {
    let args = RequestVoteArgs {
        term: Term(42),
        candidate_id: NodeId(7),
        last_log_index: LogIndex(100),
        last_log_term: Term(41),
    };

    let mut buf = [0u8; 128];
    let written = encode_request_vote_args(&mut buf, NodeId(7), NodeId(2), Term(42), &args)
        .expect("Encoding should succeed");

    assert!(written >= HEADER_SIZE + std::mem::size_of::<RequestVoteArgs>());

    let (header, payload) = decode_packet(&buf[..written]).expect("Decoding should succeed");
    assert_eq!(header.magic, MAGIC);
    assert_eq!(header.version, PROTOCOL_VERSION);
    assert_eq!(header.msg_type, MsgType::RequestVoteArgs as u16);
    assert_eq!(header.sender_id, NodeId(7));
    assert_eq!(header.receiver_id, NodeId(2));
    assert_eq!(header.term, Term(42));
    assert!(verify_checksum(&header, payload));

    let decoded_args = RequestVoteArgs::read_from_prefix(payload)
        .expect("Payload should be valid RequestVoteArgs");
    assert_eq!(decoded_args.0, args);
}

#[test]
fn test_request_vote_reply_roundtrip() {
    let reply = RequestVoteReply {
        term: Term(42),
        vote_granted: 1,
        _pad: [0; 7],
    };

    let mut buf = [0u8; 128];
    let written = encode_request_vote_reply(&mut buf, NodeId(2), NodeId(7), Term(42), &reply)
        .expect("Encoding should succeed");

    let (header, payload) = decode_packet(&buf[..written]).expect("Decoding should succeed");
    assert_eq!(header.msg_type, MsgType::RequestVoteReply as u16);
    assert!(verify_checksum(&header, payload));

    let decoded_reply = RequestVoteReply::read_from_prefix(payload)
        .expect("Payload should be valid RequestVoteReply");
    assert_eq!(decoded_reply.0, reply);
}

#[test]
fn test_append_entries_roundtrip() {
    let header_args = AppendEntriesHeader {
        term: Term(10),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(50),
        prev_log_term: Term(9),
        leader_commit: LogIndex(48),
        entries_count: 2,
        _pad: [0; 4],
    };

    let entries_raw_data = b"entry_payload_bytes_for_raft";
    let mut buf = [0u8; 256];
    let written = encode_append_entries(
        &mut buf,
        NodeId(1),
        NodeId(2),
        Term(10),
        &header_args,
        entries_raw_data,
    )
    .expect("Encode should succeed");

    let (header, payload) = decode_packet(&buf[..written]).expect("Decode should succeed");
    assert_eq!(header.msg_type, MsgType::AppendEntriesArgs as u16);
    assert!(verify_checksum(&header, payload));

    let (decoded_hdr, decoded_data) = AppendEntriesHeader::read_from_prefix(payload)
        .expect("Payload should contain AppendEntriesHeader");
    assert_eq!(decoded_hdr, header_args);
    assert_eq!(decoded_data, entries_raw_data);
}

#[test]
fn test_append_entries_reply_roundtrip() {
    let reply = AppendEntriesReply {
        term: Term(10),
        follower_id: NodeId(2),
        success: 1,
        _pad: [0; 7],
        match_index: LogIndex(52),
    };

    let mut buf = [0u8; 128];
    let written = encode_append_entries_reply(&mut buf, NodeId(2), NodeId(1), Term(10), &reply)
        .expect("Encode should succeed");

    let (header, payload) = decode_packet(&buf[..written]).expect("Decode should succeed");
    assert_eq!(header.msg_type, MsgType::AppendEntriesReply as u16);
    assert!(verify_checksum(&header, payload));

    let decoded_reply = AppendEntriesReply::read_from_prefix(payload)
        .expect("Payload should contain AppendEntriesReply");
    assert_eq!(decoded_reply.0, reply);
}

#[test]
fn test_corrupted_checksum_fails() {
    let args = RequestVoteArgs {
        term: Term(1),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(0),
        last_log_term: Term(0),
    };

    let mut buf = [0u8; 128];
    let written = encode_request_vote_args(&mut buf, NodeId(1), NodeId(2), Term(1), &args).unwrap();

    // Corrupt one byte of payload
    buf[HEADER_SIZE] ^= 0xFF;

    let (header, payload) = decode_packet(&buf[..written]).expect("Header should parse");
    assert!(
        !verify_checksum(&header, payload),
        "Corrupted payload must fail checksum verification"
    );
}

#[test]
fn test_subscriber_commit_frame_roundtrip() {
    let mut buf = [0u8; 128];
    let written = flotilla_raft::codec::encode_subscriber_commit_frame(
        &mut buf,
        NodeId(1),
        Term(2),
        LogIndex(10),
        b"hello_subscriber",
    )
    .expect("Encode subscriber commit frame");

    let (header, payload) = decode_packet(&buf[..written]).expect("Decode packet");
    assert_eq!(header.magic, MAGIC);
    assert_eq!(header.version, PROTOCOL_VERSION);
    assert_eq!(header.msg_type, MsgType::AppendEntriesArgs as u16);
    assert_eq!(header.sender_id, NodeId(1));
    assert_eq!(header.receiver_id, NodeId(0));
    assert_eq!(header.term, Term(2));
    assert!(verify_checksum(&header, payload));

    assert_eq!(payload.len(), 16 + b"hello_subscriber".len());
    let log_idx = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let term_val = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    assert_eq!(log_idx, 10);
    assert_eq!(term_val, 2);
    assert_eq!(&payload[16..], b"hello_subscriber");
}
