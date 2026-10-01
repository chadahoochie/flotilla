use flotilla::archive::{
    ArchivePipeline, ArchivedEntry, FileArchiveSink, NullArchiveSink, PipelineError,
};
use flotilla::codec::{
    CodecError, HEADER_SIZE, MAGIC, PROTOCOL_VERSION, PacketHeader, decode_packet,
    encode_append_entries, encode_append_entries_reply, encode_packet_header,
    encode_request_vote_args, encode_request_vote_reply,
};
use flotilla::commit::evaluate_commit_advancement;
use flotilla::election::{
    ElectionAction, ElectionConfig, ElectionState, is_log_up_to_date, is_quorum_reached,
    is_vote_eligible, quorum_size,
};
use flotilla::engine::{
    EngineError, OutboundMessage, RaftConfig, RaftNode, create_append_entries_packet,
    create_append_entries_reply_packet, create_request_vote_packet,
    create_request_vote_reply_packet,
};
use flotilla::message::{
    AppendEntriesHeader, AppendEntriesReply, MsgType, RequestVoteArgs, RequestVoteReply,
};
use flotilla::replication::{
    FollowerAppendResult, PeerProgressTracker, evaluate_follower_append_entries,
};
use flotilla::storage::{RingBufferLogStorage, StorageError};
use flotilla::types::{LogIndex, NodeId, Role, Term};
use flotilla::udp::framing::{ETHERNET_MTU, IP_UDP_OVERHEAD, fits_in_mtu};
use std::io::Write;
use zerocopy::FromBytes;

#[test]
fn test_archive_pipeline_full_and_empty_drain() {
    let sink = NullArchiveSink::new();
    let mut pipeline = ArchivePipeline::new(sink, 2);

    let entries = vec![
        ArchivedEntry {
            index: LogIndex(1),
            term: Term(1),
            payload: vec![1, 2, 3],
        },
        ArchivedEntry {
            index: LogIndex(2),
            term: Term(1),
            payload: vec![4, 5, 6],
        },
    ];

    assert!(pipeline.enqueue_batch(&entries).is_ok());

    // Queue full error branch
    let overflow = vec![ArchivedEntry {
        index: LogIndex(3),
        term: Term(1),
        payload: vec![7],
    }];
    let err = pipeline.enqueue_batch(&overflow).unwrap_err();
    assert_eq!(err, PipelineError::QueueFull);
    assert_eq!(
        format!("{err}"),
        "archive pipeline queue is at maximum capacity"
    );

    // Drain and advance watermark
    let count = pipeline.drain_and_persist().unwrap();
    assert_eq!(count, 2);
    assert_eq!(pipeline.persisted_watermark(), LogIndex(2));

    // Drain on empty queue branch
    let empty_count = pipeline.drain_and_persist().unwrap();
    assert_eq!(empty_count, 0);

    // Flush
    assert!(pipeline.flush().is_ok());
}

#[test]
fn test_file_archive_sink_open_append_and_crc_mismatch() {
    let dir = std::env::temp_dir().join("flotilla_test_wal_coverage");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("test_append.wal");

    {
        let mut sink = FileArchiveSink::create(&wal_path).unwrap();
        use flotilla::archive::AsyncArchiveSink;
        sink.write_entries(&[ArchivedEntry {
            index: LogIndex(1),
            term: Term(1),
            payload: b"first".to_vec(),
        }])
        .unwrap();
        sink.flush().unwrap();
    }

    // open_append branch
    {
        let mut sink = FileArchiveSink::open_append(&wal_path).unwrap();
        use flotilla::archive::AsyncArchiveSink;
        sink.write_entries(&[ArchivedEntry {
            index: LogIndex(2),
            term: Term(1),
            payload: b"second".to_vec(),
        }])
        .unwrap();
        sink.flush().unwrap();
    }

    let entries = FileArchiveSink::read_all(&wal_path).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].index, LogIndex(1));
    assert_eq!(entries[1].index, LogIndex(2));

    // Test CRC mismatch branch
    let corrupt_path = dir.join("corrupt.wal");
    std::fs::copy(&wal_path, &corrupt_path).unwrap();
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&corrupt_path)
            .unwrap();
        // Corrupt a byte in the payload
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(22)).unwrap();
        file.write_all(&[0xFF]).unwrap();
    }
    let res = FileArchiveSink::read_all(&corrupt_path);
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);

    // Test truncated WAL file (returns UnexpectedEof)
    let trunc_path = dir.join("trunc.wal");
    {
        let mut file = std::fs::File::create(&trunc_path).unwrap();
        // Write 20 bytes meta indicating 100 bytes payload, but write nothing else
        let mut meta = [0u8; 20];
        meta[16..20].copy_from_slice(&(100u32).to_le_bytes());
        file.write_all(&meta).unwrap();
    }
    let trunc_res = FileArchiveSink::read_all(&trunc_path);
    assert!(trunc_res.is_err());
    assert_eq!(
        trunc_res.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_codec_error_branches_and_buffer_too_small() {
    let buf = [0u8; 128];
    let header = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: MsgType::RequestVoteArgs.to_u16(),
        sender_id: NodeId(1),
        receiver_id: NodeId(2),
        term: Term(1),
        checksum: 0,
        payload_len: 10,
    };

    // Buffer too small for decode
    assert!(matches!(
        decode_packet(&buf[..10]),
        Err(CodecError::BufferTooSmall)
    ));

    // Buffer too small for encode_packet_header
    let mut small_buf = [0u8; 10];
    assert!(matches!(
        encode_packet_header(&mut small_buf, &header),
        Err(CodecError::BufferTooSmall)
    ));

    // Buffer too small for encode_request_vote_args
    let rv_args = RequestVoteArgs {
        term: Term(1),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(1),
        last_log_term: Term(1),
    };
    assert!(matches!(
        encode_request_vote_args(&mut small_buf, NodeId(1), NodeId(2), Term(1), &rv_args),
        Err(CodecError::BufferTooSmall)
    ));

    // Buffer too small for encode_request_vote_reply
    let rv_reply = RequestVoteReply {
        term: Term(1),
        vote_granted: 1,
        _pad: [0; 7],
    };
    assert!(matches!(
        encode_request_vote_reply(&mut small_buf, NodeId(1), NodeId(2), Term(1), &rv_reply),
        Err(CodecError::BufferTooSmall)
    ));

    // Buffer too small for encode_append_entries_reply
    let ae_reply = AppendEntriesReply {
        term: Term(1),
        follower_id: NodeId(1),
        success: 1,
        _pad: [0; 7],
        match_index: LogIndex(1),
    };
    assert!(matches!(
        encode_append_entries_reply(&mut small_buf, NodeId(1), NodeId(2), Term(1), &ae_reply),
        Err(CodecError::BufferTooSmall)
    ));

    // Buffer too small for encode_append_entries
    let ae_hdr = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
        leader_commit: LogIndex(0),
        entries_count: 0,
        _pad: [0; 4],
    };
    assert!(matches!(
        encode_append_entries(
            &mut small_buf,
            NodeId(1),
            NodeId(2),
            Term(1),
            &ae_hdr,
            b"entry"
        ),
        Err(CodecError::BufferTooSmall)
    ));

    // Invalid Magic
    let mut bad_magic_buf = [0u8; 64];
    let mut bad_header = header;
    bad_header.magic = 0xDEADBEEF;
    encode_packet_header(&mut bad_magic_buf, &bad_header).unwrap();
    assert!(matches!(
        decode_packet(&bad_magic_buf),
        Err(CodecError::InvalidMagic(0xDEADBEEF))
    ));

    // Unsupported Version
    let mut bad_ver_buf = [0u8; 64];
    let mut bad_ver_header = header;
    bad_ver_header.version = 99;
    encode_packet_header(&mut bad_ver_buf, &bad_ver_header).unwrap();
    assert!(matches!(
        decode_packet(&bad_ver_buf),
        Err(CodecError::UnsupportedVersion(99))
    ));

    // Payload Length Mismatch
    let mut mismatch_buf = [0u8; 64];
    let mut mismatch_header = header;
    mismatch_header.payload_len = 100;
    encode_packet_header(&mut mismatch_buf, &mismatch_header).unwrap();
    assert!(matches!(
        decode_packet(&mismatch_buf),
        Err(CodecError::PayloadLengthMismatch {
            expected: 100,
            actual: _
        })
    ));

    // Display for all CodecError variants
    assert!(format!("{}", CodecError::BufferTooSmall).contains("buffer is too small"));
    assert!(format!("{}", CodecError::InvalidMagic(0x123)).contains("invalid magic"));
    assert!(
        format!("{}", CodecError::UnsupportedVersion(2)).contains("unsupported protocol version")
    );
    assert!(
        format!(
            "{}",
            CodecError::PayloadLengthMismatch {
                expected: 10,
                actual: 5
            }
        )
        .contains("payload length mismatch")
    );
    assert!(
        format!(
            "{}",
            CodecError::ChecksumMismatch {
                header_crc: 1,
                computed_crc: 2
            }
        )
        .contains("checksum mismatch")
    );
    assert!(format!("{}", CodecError::InvalidMessageType(99)).contains("invalid message type"));
    assert!(format!("{}", CodecError::SerializationError).contains("failed to serialize"));
}

#[test]
fn test_commit_advancement_edge_cases() {
    // Empty match_indices
    assert_eq!(
        evaluate_commit_advancement(&[], LogIndex(0), Term(1), |_| Some(Term(1))),
        None
    );

    // Large slice > 16 nodes
    let huge = [LogIndex(10); 17];
    assert_eq!(
        evaluate_commit_advancement(&huge, LogIndex(0), Term(1), |_| Some(Term(1))),
        None
    );

    // Candidate commit <= current commit
    let indices = [LogIndex(5), LogIndex(5), LogIndex(5)];
    assert_eq!(
        evaluate_commit_advancement(&indices, LogIndex(5), Term(1), |_| Some(Term(1))),
        None
    );
    assert_eq!(
        evaluate_commit_advancement(&indices, LogIndex(6), Term(1), |_| Some(Term(1))),
        None
    );

    // Candidate commit term mismatch (term != current_term)
    assert_eq!(
        evaluate_commit_advancement(&indices, LogIndex(0), Term(2), |_| Some(Term(1))),
        None
    );

    // Term lookup returns None
    assert_eq!(
        evaluate_commit_advancement(&indices, LogIndex(0), Term(1), |_| None),
        None
    );
}

#[test]
fn test_election_rules_and_state_branches() {
    // quorum_size and is_quorum_reached
    assert_eq!(quorum_size(1), 1);
    assert_eq!(quorum_size(3), 2);
    assert_eq!(quorum_size(5), 3);
    assert!(is_quorum_reached(2, 3));
    assert!(!is_quorum_reached(1, 3));

    // is_vote_eligible
    // candidate_term < current_term -> false
    assert!(!is_vote_eligible(Term(2), NodeId::NONE, Term(1), NodeId(2)));
    // candidate_term > current_term -> true
    assert!(is_vote_eligible(Term(1), NodeId(3), Term(2), NodeId(2)));
    // candidate_term == current_term, voted_for == NONE -> true
    assert!(is_vote_eligible(Term(1), NodeId::NONE, Term(1), NodeId(2)));
    // candidate_term == current_term, voted_for == candidate -> true
    assert!(is_vote_eligible(Term(1), NodeId(2), Term(1), NodeId(2)));
    // candidate_term == current_term, voted_for != candidate -> false
    assert!(!is_vote_eligible(Term(1), NodeId(3), Term(1), NodeId(2)));

    // is_log_up_to_date
    // candidate_term > local_term -> true
    assert!(is_log_up_to_date(
        Term(2),
        LogIndex(1),
        Term(1),
        LogIndex(10)
    ));
    // candidate_term < local_term -> false
    assert!(!is_log_up_to_date(
        Term(1),
        LogIndex(10),
        Term(2),
        LogIndex(1)
    ));
    // candidate_term == local_term, candidate_index >= local_index -> true
    assert!(is_log_up_to_date(
        Term(1),
        LogIndex(10),
        Term(1),
        LogIndex(10)
    ));
    assert!(is_log_up_to_date(
        Term(1),
        LogIndex(11),
        Term(1),
        LogIndex(10)
    ));
    // candidate_term == local_term, candidate_index < local_index -> false
    assert!(!is_log_up_to_date(
        Term(1),
        LogIndex(9),
        Term(1),
        LogIndex(10)
    ));

    // ElectionState branches
    let config = ElectionConfig {
        node_id: NodeId(1),
        cluster_size: 3,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
    };
    let mut state = ElectionState::new(config);

    // Leader heartbeat timer ticks without expiring
    state.role = Role::Leader;
    assert_eq!(state.handle_tick(), ElectionAction::None);
    assert_eq!(state.handle_tick(), ElectionAction::HeartbeatTimeout);

    // check_peer_term with lower term
    assert_eq!(state.check_peer_term(Term::ZERO), ElectionAction::None);

    // handle_vote_reply with higher term causes step down
    let high_reply = RequestVoteReply {
        term: Term(10),
        vote_granted: 0,
        _pad: [0; 7],
    };
    assert_eq!(
        state.handle_vote_reply(NodeId(2), &high_reply),
        ElectionAction::StepDown(Term(10))
    );

    // handle_vote_reply not granted or when not candidate
    state.current_term = Term(10);
    state.role = Role::Candidate;
    let not_granted = RequestVoteReply {
        term: Term(10),
        vote_granted: 0,
        _pad: [0; 7],
    };
    assert_eq!(
        state.handle_vote_reply(NodeId(2), &not_granted),
        ElectionAction::None
    );

    // handle_vote_reply stale term
    let stale_reply = RequestVoteReply {
        term: Term(9),
        vote_granted: 1,
        _pad: [0; 7],
    };
    assert_eq!(
        state.handle_vote_reply(NodeId(2), &stale_reply),
        ElectionAction::None
    );
}

#[test]
fn test_packets_and_engine_error_display() {
    // create_request_vote_reply_packet with false
    let pkt_vote_false =
        create_request_vote_reply_packet(NodeId(1), NodeId(2), Term(1), false).unwrap();
    let (_, p_vote) = decode_packet(&pkt_vote_false).unwrap();
    use zerocopy::FromBytes;
    let reply = RequestVoteReply::read_from_prefix(p_vote).unwrap().0;
    assert!(!reply.is_granted());

    // create_append_entries_reply_packet with false
    let pkt_ae_false =
        create_append_entries_reply_packet(NodeId(1), NodeId(2), Term(1), false, LogIndex(0))
            .unwrap();
    let (_, p_ae) = decode_packet(&pkt_ae_false).unwrap();
    let ae_reply = AppendEntriesReply::read_from_prefix(p_ae).unwrap().0;
    assert!(!ae_reply.is_success());

    // EngineError Display & conversions
    let err_not_leader = EngineError::NotLeader;
    assert_eq!(
        format!("{err_not_leader}"),
        "current node is not the elected leader"
    );

    let err_storage: EngineError = StorageError::BufferFull.into();
    assert!(format!("{err_storage}").contains("storage engine error"));

    let err_codec: EngineError = CodecError::BufferTooSmall.into();
    assert!(format!("{err_codec}").contains("codec serialization error"));

    let err_invalid = EngineError::InvalidPacket;
    assert_eq!(
        format!("{err_invalid}"),
        "inbound packet failed decoding or verification"
    );

    // StorageError Display
    assert!(format!("{}", StorageError::BufferFull).contains("ring buffer storage is full"));
    assert!(
        format!(
            "{}",
            StorageError::PayloadTooLarge {
                max: 10,
                actual: 20
            }
        )
        .contains("entry payload too large")
    );
    assert!(
        format!("{}", StorageError::IndexOutOfBounds { index: LogIndex(5) })
            .contains("out of retained bounds")
    );
    assert!(
        format!(
            "{}",
            StorageError::CompactionIndexTooHigh {
                watermark: LogIndex(10),
                last: LogIndex(5)
            }
        )
        .contains("cannot exceed last log index")
    );
}

#[test]
fn test_raft_node_step_edge_cases_and_propose_not_leader() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(2), NodeId(3)],
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
    };
    let mut node: RaftNode<128, 128> = RaftNode::new(config);

    // propose when not leader
    assert_eq!(node.propose(b"cmd"), Err(EngineError::NotLeader));
    assert_eq!(node.commit_index(), LogIndex::ZERO);
    assert_eq!(node.last_applied(), LogIndex::ZERO);
    assert_eq!(node.last_log_term(), Term::ZERO);

    // step with invalid checksum
    let mut valid_pkt =
        create_request_vote_packet(NodeId(2), NodeId(1), Term(1), LogIndex(0), Term(0)).unwrap();
    valid_pkt[HEADER_SIZE] ^= 0xFF; // corrupt payload
    assert_eq!(
        node.step(NodeId(2), &valid_pkt),
        Err(EngineError::InvalidPacket)
    );

    // Stale AppendEntriesArgs (term < current_term)
    node.election.current_term = Term(5);
    let stale_hdr = AppendEntriesHeader {
        term: Term(4),
        leader_id: NodeId(2),
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
        leader_commit: LogIndex(0),
        entries_count: 0,
        _pad: [0; 4],
    };
    let stale_pkt = create_append_entries_packet(NodeId(1), &stale_hdr, &[]).unwrap();
    let actions = node.step(NodeId(2), &stale_pkt).unwrap();
    assert_eq!(actions.len(), 1);
    if let OutboundMessage::SendPacket { packet, .. } = &actions[0] {
        let (_, p) = decode_packet(packet).unwrap();
        let reply = AppendEntriesReply::read_from_prefix(p).unwrap().0;
        assert!(!reply.is_success());
    } else {
        panic!("expected SendPacket");
    }

    // Candidate reverts to Follower on valid AppendEntries
    node.election.role = Role::Candidate;
    let valid_hdr = AppendEntriesHeader {
        term: Term(5),
        leader_id: NodeId(2),
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
        leader_commit: LogIndex(0),
        entries_count: 0,
        _pad: [0; 4],
    };
    let valid_ae_pkt = create_append_entries_packet(NodeId(1), &valid_hdr, &[]).unwrap();
    let _ = node.step(NodeId(2), &valid_ae_pkt).unwrap();
    assert_eq!(node.role(), Role::Follower);

    // Leader receives failed AppendEntriesReply and records rejection
    node.election.role = Role::Leader;
    let reject_reply_pkt =
        create_append_entries_reply_packet(NodeId(2), NodeId(1), Term(5), false, LogIndex(0))
            .unwrap();
    let actions = node.step(NodeId(2), &reject_reply_pkt).unwrap();
    assert!(actions.is_empty());

    // Follower receives AppendEntries with leader_commit > commit_index -> ApplyEntries
    node.election.role = Role::Follower;
    let _ = node.storage.append_entry(Term(5), b"val").unwrap();
    let commit_hdr = AppendEntriesHeader {
        term: Term(5),
        leader_id: NodeId(2),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(5),
        leader_commit: LogIndex(1),
        entries_count: 0,
        _pad: [0; 4],
    };
    let commit_pkt = create_append_entries_packet(NodeId(1), &commit_hdr, &[]).unwrap();
    let actions = node.step(NodeId(2), &commit_pkt).unwrap();
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, OutboundMessage::ApplyEntries { .. }))
    );

    // Compact watermark
    assert!(node.compact_watermark(LogIndex(1)).is_ok());
}

#[test]
fn test_replication_evaluator_and_peer_progress_all_branches() {
    let mut storage: RingBufferLogStorage<16, 64> = RingBufferLogStorage::new();
    let mut commit = LogIndex::ZERO;

    // prev_log_index > 0 but storage last_index < prev_log_index -> Rejected
    let hdr_gap = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(5),
        prev_log_term: Term(1),
        leader_commit: LogIndex(0),
        entries_count: 0,
        _pad: [0; 4],
    };
    assert_eq!(
        evaluate_follower_append_entries(&hdr_gap, &[], &mut storage, &mut commit),
        FollowerAppendResult::Rejected
    );

    // prev_log_index > 0 but term mismatch -> Rejected
    storage.append_entry(Term(1), b"entry1").unwrap();
    let hdr_term_mismatch = AppendEntriesHeader {
        term: Term(2),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(2), // mismatch (actual is 1)
        leader_commit: LogIndex(0),
        entries_count: 0,
        _pad: [0; 4],
    };
    assert_eq!(
        evaluate_follower_append_entries(&hdr_term_mismatch, &[], &mut storage, &mut commit),
        FollowerAppendResult::Rejected
    );

    // Conflict truncation: storage has index 2, new entries overwrite index 2
    storage.append_entry(Term(1), b"entry2").unwrap();
    let hdr_conflict = AppendEntriesHeader {
        term: Term(2),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(1),
        leader_commit: LogIndex(2),
        entries_count: 1,
        _pad: [0; 4],
    };
    assert_eq!(
        evaluate_follower_append_entries(&hdr_conflict, b"new2", &mut storage, &mut commit),
        FollowerAppendResult::Success {
            match_index: LogIndex(2)
        }
    );
    assert_eq!(commit, LogIndex(2));

    // PeerProgressTracker branches
    let mut tracker = PeerProgressTracker::new(&[NodeId(2), NodeId(3)], LogIndex(0));
    // get_match_index & get_next_index for unknown peer
    assert_eq!(tracker.get_match_index(NodeId(99)), LogIndex::ZERO);
    assert_eq!(tracker.get_next_index(NodeId(99)), LogIndex(1));

    // record_success & rejection for unknown peer
    tracker.record_success(NodeId(99), LogIndex(10));
    assert_eq!(tracker.record_rejection(NodeId(99)), LogIndex(1));

    // record_rejection when next_index == 1
    assert_eq!(tracker.get_next_index(NodeId(2)), LogIndex(1));
    assert_eq!(tracker.record_rejection(NodeId(2)), LogIndex(1));

    // collect_all_match_indices with empty dest
    assert_eq!(tracker.collect_all_match_indices(LogIndex(5), &mut []), 0);

    // collect_all_match_indices with dest smaller than peers
    let mut small_dest = [LogIndex::ZERO; 2];
    let count = tracker.collect_all_match_indices(LogIndex(5), &mut small_dest);
    assert_eq!(count, 2);
    assert_eq!(small_dest[0], LogIndex(5));
}

#[test]
fn test_ring_buffer_storage_all_branches() {
    let mut storage: RingBufferLogStorage<4, 16> = RingBufferLogStorage::default();
    assert!(storage.is_empty());
    assert_eq!(storage.len(), 0);
    assert_eq!(storage.first_index(), LogIndex(1));
    assert_eq!(storage.last_index(), LogIndex(0));

    // Payload too large
    let huge_payload = [0u8; 32];
    assert!(matches!(
        storage.append_entry(Term(1), &huge_payload),
        Err(StorageError::PayloadTooLarge {
            max: 16,
            actual: 32
        })
    ));

    // Fill buffer to capacity (4 entries)
    for i in 1..=4 {
        storage.append_entry(Term(1), &[i]).unwrap();
    }
    assert_eq!(storage.len(), 4);
    assert!(!storage.is_empty());

    // Buffer full error
    assert!(matches!(
        storage.append_entry(Term(1), &[5]),
        Err(StorageError::BufferFull)
    ));

    // term_at & entry_at out of bounds
    assert_eq!(storage.term_at(LogIndex(0)), None);
    assert_eq!(storage.term_at(LogIndex(5)), None);
    assert!(storage.entry_at(LogIndex(0)).is_none());
    assert!(storage.entry_at(LogIndex(5)).is_none());

    // Truncate suffix: first < from_index <= last
    storage.truncate_suffix(LogIndex(3));
    assert_eq!(storage.last_index(), LogIndex(2));
    assert_eq!(storage.len(), 2);

    // Re-fill to 4
    storage.append_entry(Term(1), &[3]).unwrap();
    storage.append_entry(Term(1), &[4]).unwrap();

    // Compaction watermark > last
    assert!(matches!(
        storage.compact_prefix(LogIndex(10)),
        Err(StorageError::CompactionIndexTooHigh {
            watermark: LogIndex(10),
            last: LogIndex(4)
        })
    ));

    // Compact up to 2
    assert!(storage.compact_prefix(LogIndex(2)).is_ok());
    assert_eq!(storage.first_index(), LogIndex(3));

    // Compact already compacted index (< first)
    assert!(storage.compact_prefix(LogIndex(1)).is_ok());
    assert_eq!(storage.first_index(), LogIndex(3));

    // Truncate suffix: index <= first
    storage.truncate_suffix(LogIndex(2));
    assert_eq!(storage.last_index(), LogIndex(2));

    // Truncate suffix: index > last
    storage.truncate_suffix(LogIndex(10));
}

#[test]
fn test_types_and_framing() {
    // LogIndex
    let l = LogIndex::new(42);
    assert_eq!(l.get(), 42);
    assert_eq!(l.next(), LogIndex(43));

    // Term
    let t = Term::new(10);
    assert_eq!(t.get(), 10);
    assert_eq!(t.next(), Term(11));

    // NodeId
    let n = NodeId::new(7);
    assert_eq!(n.get(), 7);

    // Framing
    assert_eq!(ETHERNET_MTU, 1500);
    assert_eq!(IP_UDP_OVERHEAD, 28);
    assert!(fits_in_mtu(1000));
    assert!(!fits_in_mtu(2000));

    // MsgType
    assert_eq!(MsgType::from_u16(0), None);
    assert_eq!(MsgType::from_u16(1), Some(MsgType::RequestVoteArgs));
    assert_eq!(MsgType::from_u16(2), Some(MsgType::RequestVoteReply));
    assert_eq!(MsgType::from_u16(3), Some(MsgType::AppendEntriesArgs));
    assert_eq!(MsgType::from_u16(4), Some(MsgType::AppendEntriesReply));
    assert_eq!(MsgType::from_u16(5), Some(MsgType::HeartbeatArgs));
    assert_eq!(MsgType::from_u16(6), Some(MsgType::HeartbeatReply));
    assert_eq!(MsgType::from_u16(7), None);
    assert_eq!(MsgType::RequestVoteArgs.to_u16(), 1);
}

#[test]
fn test_raft_node_detailed_branches() {
    let config = RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)],
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
    };
    let mut node: RaftNode<128, 128> = RaftNode::new(config);

    // 1. last_log_term on non-empty log (covers last_idx.0 > 0)
    node.storage.append_entry(Term(3), b"first").unwrap();
    assert_eq!(node.last_log_term(), Term(3));

    // 2. broadcast_heartbeats when leader has log entries and prev_idx > 0
    node.election.role = Role::Leader;
    node.election.current_term = Term(3);
    // advance peer 2 next index so prev_idx > 0
    node.progress.record_success(NodeId(2), LogIndex(1));
    let msgs = node.broadcast_heartbeats();
    assert!(!msgs.is_empty());

    // 3. Step RequestVoteReply when candidate receives vote but NOT quorum yet
    node.election.role = Role::Candidate;
    node.election.current_term = Term(4);
    node.election.votes_granted = 1; // self vote
    let _vote_reply = RequestVoteReply {
        term: Term(4),
        vote_granted: 1,
        _pad: [0; 7],
    };
    let vote_pkt = create_request_vote_reply_packet(NodeId(2), NodeId(1), Term(4), true).unwrap();
    let actions = node.step(NodeId(2), &vote_pkt).unwrap();
    assert!(actions.is_empty()); // 2 votes out of 5 is not quorum (needs 3)
    assert_eq!(node.role(), Role::Candidate);

    // 4. Step AppendEntriesArgs when Follower receives it (role != Candidate)
    node.election.role = Role::Follower;
    let ae_hdr = AppendEntriesHeader {
        term: Term(4),
        leader_id: NodeId(2),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(3),
        leader_commit: LogIndex(1),
        entries_count: 0,
        _pad: [0; 4],
    };
    let ae_pkt = create_append_entries_packet(NodeId(1), &ae_hdr, &[]).unwrap();
    // commit_index will advance to 1 here
    let _actions = node.step(NodeId(2), &ae_pkt).unwrap();
    assert_eq!(node.commit_index(), LogIndex(1));

    // 5. Step AppendEntriesArgs without commit advancement (commit_index <= prev_commit)
    let ae_pkt2 = create_append_entries_packet(NodeId(1), &ae_hdr, &[]).unwrap();
    let actions2 = node.step(NodeId(2), &ae_pkt2).unwrap();
    // Does not produce ApplyEntries because commit_index is already 1
    assert!(
        !actions2
            .iter()
            .any(|a| matches!(a, OutboundMessage::ApplyEntries { .. }))
    );

    // 6. Step AppendEntriesReply when Follower receives it (role != Leader)
    let ae_reply_pkt =
        create_append_entries_reply_packet(NodeId(2), NodeId(1), Term(4), true, LogIndex(1))
            .unwrap();
    let actions3 = node.step(NodeId(2), &ae_reply_pkt).unwrap();
    assert!(actions3.is_empty());

    // 7. Step AppendEntriesReply when Leader receives success reply, but commit advancement is None
    node.election.role = Role::Leader;
    let _ = node.storage.append_entry(Term(4), b"new_entry").unwrap(); // index 2
    // Peer 2 acknowledges index 2, but peers 3,4,5 are at 0 (only 2 nodes have index 2 out of 5, quorum needs 3)
    let ae_reply_pkt2 =
        create_append_entries_reply_packet(NodeId(2), NodeId(1), Term(4), true, LogIndex(2))
            .unwrap();
    let actions4 = node.step(NodeId(2), &ae_reply_pkt2).unwrap();
    // Does not advance commit index
    assert!(
        !actions4
            .iter()
            .any(|a| matches!(a, OutboundMessage::ApplyEntries { .. }))
    );

    // 8. Higher term causes leader to step down to follower
    node.election.role = Role::Leader;
    node.election.current_term = Term(4);
    let higher_term_hdr = AppendEntriesHeader {
        term: Term(10),
        leader_id: NodeId(2),
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
        leader_commit: LogIndex(0),
        entries_count: 0,
        _pad: [0; 4],
    };
    let higher_term_pkt = create_append_entries_packet(NodeId(1), &higher_term_hdr, &[]).unwrap();
    let _ = node.step(NodeId(2), &higher_term_pkt).unwrap();
    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term(10));

    // 9. Unknown msg_type packet returns InvalidPacket
    let mut bad_type_buf = [0u8; 64];
    let bad_type_hdr = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: 999,
        sender_id: NodeId(2),
        receiver_id: NodeId(1),
        term: Term(10),
        checksum: 0,
        payload_len: 0,
    };
    encode_packet_header(&mut bad_type_buf, &bad_type_hdr).unwrap();
    assert_eq!(
        node.step(NodeId(2), &bad_type_buf),
        Err(EngineError::InvalidPacket)
    );

    // 10. HeartbeatArgs msg_type packet is ignored
    let mut hb_buf = [0u8; 64];
    let hb_hdr = PacketHeader {
        magic: MAGIC,
        version: PROTOCOL_VERSION,
        msg_type: MsgType::HeartbeatArgs.to_u16(),
        sender_id: NodeId(2),
        receiver_id: NodeId(1),
        term: Term(10),
        checksum: 0,
        payload_len: 0,
    };
    encode_packet_header(&mut hb_buf, &hb_hdr).unwrap();
    let hb_actions = node.step(NodeId(2), &hb_buf).unwrap();
    assert!(hb_actions.is_empty());

    // 11. Lagging follower whose next_index was compacted away (storage.entry_at returns None)
    let mut lagging_node: RaftNode<128, 128> = RaftNode::new(RaftConfig {
        node_id: NodeId(1),
        peers: vec![NodeId(2)],
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
    });
    lagging_node.election.role = Role::Leader;
    for i in 1..=4 {
        lagging_node.storage.append_entry(Term(1), &[i]).unwrap();
    }
    lagging_node.storage.compact_prefix(LogIndex(2)).unwrap(); // first is now 3
    lagging_node.progress.peers[0].next_index = LogIndex(2); // next_index <= last_index (4), but entry_at(2) is None!
    let msgs = lagging_node.broadcast_heartbeats();
    assert_eq!(msgs.len(), 1);
}

#[test]
fn test_replication_evaluator_remaining_branches() {
    let mut storage: RingBufferLogStorage<16, 64> = RingBufferLogStorage::new();
    let mut commit = LogIndex(1);
    storage.append_entry(Term(1), b"entry1").unwrap();
    storage.append_entry(Term(1), b"entry2").unwrap();
    storage.append_entry(Term(1), b"entry3").unwrap();

    // 1. header.entries_count > 0 but entries_raw is empty
    let hdr_empty_entries = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(3),
        prev_log_term: Term(1),
        leader_commit: LogIndex(1),
        entries_count: 1,
        _pad: [0; 4],
    };
    let res = evaluate_follower_append_entries(&hdr_empty_entries, &[], &mut storage, &mut commit);
    assert!(matches!(res, FollowerAppendResult::Success { .. }));

    // 2. leader_commit < storage.last_index (min_commit is leader_commit)
    let hdr_min_commit = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(3),
        prev_log_term: Term(1),
        leader_commit: LogIndex(2), // < last_index (3)
        entries_count: 0,
        _pad: [0; 4],
    };
    let res = evaluate_follower_append_entries(&hdr_min_commit, &[], &mut storage, &mut commit);
    assert!(matches!(res, FollowerAppendResult::Success { .. }));
    assert_eq!(commit, LogIndex(2));

    // 3. min_commit <= current_commit (commit does not advance)
    let hdr_no_commit = AppendEntriesHeader {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(3),
        prev_log_term: Term(1),
        leader_commit: LogIndex(2), // equal to commit (2)
        entries_count: 0,
        _pad: [0; 4],
    };
    let res = evaluate_follower_append_entries(&hdr_no_commit, &[], &mut storage, &mut commit);
    assert!(matches!(res, FollowerAppendResult::Success { .. }));
    assert_eq!(commit, LogIndex(2));
}
