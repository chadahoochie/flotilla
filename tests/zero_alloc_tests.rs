use flotilla::codec::{decode_packet, encode_request_vote_args, verify_checksum};
use flotilla::commit::evaluate_commit_advancement;
use flotilla::election::{ElectionConfig, ElectionState};
use flotilla::message::RequestVoteArgs;
use flotilla::storage::ring_buffer::RingBufferLogStorage;
use flotilla::types::{LogIndex, NodeId, Term};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct TrackingAllocator;

static TRACK_ALLOC: AtomicBool = AtomicBool::new(false);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK_ALLOC.load(Ordering::SeqCst) {
            ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
        }
        // SAFETY: Delegating to the system allocator with identical layout
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Delegating deallocation with identical pointer and layout
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

#[test]
fn test_all_consensus_hot_paths_zero_allocations() {
    // 1. Verify Storage Engine Hot Path
    let mut storage = RingBufferLogStorage::<1024, 64>::new();

    ALLOC_COUNT.store(0, Ordering::SeqCst);
    ALLOC_BYTES.store(0, Ordering::SeqCst);
    TRACK_ALLOC.store(true, Ordering::SeqCst);

    let payload = b"steady_state_command_payload";
    for i in 1..=500 {
        let idx = storage.append_entry(Term(1), payload).unwrap();
        assert_eq!(idx, LogIndex(i));
        assert!(storage.entry_at(idx).is_some());
        assert!(storage.term_at(idx).is_some());
    }

    storage.compact_prefix(LogIndex(250)).unwrap();
    storage.truncate_suffix(LogIndex(450));

    TRACK_ALLOC.store(false, Ordering::SeqCst);
    let storage_allocs = ALLOC_COUNT.load(Ordering::SeqCst);
    let storage_bytes = ALLOC_BYTES.load(Ordering::SeqCst);
    assert_eq!(
        storage_allocs, 0,
        "Storage hot path must have 0 heap allocations, got {storage_allocs} ({storage_bytes} bytes)"
    );

    // 2. Verify Codec Hot Path
    let args = RequestVoteArgs {
        term: Term(10),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(50),
        last_log_term: Term(9),
    };
    let mut buf = [0u8; 128];

    ALLOC_COUNT.store(0, Ordering::SeqCst);
    ALLOC_BYTES.store(0, Ordering::SeqCst);
    TRACK_ALLOC.store(true, Ordering::SeqCst);

    for _ in 0..1000 {
        let written =
            encode_request_vote_args(&mut buf, NodeId(1), NodeId(2), Term(10), &args).unwrap();

        let (header, payload) = decode_packet(&buf[..written]).unwrap();
        assert!(verify_checksum(&header, payload));
    }

    TRACK_ALLOC.store(false, Ordering::SeqCst);
    let codec_allocs = ALLOC_COUNT.load(Ordering::SeqCst);
    assert_eq!(
        codec_allocs, 0,
        "Codec hot path must have 0 heap allocations, got {codec_allocs}"
    );

    // 3. Verify Election & Commit Hot Path
    let config = ElectionConfig {
        node_id: NodeId(1),
        cluster_size: 3,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
    };
    let mut election = ElectionState::new(config);

    ALLOC_COUNT.store(0, Ordering::SeqCst);
    ALLOC_BYTES.store(0, Ordering::SeqCst);
    TRACK_ALLOC.store(true, Ordering::SeqCst);

    for _ in 0..100 {
        election.handle_tick();
    }

    let match_indices = [LogIndex(10), LogIndex(10), LogIndex(8)];
    for _ in 0..100 {
        let _ =
            evaluate_commit_advancement(&match_indices, LogIndex(5), Term(1), |_| Some(Term(1)));
    }

    TRACK_ALLOC.store(false, Ordering::SeqCst);
    let election_allocs = ALLOC_COUNT.load(Ordering::SeqCst);
    assert_eq!(
        election_allocs, 0,
        "Election & Commit hot path must have 0 heap allocations, got {election_allocs}"
    );
}
