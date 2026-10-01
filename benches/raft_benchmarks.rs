use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use flotilla::codec::{decode_packet, encode_request_vote_args, verify_checksum};
use flotilla::commit::evaluate_commit_advancement;
use flotilla::message::RequestVoteArgs;
use flotilla::storage::ring_buffer::RingBufferLogStorage;
use flotilla::types::{LogIndex, NodeId, Term};

pub fn bench_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec");
    let args = RequestVoteArgs {
        term: Term(10),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(100),
        last_log_term: Term(9),
    };
    let mut buf = [0u8; 128];
    let written =
        encode_request_vote_args(&mut buf, NodeId(1), NodeId(2), Term(10), &args).unwrap();

    group.throughput(Throughput::Bytes(written as u64));

    group.bench_function("encode_request_vote", |b| {
        b.iter(|| {
            let mut out = [0u8; 128];
            encode_request_vote_args(
                black_box(&mut out),
                black_box(NodeId(1)),
                black_box(NodeId(2)),
                black_box(Term(10)),
                black_box(&args),
            )
            .unwrap()
        });
    });

    group.bench_function("decode_and_verify_packet", |b| {
        b.iter(|| {
            let (header, payload) = decode_packet(black_box(&buf[..written])).unwrap();
            black_box(verify_checksum(&header, payload))
        });
    });

    group.finish();
}

pub fn bench_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_ring_buffer");
    let payload = b"log_command_payload_32_bytes___";
    group.throughput(Throughput::Bytes(payload.len() as u64));

    group.bench_function("append_entry_steady_state", |b| {
        let mut storage = RingBufferLogStorage::<1024, 64>::new();
        let mut term = 1u64;
        b.iter(|| {
            let last = storage.last_index();
            if last.0 >= storage.first_index().0 + 512 {
                let _ = storage.compact_prefix(LogIndex(last.0 - 256));
            }
            term += 1;
            storage
                .append_entry(Term(term), black_box(payload))
                .unwrap()
        });
    });

    group.bench_function("entry_lookup", |b| {
        let mut storage = RingBufferLogStorage::<1024, 64>::new();
        for _i in 1..=512 {
            storage.append_entry(Term(1), payload).unwrap();
        }
        b.iter(|| black_box(storage.entry_at(black_box(LogIndex(256)))));
    });

    group.finish();
}

pub fn bench_commit_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("consensus_commit");
    let match_indices_3 = [LogIndex(100), LogIndex(100), LogIndex(85)];
    let match_indices_5 = [
        LogIndex(100),
        LogIndex(100),
        LogIndex(95),
        LogIndex(50),
        LogIndex(30),
    ];

    group.bench_function("evaluate_commit_3_nodes", |b| {
        b.iter(|| {
            evaluate_commit_advancement(
                black_box(&match_indices_3),
                black_box(LogIndex(90)),
                black_box(Term(2)),
                |_| Some(Term(2)),
            )
        });
    });

    group.bench_function("evaluate_commit_5_nodes", |b| {
        b.iter(|| {
            evaluate_commit_advancement(
                black_box(&match_indices_5),
                black_box(LogIndex(90)),
                black_box(Term(2)),
                |_| Some(Term(2)),
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_codec, bench_storage, bench_commit_evaluation);
criterion_main!(benches);
