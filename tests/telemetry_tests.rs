use flotilla_raft::telemetry::{
    Counter, DurationHistogram, DurationTimer, FlotillaMetrics, SpanId, TraceContext,
    TraceEnvelope, TraceId,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_trace_id_generation_and_formatting() {
    let id1 = TraceId::generate();
    let id2 = TraceId::generate();
    assert_ne!(id1, id2);
    assert_ne!(id1, TraceId::NIL);

    let hex_str = id1.to_hex();
    assert_eq!(hex_str.len(), 32);

    let parsed = TraceId::from_hex(&hex_str).expect("Valid 32-char hex string must parse");
    assert_eq!(id1, parsed);

    assert!(TraceId::from_hex("invalid_hex").is_none());
    assert!(TraceId::from_hex("1234").is_none());
}

#[test]
fn test_span_id_generation_and_formatting() {
    let span1 = SpanId::generate();
    let span2 = SpanId::generate();
    assert_ne!(span1, span2);
    assert_ne!(span1, SpanId::NIL);

    let hex_str = span1.to_hex();
    assert_eq!(hex_str.len(), 16);

    let parsed = SpanId::from_hex(&hex_str).expect("Valid 16-char hex string must parse");
    assert_eq!(span1, parsed);

    assert!(SpanId::from_hex("short").is_none());
}

#[test]
fn test_trace_context_child_derivation_and_w3c_traceparent() {
    let root = TraceContext::new_root();
    assert!(root.flags.is_sampled());

    let child = root.child();
    // Trace ID must propagate through
    assert_eq!(root.trace_id, child.trace_id);
    // Span ID must be new
    assert_ne!(root.span_id, child.span_id);
    assert_eq!(root.flags, child.flags);

    // Format to W3C traceparent standard: 00-{trace_id}-{span_id}-{flags}
    let traceparent = root.to_traceparent();
    assert_eq!(traceparent.len(), 55);
    assert!(traceparent.starts_with("00-"));

    let parsed =
        TraceContext::from_traceparent(&traceparent).expect("Valid traceparent must parse");
    assert_eq!(root.trace_id, parsed.trace_id);
    assert_eq!(root.span_id, parsed.span_id);
    assert_eq!(root.flags, parsed.flags);

    // Invalid traceparents must fail gracefully
    assert!(TraceContext::from_traceparent("invalid").is_none());
    assert!(TraceContext::from_traceparent("01-unknown-version").is_none());
    assert!(
        TraceContext::from_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
            .is_none()
    ); // all zeros trace_id
    assert!(
        TraceContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")
            .is_none()
    ); // all zeros span_id
}

#[test]
fn test_trace_envelope_payload_wrapping() {
    let trace = TraceContext::new_root();
    let payload = b"command_to_replicate_with_trace_context";

    let mut buf = [0u8; 256];
    let written = TraceEnvelope::wrap(&trace, payload, &mut buf).expect("Wrapping should succeed");

    assert!(TraceEnvelope::is_enveloped(&buf[..written]));

    let (extracted_trace, extracted_payload) =
        TraceEnvelope::unwrap(&buf[..written]).expect("Unwrapping should succeed");

    assert_eq!(extracted_trace.trace_id, trace.trace_id);
    assert_eq!(extracted_trace.span_id, trace.span_id);
    assert_eq!(extracted_trace.flags, trace.flags);
    assert_eq!(extracted_payload, payload);

    // Unwrapping raw payload without envelope should return error
    assert!(TraceEnvelope::unwrap(b"raw_unwrapped_payload").is_err());
}

#[test]
fn test_counter_atomic_operations() {
    let counter = Arc::new(Counter::new(0));
    assert_eq!(counter.get(), 0);

    counter.inc();
    assert_eq!(counter.get(), 1);

    counter.inc_by(9);
    assert_eq!(counter.get(), 10);

    // Multi-threaded increments
    let mut handles = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                c.inc();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(counter.get(), 8010);

    counter.reset();
    assert_eq!(counter.get(), 0);
}

#[test]
fn test_duration_histogram_statistics_and_percentiles() {
    let hist = DurationHistogram::new();
    assert_eq!(hist.count(), 0);
    assert_eq!(hist.min_micros(), 0);
    assert_eq!(hist.max_micros(), 0);

    // Record various durations
    hist.record(Duration::from_micros(5)); // <= 10µs bucket
    hist.record(Duration::from_micros(40)); // <= 50µs bucket
    hist.record(Duration::from_micros(150)); // <= 250µs bucket
    hist.record(Duration::from_millis(2)); // <= 2.5ms bucket
    hist.record(Duration::from_millis(50)); // <= 50ms bucket

    assert_eq!(hist.count(), 5);
    assert_eq!(hist.min_micros(), 5);
    assert_eq!(hist.max_micros(), 50_000);
    assert_eq!(hist.sum_micros(), 5 + 40 + 150 + 2000 + 50_000);
    assert!(hist.mean_micros() > 10_000.0 && hist.mean_micros() < 11_000.0);

    // Percentiles
    let p50 = hist.p50_micros();
    let p90 = hist.p90_micros();
    let p99 = hist.p99_micros();
    assert!(p50 <= p90);
    assert!(p90 <= p99);

    // Bucket verification
    let buckets = hist.bucket_counts();
    assert!(!buckets.is_empty());
}

#[test]
fn test_duration_timer_raii() {
    let hist = DurationHistogram::new();
    {
        let _timer = DurationTimer::start(&hist);
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(hist.count(), 1);
    assert!(hist.min_micros() >= 4000);
}

#[test]
fn test_flotilla_metrics_comprehensive_counts_and_durations() {
    let metrics = FlotillaMetrics::new();

    // Increment various counts
    metrics.proposals_total.inc();
    metrics.proposals_committed.inc();
    metrics.messages_received_total.inc_by(10);
    metrics.messages_sent_total.inc_by(15);
    metrics.ticks_total.inc_by(100);
    metrics.elections_started.inc();
    metrics.elections_won.inc();

    // Record durations
    metrics.proposal_duration.record(Duration::from_micros(250));
    metrics.step_duration.record(Duration::from_micros(15));
    metrics.tick_duration.record(Duration::from_micros(8));

    let snap = metrics.snapshot();
    assert_eq!(snap.proposals_total, 1);
    assert_eq!(snap.proposals_committed, 1);
    assert_eq!(snap.messages_received_total, 10);
    assert_eq!(snap.messages_sent_total, 15);
    assert_eq!(snap.ticks_total, 100);
    assert_eq!(snap.elections_started, 1);
    assert_eq!(snap.elections_won, 1);
    assert_eq!(snap.proposal_duration.count, 1);
    assert_eq!(snap.step_duration.count, 1);
    assert_eq!(snap.tick_duration.count, 1);

    // Test Prometheus text exposition
    let prom = snap.to_prometheus_text();
    assert!(prom.contains("# TYPE flotilla_proposals_total counter"));
    assert!(prom.contains("flotilla_proposals_total 1"));
    assert!(prom.contains("# TYPE flotilla_proposal_duration_seconds histogram"));
    assert!(prom.contains("flotilla_proposal_duration_seconds_count 1"));

    // Test Summary format
    let summary = snap.to_summary_report();
    assert!(summary.contains("Flotilla Telemetry Metrics Summary"));
    assert!(summary.contains("proposals_total: 1"));
}

#[test]
fn test_telemetry_hot_path_zero_allocations() {
    let hist = DurationHistogram::new();
    let counter = Counter::new(0);

    // Direct recording should not allocate
    for _ in 0..1000 {
        counter.inc();
        hist.record(Duration::from_micros(10));
    }

    assert_eq!(counter.get(), 1000);
    assert_eq!(hist.count(), 1000);
}

#[test]
fn test_duration_timer_stop_and_elapsed() {
    let hist = DurationHistogram::new();
    let mut timer = DurationTimer::start(&hist);
    thread::sleep(Duration::from_millis(2));
    assert!(timer.elapsed() >= Duration::from_millis(1));

    let recorded = timer.stop();
    assert!(recorded >= Duration::from_millis(1));
    assert_eq!(hist.count(), 1);

    // Second stop call returns Duration::ZERO
    let second_stop = timer.stop();
    assert_eq!(second_stop, Duration::ZERO);
    assert_eq!(hist.count(), 1);
}

#[test]
fn test_trace_envelope_edge_cases() {
    use flotilla_raft::telemetry::TelemetryError;

    // Buffer too small for header
    let mut small_buf = [0u8; 10];
    let trace = TraceContext::new_root();
    assert_eq!(
        TraceEnvelope::wrap(&trace, b"payload", &mut small_buf),
        Err(TelemetryError::BufferTooSmall)
    );

    // Unwrapping buffer too small
    assert_eq!(
        TraceEnvelope::unwrap(&[0u8; 10]),
        Err(TelemetryError::BufferTooSmall)
    );

    // Invalid magic bytes
    let mut bad_magic = [0u8; 50];
    bad_magic[0] = 0xFF;
    assert_eq!(
        TraceEnvelope::unwrap(&bad_magic),
        Err(TelemetryError::InvalidEnvelopeMagic)
    );

    // Payload length mismatch
    let mut valid_envelope = [0u8; 100];
    let written = TraceEnvelope::wrap(&trace, b"hello", &mut valid_envelope).unwrap();
    // Truncate valid envelope
    assert_eq!(
        TraceEnvelope::unwrap(&valid_envelope[..written - 2]),
        Err(TelemetryError::PayloadLengthMismatch)
    );
}

#[test]
fn test_telemetry_error_display() {
    use flotilla_raft::telemetry::TelemetryError;
    assert!(format!("{}", TelemetryError::BufferTooSmall).contains("buffer too small"));
    assert!(format!("{}", TelemetryError::InvalidEnvelopeMagic).contains("magic bytes"));
    assert!(format!("{}", TelemetryError::PayloadLengthMismatch).contains("mismatch"));
    assert!(format!("{}", TelemetryError::InvalidTraceparent).contains("traceparent"));
}

#[cfg(all(feature = "client-grpc", feature = "server-grpc"))]
#[tokio::test]
async fn test_grpc_trace_propagation_and_metrics() {
    use flotilla_raft::client::{FlotillaClient, GrpcClient};
    use flotilla_raft::engine::{RaftConfig, RaftNode};
    use flotilla_raft::server::GrpcService;
    use flotilla_raft::types::NodeId;
    use parking_lot::Mutex;

    let initial_proposals = flotilla_raft::telemetry::metrics().grpc_proposals.get();

    let mut raft = RaftNode::<1024, 1024>::new(RaftConfig {
        node_id: NodeId(1),
        peers: vec![],
        election_timeout_ticks: 1,
        heartbeat_interval_ticks: 1,
    });
    // Single node cluster campaigns and elects itself leader
    let _ = raft.tick();
    assert_eq!(raft.role(), flotilla_raft::types::Role::Leader);

    let node = Arc::new(Mutex::new(raft));
    let service = GrpcService::new(Arc::clone(&node));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    tokio::spawn(async move {
        let _ = service.serve(addr).await;
    });

    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let client = GrpcClient::connect(format!("http://{addr}"));
    let trace = TraceContext::new_root();

    let result = client
        .propose_with_trace(b"grpc_telemetry_command", &trace)
        .await
        .expect("gRPC proposal should succeed");
    assert!(result.is_success());

    assert!(flotilla_raft::telemetry::metrics().grpc_proposals.get() > initial_proposals);
    assert!(
        flotilla_raft::telemetry::metrics()
            .grpc_proposal_duration
            .count()
            > 0
    );
}

#[cfg(feature = "client-udp")]
#[tokio::test]
async fn test_udp_client_metrics_tracking() {
    use flotilla_raft::client::{FlotillaClient, UdpClient};
    use flotilla_raft::engine::{RaftConfig, RaftNode};
    use flotilla_raft::server::UdpListener;
    use flotilla_raft::types::NodeId;
    use flotilla_raft::udp::UdpClusterRouter;

    let init_proposals = flotilla_raft::telemetry::metrics()
        .client_proposals_total
        .get();

    let mut router = UdpClusterRouter::new();
    let listener = UdpListener::bind("127.0.0.1:0", router.clone()).unwrap();
    let local_addr = listener.local_addr().unwrap();
    router.register_peer(NodeId(1), local_addr);

    let client = UdpClient::connect(local_addr).unwrap();
    let trace = TraceContext::new_root();

    // Spawn server thread to handle the proposal
    let listener_handle = thread::spawn(move || {
        let mut node = RaftNode::<1024, 1024>::new(RaftConfig {
            node_id: NodeId(1),
            peers: vec![],
            election_timeout_ticks: 10,
            heartbeat_interval_ticks: 3,
        });
        node.tick(); // Elect leader

        let mut buf = [0u8; 1500];
        let _ = listener.poll_and_step(&mut node, &mut buf);
    });

    let res = client
        .propose_with_trace(b"udp_telemetry_cmd", &trace)
        .await;
    let _ = listener_handle.join();

    assert!(res.is_ok());
    assert!(
        flotilla_raft::telemetry::metrics()
            .client_proposals_total
            .get()
            > init_proposals
    );
    assert!(
        flotilla_raft::telemetry::metrics()
            .proposal_duration
            .count()
            > 0
    );
}

#[test]
fn test_telemetry_branches_and_defaults() {
    use flotilla_raft::telemetry::{
        Counter, DurationHistogram, FlotillaMetrics, HistogramSnapshot, SpanId, TraceContext,
        TraceEnvelope, TraceFlags, TraceId,
    };

    // TraceId
    assert!(TraceId::NIL.is_nil());
    assert!(!TraceId::generate().is_nil());
    assert!(TraceId::from_hex(&"z".repeat(32)).is_none());
    let raw_bytes = [1u8; 16];
    let id_from_raw = TraceId::from_bytes(raw_bytes);
    assert_eq!(id_from_raw.as_bytes(), &raw_bytes);
    assert_eq!(format!("{id_from_raw}"), id_from_raw.to_hex());
    assert!(format!("{id_from_raw:?}").contains("TraceId("));

    // SpanId
    assert!(SpanId::NIL.is_nil());
    assert!(!SpanId::generate().is_nil());
    assert!(SpanId::from_hex(&"z".repeat(16)).is_none());
    let span_raw = [2u8; 8];
    let span_from_raw = SpanId::from_bytes(span_raw);
    assert_eq!(span_from_raw.as_bytes(), &span_raw);
    assert_eq!(format!("{span_from_raw}"), span_from_raw.to_hex());
    assert!(format!("{span_from_raw:?}").contains("SpanId("));

    // TraceFlags
    assert!(!TraceFlags::NONE.is_sampled());
    assert!(TraceFlags::SAMPLED.is_sampled());
    assert_eq!(TraceFlags::from_hex("01"), Some(TraceFlags::SAMPLED));
    assert_eq!(TraceFlags::from_hex("00"), Some(TraceFlags::NONE));
    assert_eq!(TraceFlags::from_hex("invalid"), None);
    assert_eq!(TraceFlags::from_hex("1"), None);
    assert_eq!(TraceFlags::from_hex("zz"), None);
    assert!(format!("{:?}", TraceFlags::SAMPLED).contains("TraceFlags(0x01)"));
    assert_eq!(format!("{}", TraceFlags::SAMPLED), "01");

    // TraceContext
    let default_ctx = TraceContext::default();
    assert!(default_ctx.flags.is_sampled());
    let ctx_bytes = default_ctx.to_bytes();
    let ctx_reconstructed = TraceContext::from_bytes(&ctx_bytes);
    assert_eq!(default_ctx, ctx_reconstructed);
    assert!(format!("{default_ctx:?}").contains("TraceContext"));
    assert_eq!(format!("{default_ctx}"), default_ctx.to_traceparent());

    // TraceContext::from_traceparent error branches
    assert!(TraceContext::from_traceparent("00-invalid").is_none());
    assert!(
        TraceContext::from_traceparent(&format!("00-{}-0000000000000001-01", "z".repeat(32)))
            .is_none()
    );
    assert!(
        TraceContext::from_traceparent(&format!(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-{}-01",
            "z".repeat(16)
        ))
        .is_none()
    );
    assert!(
        TraceContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-zz")
            .is_none()
    );

    // TraceEnvelope branch tests
    assert!(!TraceEnvelope::is_enveloped(&[0u8; 10]));
    let not_magic_50 = [0u8; 50];
    assert!(!TraceEnvelope::is_enveloped(&not_magic_50));
    let mut half_magic_50 = [0u8; 50];
    half_magic_50[0] = TraceEnvelope::MAGIC[0];
    half_magic_50[1] = 0x00;
    assert!(!TraceEnvelope::is_enveloped(&half_magic_50));

    let mut bad_magic_second_byte = [0u8; 40];
    bad_magic_second_byte[0] = TraceEnvelope::MAGIC[0];
    bad_magic_second_byte[1] = 0xFF;
    assert!(!TraceEnvelope::is_enveloped(&bad_magic_second_byte));
    assert_eq!(
        TraceEnvelope::unwrap(&bad_magic_second_byte),
        Err(flotilla_raft::telemetry::TelemetryError::InvalidEnvelopeMagic)
    );

    let mut bad_ver_buf = [0u8; 40];
    bad_ver_buf[0] = TraceEnvelope::MAGIC[0];
    bad_ver_buf[1] = TraceEnvelope::MAGIC[1];
    bad_ver_buf[2..4].copy_from_slice(&99u16.to_be_bytes());
    assert_eq!(
        TraceEnvelope::unwrap(&bad_ver_buf),
        Err(flotilla_raft::telemetry::TelemetryError::InvalidEnvelopeMagic)
    );

    // DurationHistogram empty statistics
    let hist = DurationHistogram::default();
    assert_eq!(hist.min_micros(), 0);
    assert_eq!(hist.mean_micros(), 0.0);
    assert_eq!(hist.estimate_percentile(0.5), 0);
    assert!(format!("{hist:?}").contains("DurationHistogram"));

    // Counter default & reset
    let counter = Counter::default();
    assert_eq!(counter.get(), 0);
    assert!(format!("{counter:?}").contains("Counter(0)"));

    // HistogramSnapshot default
    let snap_empty = HistogramSnapshot::default();
    assert_eq!(snap_empty, HistogramSnapshot::empty());

    // FlotillaMetrics default & reset
    let metrics = FlotillaMetrics::default();
    metrics.proposals_total.inc();
    metrics.reset();
    assert_eq!(metrics.proposals_total.get(), 0);
}

#[test]
fn test_hex_and_traceparent_branch_exhaustion() {
    use flotilla_raft::telemetry::{DurationHistogram, SpanId, TraceContext, TraceFlags, TraceId};

    // TraceId::from_hex branches:
    assert!(TraceId::from_hex("").is_none());
    assert!(TraceId::from_hex("1234").is_none());
    assert!(TraceId::from_hex("0123456789abcdef0123456789abcdef01").is_none());
    assert!(TraceId::from_hex(&"a".repeat(40)).is_none());
    assert!(TraceId::from_hex(&"0".repeat(32)).is_some());
    assert!(TraceId::from_hex(&format!("{}g{}", "0".repeat(30), "0")).is_none());

    // SpanId::from_hex branches:
    assert!(SpanId::from_hex("").is_none());
    assert!(SpanId::from_hex("123").is_none());
    assert!(SpanId::from_hex("0123456789abcdef0").is_none());
    assert!(SpanId::from_hex(&"b".repeat(20)).is_none());
    assert!(SpanId::from_hex("000000000000000g").is_none());

    // TraceFlags::from_hex branches:
    assert!(TraceFlags::from_hex("").is_none());
    assert!(TraceFlags::from_hex("1").is_none());
    assert!(TraceFlags::from_hex("123").is_none());
    assert!(TraceFlags::from_hex("gg").is_none());
    assert!(TraceFlags::from_hex("0g").is_none());
    assert!(TraceFlags::from_hex("g0").is_none());

    // TraceContext::from_traceparent branches:
    assert!(TraceContext::from_traceparent("00-1-2").is_none());
    assert!(
        TraceContext::from_traceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .is_none()
    );
    assert!(
        TraceContext::from_traceparent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .is_none()
    );
    assert!(
        TraceContext::from_traceparent("00-not_a_valid_hex_trace_id_32_chars!-00f067aa0ba902b7-01")
            .is_none()
    );
    assert!(
        TraceContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-not_valid_span-01")
            .is_none()
    );
    assert!(
        TraceContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-gg")
            .is_none()
    );
    assert!(
        TraceContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1")
            .is_none()
    );

    // DurationHistogram branches:
    let hist = DurationHistogram::new();
    hist.record_micros(20_000_000); // 20s > 10s largest bucket
    assert_eq!(hist.max_micros(), 20_000_000);
    assert_eq!(hist.estimate_percentile(1.0), u64::MAX);

    hist.record_micros(100);
    hist.record_micros(50);
    hist.record_micros(500);
    hist.record_micros(10);
    assert_eq!(hist.min_micros(), 10);
    assert_eq!(hist.max_micros(), 20_000_000);
}

#[cfg(any(feature = "client-grpc", feature = "server-grpc"))]
#[test]
fn test_grpc_propagation_helpers_edge_cases() {
    use flotilla_raft::telemetry::{
        TraceContext, extract_grpc_traceparent, inject_grpc_traceparent,
    };
    use tonic::metadata::MetadataMap;

    let mut map = MetadataMap::new();
    assert!(extract_grpc_traceparent(&map).is_none());

    let trace = TraceContext::new_root();
    inject_grpc_traceparent(&mut map, &trace);
    let extracted = extract_grpc_traceparent(&map).expect("should extract injected trace");
    assert_eq!(extracted.trace_id, trace.trace_id);
    assert_eq!(extracted.span_id, trace.span_id);

    map.insert(
        tonic::metadata::MetadataKey::from_static("traceparent"),
        tonic::metadata::MetadataValue::from_static("invalid"),
    );
    assert!(extract_grpc_traceparent(&map).is_none());
}

#[tokio::test]
async fn test_flotilla_client_default_propose() {
    use flotilla_raft::client::{ClientError, FlotillaClient, ProposalResult};
    use flotilla_raft::telemetry::TraceContext;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestClient {
        trace_called: AtomicBool,
    }

    impl FlotillaClient for TestClient {
        async fn propose_with_trace(
            &self,
            _payload: &[u8],
            trace: &TraceContext,
        ) -> Result<ProposalResult, ClientError> {
            assert!(trace.flags.is_sampled());
            self.trace_called.store(true, Ordering::SeqCst);
            Ok(ProposalResult::success(
                flotilla_raft::types::LogIndex(1),
                flotilla_raft::types::Term(1),
                flotilla_raft::types::NodeId(1),
            ))
        }

        async fn ping(&self) -> Result<bool, ClientError> {
            Ok(true)
        }
    }

    let client = TestClient {
        trace_called: AtomicBool::new(false),
    };

    let res = client.propose(b"default_payload").await.unwrap();
    assert!(res.is_success());
    assert!(client.trace_called.load(Ordering::SeqCst));
    assert!(client.ping().await.unwrap());
}
