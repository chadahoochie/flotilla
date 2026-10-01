#![cfg(feature = "cosmos")]

use chrono::{TimeZone, Utc};
use flotilla::archive::cosmos::*;
use flotilla::archive::{ArchivePipeline, ArchivedEntry, AsyncArchiveSink};
use flotilla::types::{LogIndex, Term};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::unbounded_channel;

#[test]
fn test_cosmos_config_defaults() {
    let config = CosmosConfig::new(
        "https://test-account.documents.azure.com:443/",
        "dGVzdF9rZXk=",
        "testdb",
        "ingress-journal-v2",
        "adt",
    );

    assert_eq!(
        config.endpoint,
        "https://test-account.documents.azure.com:443/"
    );
    assert_eq!(config.master_key, "dGVzdF9rZXk=");
    assert_eq!(config.database, "testdb");
    assert_eq!(config.container, "ingress-journal-v2");
    assert_eq!(config.slice_key, "adt");
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.flush_interval_ms, 250);
    assert_eq!(config.ttl_seconds, 7_776_000); // 90 days
    assert_eq!(config.max_retries, 5);
}

#[test]
fn test_cosmos_document_creation_and_serialization() {
    let now = Utc.with_ymd_and_hms(2026, 9, 29, 22, 30, 0).unwrap();
    let entry = ArchivedEntry {
        index: LogIndex(105),
        term: Term(3),
        payload: b"{\"patient_id\":\"12345\"}".to_vec(),
    };

    let doc = create_cosmos_document(&entry, "adt", 7_776_000, now);

    assert_eq!(doc.id, "adt_105");
    assert_eq!(doc.slice_key, "adt");
    assert_eq!(doc.date_bucket, "2026-09-29");
    assert_eq!(doc.log_index, 105);
    assert_eq!(doc.term, 3);
    assert_eq!(doc.ttl, 7_776_000);
    assert_eq!(doc.payload, "{\"patient_id\":\"12345\"}");
    assert_eq!(doc.source, "flotilla");
    assert_eq!(doc.message_type, "RaftLogEntry");
    assert_eq!(doc.outcome, "Committed");

    let json = serde_json::to_string(&doc).expect("Serialize to JSON");
    assert!(json.contains("\"sliceKey\":\"adt\""));
    assert!(json.contains("\"dateBucket\":\"2026-09-29\""));
    assert!(json.contains("\"logIndex\":105"));
    assert!(json.contains("\"receivedAt\":\"2026-09-29T22:30:00+00:00\""));
    assert!(json.contains("\"expiresAt\":\"2026-12-28T22:30:00+00:00\""));

    let deserialized: CosmosDocument = serde_json::from_str(&json).expect("Deserialize JSON");
    assert_eq!(deserialized, doc);
}

#[test]
fn test_cosmos_document_binary_payload_fallback() {
    let now = Utc::now();
    // Non UTF-8 binary bytes
    let entry = ArchivedEntry {
        index: LogIndex(1),
        term: Term(1),
        payload: vec![0xFF, 0xFE, 0xFD, 0xFC],
    };

    let doc = create_cosmos_document(&entry, "siu", 86400, now);
    assert_eq!(doc.slice_key, "siu");
    assert_eq!(doc.id, "siu_1");
    // Should be base64 encoded
    assert!(!doc.payload.is_empty());
}

#[test]
fn test_cosmos_auth_signature_generation() {
    // Valid 256-bit dummy key in base64 (32 bytes)
    let key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    let verb = "post";
    let resource_type = "docs";
    let resource_id = "dbs/testdb/colls/ingress-journal-v2";
    let date = "Tue, 29 Sep 2026 22:30:00 GMT";

    let auth = CosmosAuth::generate_auth_header(verb, resource_type, resource_id, date, key)
        .expect("Generate auth header");

    assert!(auth.starts_with("type=master&ver=1.0&sig="));
    assert!(auth.len() > 30);

    // Invalid base64 key should fail cleanly
    let invalid_key = "not_valid_base64!!!";
    let err = CosmosAuth::generate_auth_header(verb, resource_type, resource_id, date, invalid_key);
    assert!(err.is_err());
    match err {
        Err(CosmosError::Auth(msg)) => assert!(msg.contains("Invalid master key base64")),
        _ => panic!("Expected CosmosError::Auth"),
    }
}

#[test]
fn test_cosmos_client_header_formatting() {
    let pk_header = format_partition_key_header("adt", "2026-09-29");
    assert_eq!(pk_header, "[\"adt\",\"2026-09-29\"]");

    let resource_link = build_docs_resource_link("journal_db", "journal_coll");
    assert_eq!(resource_link, "dbs/journal_db/colls/journal_coll");

    let url = build_docs_url("https://account.documents.azure.com:443/", "mydb", "mycoll");
    assert_eq!(
        url,
        "https://account.documents.azure.com:443/dbs/mydb/colls/mycoll/docs"
    );

    let dt = Utc.with_ymd_and_hms(2026, 9, 29, 22, 0, 0).unwrap();
    let rfc1123 = format_rfc1123_date(dt);
    assert_eq!(rfc1123, "Tue, 29 Sep 2026 22:00:00 GMT");
}

#[test]
fn test_cosmos_archive_sink_queue_and_flush() {
    let (tx, mut rx) = unbounded_channel();
    let watermark = Arc::new(AtomicU64::new(0));

    let mut sink = CosmosArchiveSink::from_sender(tx, Arc::clone(&watermark));

    let entries = vec![
        ArchivedEntry {
            index: LogIndex(10),
            term: Term(1),
            payload: b"entry_10".to_vec(),
        },
        ArchivedEntry {
            index: LogIndex(11),
            term: Term(1),
            payload: b"entry_11".to_vec(),
        },
    ];

    let written = sink.write_entries(&entries).expect("Write entries");
    assert_eq!(written, 2);

    // Verify command received in channel
    match rx.try_recv() {
        Ok(OffloaderCommand::WriteEntries(received)) => {
            assert_eq!(received.len(), 2);
            assert_eq!(received[0].index, LogIndex(10));
            assert_eq!(received[1].index, LogIndex(11));
        }
        other => panic!("Expected OffloaderCommand::WriteEntries, got {other:?}"),
    }

    // Spawn thread to respond to flush command
    let handle = std::thread::spawn(move || {
        if let Some(OffloaderCommand::Flush(responder)) = rx.blocking_recv() {
            watermark.store(11, Ordering::Release);
            let _ = responder.send(Ok(()));
        }
    });

    sink.flush().expect("Flush should succeed");
    handle.join().unwrap();

    assert_eq!(sink.persisted_watermark(), LogIndex(11));
}

#[test]
fn test_cosmos_archive_pipeline_integration() {
    let (tx, mut rx) = unbounded_channel();
    let watermark = Arc::new(AtomicU64::new(0));
    let sink = CosmosArchiveSink::from_sender(tx, Arc::clone(&watermark));

    let mut pipeline = ArchivePipeline::new(sink, 16);

    let entries = vec![
        ArchivedEntry {
            index: LogIndex(1),
            term: Term(1),
            payload: b"p1".to_vec(),
        },
        ArchivedEntry {
            index: LogIndex(2),
            term: Term(1),
            payload: b"p2".to_vec(),
        },
    ];

    pipeline.enqueue_batch(&entries).expect("Enqueue");

    // Spawn thread to handle flush
    let watermark_clone = Arc::clone(&watermark);
    let handle = std::thread::spawn(move || {
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                OffloaderCommand::WriteEntries(batch) => {
                    if let Some(last) = batch.last() {
                        watermark_clone.store(last.index.0, Ordering::Release);
                    }
                }
                OffloaderCommand::Flush(resp) => {
                    let _ = resp.send(Ok(()));
                    break;
                }
                OffloaderCommand::Shutdown => break,
            }
        }
    });

    let persisted_count = pipeline.drain_and_persist().expect("Drain and persist");
    assert_eq!(persisted_count, 2);
    assert_eq!(pipeline.persisted_watermark(), LogIndex(2));

    handle.join().unwrap();
}

#[tokio::test]
async fn test_upload_entry_batch_empty() {
    let config = CosmosConfig::new(
        "http://localhost:8080",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "db",
        "coll",
        "adt",
    );
    let client = CosmosClient::new(config.clone());

    let res = upload_entry_batch(&client, &[], &config).await;
    assert_eq!(res.unwrap(), None);
}
