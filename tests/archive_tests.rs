use flotilla::archive::sinks::{FileArchiveSink, NullArchiveSink};
use flotilla::archive::{ArchivePipeline, ArchivedEntry, AsyncArchiveSink};
use flotilla::storage::ring_buffer::RingBufferLogStorage;
use flotilla::types::{LogIndex, Term};
use std::fs;

#[test]
fn test_null_archive_sink() {
    let mut sink = NullArchiveSink::new();
    let entries = vec![
        ArchivedEntry {
            index: LogIndex(1),
            term: Term(1),
            payload: b"command1".to_vec(),
        },
        ArchivedEntry {
            index: LogIndex(2),
            term: Term(1),
            payload: b"command2".to_vec(),
        },
    ];

    let written = sink
        .write_entries(&entries)
        .expect("Null sink should succeed");
    assert_eq!(written, 2);
    sink.flush().expect("Flush should succeed");
}

#[test]
fn test_file_archive_sink_roundtrip() {
    let test_dir = std::env::temp_dir().join("flotilla_test_archive");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let wal_path = test_dir.join("wal.log");
    let mut sink = FileArchiveSink::create(&wal_path).expect("Create sink");

    let entries = vec![
        ArchivedEntry {
            index: LogIndex(1),
            term: Term(1),
            payload: b"first_wal_entry".to_vec(),
        },
        ArchivedEntry {
            index: LogIndex(2),
            term: Term(1),
            payload: b"second_wal_entry".to_vec(),
        },
    ];

    let written = sink.write_entries(&entries).expect("Write entries");
    assert_eq!(written, 2);
    sink.flush().expect("Flush to disk");

    // Read back and verify
    let read_back = FileArchiveSink::read_all(&wal_path).expect("Read back from WAL");
    assert_eq!(read_back.len(), 2);
    assert_eq!(read_back[0].index, LogIndex(1));
    assert_eq!(read_back[0].payload, b"first_wal_entry");
    assert_eq!(read_back[1].index, LogIndex(2));
    assert_eq!(read_back[1].payload, b"second_wal_entry");

    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_archive_pipeline_drains_and_compacts_storage() {
    let mut storage = RingBufferLogStorage::<8, 64>::new();
    for i in 1..=5 {
        let payload = format!("data_{i}");
        storage.append_entry(Term(1), payload.as_bytes()).unwrap();
    }
    assert_eq!(storage.first_index(), LogIndex(1));
    assert_eq!(storage.last_index(), LogIndex(5));

    let sink = NullArchiveSink::new();
    let mut pipeline = ArchivePipeline::new(sink, 16);

    // Enqueue committed entries 1..=4
    let mut batch = Vec::new();
    for i in 1..=4 {
        let slot = storage.entry_at(LogIndex(i)).unwrap();
        batch.push(ArchivedEntry {
            index: slot.index,
            term: slot.term,
            payload: slot.payload_bytes().to_vec(),
        });
    }

    pipeline
        .enqueue_batch(&batch)
        .expect("Enqueue should succeed");
    pipeline
        .drain_and_persist()
        .expect("Persist should succeed");

    let persisted = pipeline.persisted_watermark();
    assert_eq!(persisted, LogIndex(4));

    // Storage can now compact prefix up to watermark
    storage
        .compact_prefix(persisted)
        .expect("Compaction should succeed");
    assert_eq!(storage.first_index(), LogIndex(5));
    assert_eq!(storage.entry_at(LogIndex(1)), None);
    assert_eq!(storage.entry_at(LogIndex(4)), None);
    assert!(storage.entry_at(LogIndex(5)).is_some());
}
