use flotilla::storage::ring_buffer::{RingBufferLogStorage, StorageError, compute_slot_index};
use flotilla::types::{LogIndex, Term};

#[test]
fn test_ring_buffer_append_and_retrieve() {
    let mut storage = RingBufferLogStorage::<16, 64>::new();
    assert_eq!(storage.first_index(), LogIndex(1));
    assert_eq!(storage.last_index(), LogIndex(0));
    assert_eq!(storage.len(), 0);
    assert!(storage.is_empty());

    let payload1 = b"command_1";
    let idx1 = storage
        .append_entry(Term(1), payload1)
        .expect("Append should succeed");
    assert_eq!(idx1, LogIndex(1));
    assert_eq!(storage.last_index(), LogIndex(1));
    assert_eq!(storage.term_at(LogIndex(1)), Some(Term(1)));

    let entry1 = storage.entry_at(LogIndex(1)).expect("Entry 1 must exist");
    assert_eq!(entry1.payload_bytes(), payload1);

    let payload2 = b"command_2";
    let idx2 = storage
        .append_entry(Term(1), payload2)
        .expect("Append should succeed");
    assert_eq!(idx2, LogIndex(2));
    assert_eq!(storage.last_index(), LogIndex(2));
    assert_eq!(storage.term_at(LogIndex(2)), Some(Term(1)));

    let entry2 = storage.entry_at(LogIndex(2)).expect("Entry 2 must exist");
    assert_eq!(entry2.payload_bytes(), payload2);
}

#[test]
fn test_ring_buffer_truncate_suffix() {
    let mut storage = RingBufferLogStorage::<16, 64>::new();
    for i in 1..=5 {
        let payload = format!("cmd_{i}");
        storage.append_entry(Term(1), payload.as_bytes()).unwrap();
    }
    assert_eq!(storage.last_index(), LogIndex(5));

    // Truncate suffix from index 4 onwards
    storage.truncate_suffix(LogIndex(4));
    assert_eq!(storage.last_index(), LogIndex(3));
    assert!(storage.entry_at(LogIndex(3)).is_some());
    assert!(storage.entry_at(LogIndex(4)).is_none());
    assert!(storage.entry_at(LogIndex(5)).is_none());

    // Appending now replaces index 4
    let new_idx = storage.append_entry(Term(2), b"new_cmd_4").unwrap();
    assert_eq!(new_idx, LogIndex(4));
    assert_eq!(storage.term_at(LogIndex(4)), Some(Term(2)));
}

#[test]
fn test_ring_buffer_compaction_and_wrap_around() {
    let mut storage = RingBufferLogStorage::<4, 32>::new();

    // Fill buffer to capacity (4 entries)
    for i in 1..=4 {
        storage.append_entry(Term(1), &[i as u8]).unwrap();
    }
    assert_eq!(storage.last_index(), LogIndex(4));

    // Next append should fail because buffer is full
    let err = storage.append_entry(Term(1), &[5]);
    assert_eq!(err, Err(StorageError::BufferFull));

    // Compact up to index 2
    storage
        .compact_prefix(LogIndex(2))
        .expect("Compaction should succeed");
    assert_eq!(storage.first_index(), LogIndex(3));
    assert_eq!(storage.entry_at(LogIndex(1)), None);
    assert_eq!(storage.entry_at(LogIndex(2)), None);

    // Now we can append 2 more entries (slots 5 and 6)
    storage.append_entry(Term(2), &[5]).unwrap();
    storage.append_entry(Term(2), &[6]).unwrap();
    assert_eq!(storage.last_index(), LogIndex(6));
    assert_eq!(storage.entry_at(LogIndex(5)).unwrap().payload_bytes(), &[5]);
    assert_eq!(storage.entry_at(LogIndex(6)).unwrap().payload_bytes(), &[6]);
}

#[test]
fn test_slot_index_power_of_two_masking() {
    let mask = 16 - 1; // 15
    assert_eq!(compute_slot_index(LogIndex(1), mask), 0);
    assert_eq!(compute_slot_index(LogIndex(16), mask), 15);
    assert_eq!(compute_slot_index(LogIndex(17), mask), 0);
    assert_eq!(compute_slot_index(LogIndex(32), mask), 15);
}
