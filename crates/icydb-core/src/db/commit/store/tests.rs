use super::{RawCommitMarker, marker_envelope::encode_commit_marker_bytes};
use crate::{
    db::{
        commit::marker::{
            COMMIT_MARKER_FORMAT_VERSION_CURRENT, CommitMarker, MAX_COMMIT_BYTES,
            decode_commit_marker_payload, encode_commit_marker_payload,
        },
        data::{DecodedDataStoreKey, RawDataStoreKey},
        integrity::DatabaseIncarnationId,
        journal::{JournalBatch, JournalRecord, JournalSequence},
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    types::EntityTag,
};
use ic_stable_structures::Memory;
// Wrap one test marker payload in the canonical marker envelope so strict
// decode still reaches shape validation.
fn encode_test_marker_payload(marker: &CommitMarker) -> Vec<u8> {
    let payload =
        encode_commit_marker_payload(marker).expect("test marker payload encode should succeed");

    encode_commit_marker_bytes(COMMIT_MARKER_FORMAT_VERSION_CURRENT, &payload)
        .expect("test marker envelope encode should succeed")
}

// Materialize one canonical raw data-store key for marker tests.
fn raw_data_store_key(fill: u8) -> RawDataStoreKey {
    DecodedDataStoreKey::try_from_typed_key(EntityTag::new(1), &u64::from(fill))
        .expect("test key should encode")
        .to_raw()
        .expect("test key should materialize")
}

#[test]
fn commit_control_slot_rejects_corrupt_magic() {
    let store = super::CommitStore::init(test_memory(233));
    let mut malformed = Vec::new();
    malformed.extend_from_slice(b"XMCS");
    malformed.push(1);
    malformed.extend_from_slice(&DatabaseIncarnationId::for_tests(0x41).to_bytes());
    malformed.extend_from_slice(&0u32.to_le_bytes());
    store.set_raw_marker_bytes_for_tests(malformed);

    let err = store
        .marker_is_empty()
        .expect_err("corrupt control-slot magic should fail closed");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
}

#[test]
fn commit_marker_empty_bytes_decode_as_absent_marker() {
    let decoded = RawCommitMarker(Vec::new())
        .try_decode()
        .expect("empty marker bytes should decode as marker absence");

    assert!(
        decoded.is_none(),
        "empty marker bytes are the explicit no-marker durable state",
    );
}

#[test]
fn commit_marker_rejects_truncated_envelope_header() {
    let err = RawCommitMarker(vec![COMMIT_MARKER_FORMAT_VERSION_CURRENT])
        .try_decode()
        .expect_err("truncated marker envelope header should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_rejects_truncated_envelope_payload() {
    let mut bytes = Vec::new();
    bytes.push(COMMIT_MARKER_FORMAT_VERSION_CURRENT);
    bytes.extend_from_slice(&4u32.to_le_bytes());
    bytes.push(0xAA);

    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("truncated marker envelope payload should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_rejects_trailing_payload_bytes() {
    let marker = CommitMarker {
        id: [0u8; 16],
        journal_batches: Vec::new(),
    };

    let mut bytes = encode_test_marker_payload(&marker);
    bytes.push(0xFF);
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("trailing payload bytes should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_current_version_round_trip_succeeds() {
    let marker = CommitMarker {
        id: [9u8; 16],
        journal_batches: Vec::new(),
    };
    let encoded = encode_test_marker_payload(&marker);
    let decoded = RawCommitMarker(encoded)
        .try_decode()
        .expect("current-version marker envelope should decode")
        .expect("marker payload should be present");

    assert_eq!(decoded.id, marker.id);
    assert!(decoded.journal_batches().is_empty());
}

#[test]
fn commit_marker_embeds_marker_bound_journal_batches() {
    let marker_id = [0xAB; 16];
    let journal_batch = JournalBatch::new(
        [0x44; 16],
        marker_id,
        JournalSequence::new(1),
        vec![
            JournalRecord::row_put(
                "test::Entity",
                raw_data_store_key(4),
                vec![0x77; 3],
                [0x55; 16],
            )
            .expect("journal row record should build"),
        ],
    )
    .expect("journal batch should build");
    let marker = CommitMarker::from_parts(marker_id, vec![journal_batch.clone()])
        .expect("marker-bound journal batch should build");

    let bytes =
        encode_commit_marker_payload(&marker).expect("marker payload should encode journal batch");
    let decoded = decode_commit_marker_payload(&bytes)
        .expect("marker payload should decode embedded journal batch");

    assert_eq!(decoded.journal_batches(), &[journal_batch]);
}

#[test]
fn commit_marker_rejects_unbound_journal_batch() {
    let marker_id = [0xAB; 16];
    let journal_batch = JournalBatch::new(
        [0x44; 16],
        [0xCD; 16],
        JournalSequence::new(1),
        vec![
            JournalRecord::row_delete("test::Entity", raw_data_store_key(4), [0x55; 16])
                .expect("journal row-delete record should build"),
        ],
    )
    .expect("journal batch should build");

    let err = CommitMarker::from_parts(marker_id, vec![journal_batch])
        .expect_err("journal batch must be bound to enclosing marker id");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_future_version_fails_closed() {
    let marker = CommitMarker {
        id: [6u8; 16],
        journal_batches: Vec::new(),
    };
    let marker_payload = encode_commit_marker_payload(&marker)
        .expect("marker payload encode for future-version test should work");
    let future_version = COMMIT_MARKER_FORMAT_VERSION_CURRENT.saturating_add(1);
    let encoded = encode_commit_marker_bytes(future_version, &marker_payload)
        .expect("future-version marker envelope encode should succeed");
    let err = RawCommitMarker(encoded)
        .try_decode()
        .expect_err("future marker versions must fail closed");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
}

#[test]
fn commit_marker_rejects_oversized_stored_payload_as_corruption() {
    let len = (MAX_COMMIT_BYTES as usize).saturating_add(1);
    let err = RawCommitMarker(vec![0; len])
        .try_decode()
        .expect_err("oversized persisted marker should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn clear_verified_rejects_malformed_control_slot() {
    let store = super::CommitStore::init(test_memory(232));
    let mut malformed = Vec::new();
    malformed.extend_from_slice(b"ICCS");
    malformed.push(1);
    malformed.extend_from_slice(&DatabaseIncarnationId::for_tests(0x42).to_bytes());
    malformed.extend_from_slice(&1u32.to_le_bytes());
    store.set_raw_marker_bytes_for_tests(malformed.clone());

    let err = store
        .clear_verified()
        .expect_err("malformed control slot should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(store.raw_control_slot_bytes_for_tests(), malformed);
}

#[test]
fn commit_slot_writes_and_clears_preserve_database_boot_record() {
    let memory = test_memory(231);
    let store = super::CommitStore::init(memory.clone());
    let mut boot_before = [0_u8; crate::db::database_format::DATABASE_BOOT_RECORD_BYTES];
    memory.read(0, &mut boot_before);
    let control_slot = super::CommitStore::encode_raw_control_slot_for_tests(vec![0xaa])
        .expect("test control slot should encode");

    store.set_raw_marker_bytes_for_tests(control_slot);
    store.clear_raw_for_tests();

    let mut boot_after = [0_u8; crate::db::database_format::DATABASE_BOOT_RECORD_BYTES];
    memory.read(0, &mut boot_after);
    assert_eq!(boot_after, boot_before);
}

#[test]
fn commit_marker_transitions_preserve_database_incarnation() {
    let store = super::CommitStore::init(test_memory(227));
    let incarnation_before = store
        .database_incarnation_id()
        .expect("current control slot should carry an incarnation");
    let marker = CommitMarker {
        id: [0xA7; 16],
        journal_batches: Vec::new(),
    };

    store
        .set_if_empty(&marker)
        .expect("marker publication should preserve control metadata");
    assert_eq!(
        store
            .database_incarnation_id()
            .expect("marker-bearing control slot should carry an incarnation"),
        incarnation_before,
    );

    store
        .clear_verified()
        .expect("marker clear should preserve control metadata");
    assert_eq!(
        store
            .database_incarnation_id()
            .expect("cleared control slot should carry an incarnation"),
        incarnation_before,
    );
}

#[test]
fn ordinary_reopen_preserves_database_incarnation() {
    let memory = test_memory(225);
    let first = super::CommitStore::init(memory.clone());
    let incarnation = first
        .database_incarnation_id()
        .expect("initial current control slot should carry an incarnation");

    let reopened = super::CommitStore::open(memory)
        .expect("ordinary reopen should admit the same current control state");

    assert_eq!(
        reopened
            .database_incarnation_id()
            .expect("reopened current control slot should carry an incarnation"),
        incarnation,
    );
}

#[test]
fn database_control_rejects_zero_incarnation() {
    let store = super::CommitStore::init(test_memory(226));
    let mut control_slot = Vec::new();
    control_slot.extend_from_slice(b"ICCS");
    control_slot.push(1);
    control_slot.extend_from_slice(&[0; 16]);
    control_slot.extend_from_slice(&0_u32.to_le_bytes());
    store.set_raw_marker_bytes_for_tests(control_slot);

    let err = store
        .database_incarnation_id()
        .expect_err("zero database incarnation must fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn database_control_frame_checksum_corruption_fails_closed() {
    let memory = test_memory(230);
    let store = super::CommitStore::init(memory.clone());
    let checksum_offset = super::DATABASE_CONTROL_SLOT_FRAME_OFFSET
        + super::DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET as u64;
    let mut checksum_byte = [0_u8; 1];
    memory.read(checksum_offset, &mut checksum_byte);
    checksum_byte[0] ^= 0xff;
    memory.write(checksum_offset, &checksum_byte);

    let err = store
        .marker_is_empty()
        .expect_err("corrupt database-control checksum should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn database_control_frame_future_version_fails_closed() {
    let memory = test_memory(229);
    let store = super::CommitStore::init(memory.clone());
    let version_offset = super::DATABASE_CONTROL_SLOT_FRAME_OFFSET
        + super::DATABASE_CONTROL_SLOT_FRAME_MAGIC.len() as u64;
    memory.write(
        version_offset,
        &[super::DATABASE_CONTROL_SLOT_FRAME_VERSION.saturating_add(1)],
    );

    let err = store
        .marker_is_empty()
        .expect_err("future database-control frame must fail closed");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
}

#[test]
fn commit_control_slot_future_version_fails_closed() {
    let store = super::CommitStore::init(test_memory(228));
    let mut control_slot = super::CommitStore::encode_raw_control_slot_for_tests(Vec::new())
        .expect("current empty commit-control slot should encode");
    control_slot[4] = 2;
    store.set_raw_marker_bytes_for_tests(control_slot);

    let err = store
        .marker_is_empty()
        .expect_err("future commit-control slot must fail closed");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
}
