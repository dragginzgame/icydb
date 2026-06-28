use super::{RawCommitMarker, marker_envelope::encode_commit_marker_bytes};
use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::marker::{
            COMMIT_MARKER_FORMAT_VERSION_CURRENT, CommitMarker, CommitRowOp, MAX_COMMIT_BYTES,
            decode_commit_marker_payload, encode_commit_marker_payload,
        },
        data::{DecodedDataStoreKey, RawDataStoreKey},
        journal::{JournalBatch, JournalRecord, JournalSequence},
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    types::EntityTag,
};
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

// Encode one single-row marker payload directly from raw row-op fields so
// corruption tests can exercise malformed persisted keys that no longer fit
// through the typed `CommitRowOp` constructor.
fn encode_test_single_row_payload_from_fields(
    entity_path: &str,
    key_bytes: &[u8],
    before: Option<&[u8]>,
    after: Option<&[u8]>,
    schema_fingerprint: [u8; 16],
) -> Vec<u8> {
    let mut flags = 0u8;
    if before.is_some() {
        flags |= 0b0000_0001;
    }
    if after.is_some() {
        flags |= 0b0000_0010;
    }

    encode_test_single_row_payload_with_flags(
        entity_path,
        key_bytes,
        flags,
        before,
        after,
        schema_fingerprint,
    )
}

fn encode_test_single_row_payload_with_flags(
    entity_path: &str,
    key_bytes: &[u8],
    flags: u8,
    before: Option<&[u8]>,
    after: Option<&[u8]>,
    schema_fingerprint: [u8; 16],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&[0u8; 16]);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&(u32::try_from(entity_path.len()).expect("len fits")).to_le_bytes());
    payload.extend_from_slice(entity_path.as_bytes());
    payload.extend_from_slice(&(u32::try_from(key_bytes.len()).expect("len fits")).to_le_bytes());
    payload.extend_from_slice(key_bytes);

    payload.push(flags);

    if let Some(before) = before {
        payload.extend_from_slice(&(u32::try_from(before.len()).expect("len fits")).to_le_bytes());
        payload.extend_from_slice(before);
    }
    if let Some(after) = after {
        payload.extend_from_slice(&(u32::try_from(after.len()).expect("len fits")).to_le_bytes());
        payload.extend_from_slice(after);
    }

    payload.extend_from_slice(&schema_fingerprint);
    payload.extend_from_slice(&0u32.to_le_bytes());

    encode_commit_marker_bytes(COMMIT_MARKER_FORMAT_VERSION_CURRENT, &payload)
        .expect("test marker envelope encode should succeed")
}

#[test]
fn commit_control_slot_rejects_corrupt_magic() {
    let mut store = super::CommitStore::init(test_memory(233));
    let mut malformed = Vec::new();
    malformed.extend_from_slice(b"XMCS");
    malformed.push(1);
    malformed.extend_from_slice(&0u32.to_le_bytes());
    store.set_raw_marker_bytes_for_tests(malformed);

    let err = store
        .marker_is_empty()
        .expect_err("corrupt control-slot magic should fail closed");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
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
        row_ops: Vec::new(),
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
fn commit_marker_payload_decode_allows_bytes_over_row_limit() {
    let marker = CommitMarker {
        id: [0xAA; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_store_key(0),
            Some(vec![0x11; MAX_ROW_BYTES as usize + 1]),
            None,
            [0x22; 16],
        )],
        journal_batches: Vec::new(),
    };

    let bytes =
        encode_commit_marker_payload(&marker).expect("payload encode should succeed for test");
    let decoded = decode_commit_marker_payload(&bytes)
        .expect("payload decode should allow large row bytes before shape validation");

    assert_eq!(decoded.row_ops.len(), 1);
    assert_eq!(
        decoded.row_ops[0]
            .before
            .as_ref()
            .expect("before payload should remain present")
            .len(),
        MAX_ROW_BYTES as usize + 1
    );
}

#[test]
fn commit_marker_current_version_round_trip_succeeds() {
    let marker = CommitMarker {
        id: [9u8; 16],
        row_ops: Vec::new(),
        journal_batches: Vec::new(),
    };
    let encoded = encode_test_marker_payload(&marker);
    let decoded = RawCommitMarker(encoded)
        .try_decode()
        .expect("current-version marker envelope should decode")
        .expect("marker payload should be present");

    assert_eq!(decoded.id, marker.id);
    assert_eq!(decoded.row_ops.len(), 0);
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
    let marker = CommitMarker::from_parts(marker_id, Vec::new(), vec![journal_batch.clone()])
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

    let err = CommitMarker::from_parts(marker_id, Vec::new(), vec![journal_batch])
        .expect_err("journal batch must be bound to enclosing marker id");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_future_version_fails_closed() {
    let marker = CommitMarker {
        id: [6u8; 16],
        row_ops: Vec::new(),
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
fn commit_marker_older_version_fails_closed() {
    let marker = CommitMarker {
        id: [5u8; 16],
        row_ops: Vec::new(),
        journal_batches: Vec::new(),
    };
    let marker_payload = encode_commit_marker_payload(&marker)
        .expect("marker payload encode for old-version test should work");
    let older_version = COMMIT_MARKER_FORMAT_VERSION_CURRENT.saturating_sub(1);
    let encoded = encode_commit_marker_bytes(older_version, &marker_payload)
        .expect("older-version marker envelope encode should succeed");
    let err = RawCommitMarker(encoded)
        .try_decode()
        .expect_err("older marker versions must fail closed");

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
fn direct_multi_row_control_slot_rejects_oversized_payload_before_persist() {
    let oversized_after = vec![0u8; MAX_COMMIT_BYTES as usize + 1];
    let marker = CommitMarker {
        id: [2u8; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_store_key(1),
            None,
            Some(oversized_after),
            [0u8; 16],
        )],
        journal_batches: Vec::new(),
    };

    let err = super::CommitStore::encode_raw_direct_control_slot_for_tests(&marker)
        .expect_err("direct control-slot encoder must reject oversized marker payload");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_rejects_unknown_row_op_flags() {
    let bytes = encode_test_single_row_payload_with_flags(
        "test::Entity",
        raw_data_store_key(9).as_bytes(),
        0b1000_0000,
        None,
        None,
        [0u8; 16],
    );
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("unknown row-op flags should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_rejects_row_op_without_before_or_after() {
    let marker = CommitMarker {
        id: [1u8; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_store_key(9),
            None,
            None,
            [0u8; 16],
        )],
        journal_batches: Vec::new(),
    };

    let bytes = encode_test_marker_payload(&marker);
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op without before/after should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn direct_multi_row_control_slot_rejects_row_op_without_before_or_after_before_persist() {
    let marker = CommitMarker {
        id: [0x44; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_store_key(9),
            None,
            None,
            [0u8; 16],
        )],
        journal_batches: Vec::new(),
    };

    let err = super::CommitStore::encode_raw_direct_control_slot_for_tests(&marker)
        .expect_err("direct control-slot encoder must reject invalid marker shape");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn direct_single_row_control_slot_rejects_invalid_row_op_before_persist() {
    let row_op = CommitRowOp::new("test::Entity", raw_data_store_key(9), None, None, [0u8; 16]);

    let err = super::CommitStore::encode_raw_single_row_control_slot_for_tests([0x55; 16], &row_op)
        .expect_err("single-row direct encoder must reject invalid row shape");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn clear_verified_rejects_malformed_control_slot() {
    let mut store = super::CommitStore::init(test_memory(232));
    let mut malformed = Vec::new();
    malformed.extend_from_slice(b"CMCS");
    malformed.push(1);
    malformed.extend_from_slice(&1u32.to_le_bytes());
    store.set_raw_marker_bytes_for_tests(malformed.clone());

    let err = store
        .clear_verified()
        .expect_err("malformed control slot should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(store.cell.get().as_bytes(), malformed.as_slice());
}

#[test]
fn commit_marker_rejects_row_op_with_empty_entity_path() {
    let marker = CommitMarker {
        id: [3u8; 16],
        row_ops: vec![CommitRowOp::new(
            "",
            raw_data_store_key(9),
            Some(vec![1u8]),
            None,
            [0u8; 16],
        )],
        journal_batches: Vec::new(),
    };

    let bytes = encode_test_marker_payload(&marker);
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op with empty entity path should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_rejects_row_op_with_invalid_key_length() {
    let bytes = encode_test_single_row_payload_from_fields(
        "test::Entity",
        &[9u8],
        Some(&[1u8]),
        None,
        [0u8; 16],
    );
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op with invalid key length should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_rejects_row_op_with_invalid_key_shape() {
    let mut malformed_key = vec![0u8; RawDataStoreKey::MAX_STORED_SIZE_USIZE];
    malformed_key[RawDataStoreKey::ENTITY_TAG_SIZE_USIZE] = 0xFF;

    let bytes = encode_test_single_row_payload_from_fields(
        "test::Entity",
        malformed_key.as_slice(),
        Some(&[1u8]),
        None,
        [0u8; 16],
    );
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op with invalid key shape should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn commit_marker_rejects_row_op_with_oversized_payload() {
    let marker = CommitMarker {
        id: [6u8; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_store_key(9),
            Some(vec![0u8; MAX_ROW_BYTES as usize + 1]),
            None,
            [0u8; 16],
        )],
        journal_batches: Vec::new(),
    };

    let bytes = encode_test_marker_payload(&marker);
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op with oversized payload should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}
