use super::{RawCommitMarker, encode_commit_marker_bytes, serialize_commit_marker};
use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::{
            COMMIT_MARKER_FORMAT_VERSION_CURRENT, CommitMarker, CommitRowOp, MAX_COMMIT_BYTES,
            decode_commit_marker_payload, encode_commit_marker_payload,
        },
        data::{DataKey, RawDataKey},
    },
    error::{ErrorClass, ErrorOrigin},
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

// Materialize one canonical fixed-width raw data key for marker tests.
fn raw_data_key(fill: u8) -> RawDataKey {
    DataKey::try_from_field_value(EntityTag::new(1), &u64::from(fill))
        .expect("test key should encode")
        .to_raw()
        .expect("test key should materialize")
}

// Encode one single-row marker payload directly from raw row-op fields so
// corruption tests can exercise malformed persisted keys that no longer fit
// through the typed `CommitRowOp` constructor.
fn encode_test_single_row_payload_from_parts(
    entity_path: &str,
    key_bytes: &[u8],
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

    let mut flags = 0u8;
    if before.is_some() {
        flags |= 0b0000_0001;
    }
    if after.is_some() {
        flags |= 0b0000_0010;
    }
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

    encode_commit_marker_bytes(COMMIT_MARKER_FORMAT_VERSION_CURRENT, &payload)
        .expect("test marker envelope encode should succeed")
}

#[test]
fn commit_marker_rejects_trailing_payload_bytes() {
    let marker = CommitMarker {
        id: [0u8; 16],
        row_ops: Vec::new(),
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
            raw_data_key(0),
            Some(vec![0x11; MAX_ROW_BYTES as usize + 1]),
            None,
            [0x22; 16],
        )],
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
    };
    let encoded = serialize_commit_marker(&marker)
        .expect("current-version marker envelope encode should succeed");
    let decoded = RawCommitMarker(encoded)
        .try_decode()
        .expect("current-version marker envelope should decode")
        .expect("marker payload should be present");

    assert_eq!(decoded.id, marker.id);
    assert_eq!(decoded.row_ops.len(), 0);
}

#[test]
fn commit_marker_future_version_fails_closed() {
    let marker = CommitMarker {
        id: [6u8; 16],
        row_ops: Vec::new(),
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
    assert!(
        err.message.contains("commit marker exceeds max size"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn commit_marker_rejects_oversized_payload_before_persist() {
    let oversized_after = vec![0u8; MAX_COMMIT_BYTES as usize + 1];
    let marker = CommitMarker {
        id: [2u8; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_key(1),
            None,
            Some(oversized_after),
            [0u8; 16],
        )],
    };

    let err = RawCommitMarker::try_from_marker(&marker)
        .expect_err("oversized marker payload must be rejected before persist");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains("commit marker exceeds max size"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn commit_marker_rejects_row_op_without_before_or_after() {
    let marker = CommitMarker {
        id: [1u8; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_key(9),
            None,
            None,
            [0u8; 16],
        )],
    };

    let bytes = encode_test_marker_payload(&marker);
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op without before/after should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("row op has neither before nor after payload"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn commit_marker_rejects_row_op_with_empty_entity_path() {
    let marker = CommitMarker {
        id: [3u8; 16],
        row_ops: vec![CommitRowOp::new(
            "",
            raw_data_key(9),
            Some(vec![1u8]),
            None,
            [0u8; 16],
        )],
    };

    let bytes = encode_test_marker_payload(&marker);
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op with empty entity path should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains("row op has empty entity_path"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn commit_marker_rejects_row_op_with_invalid_key_length() {
    let bytes = encode_test_single_row_payload_from_parts(
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
    assert!(
        err.message.contains("invalid length"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn commit_marker_rejects_row_op_with_invalid_key_shape() {
    let mut malformed_key = vec![0u8; DataKey::STORED_SIZE_USIZE];
    malformed_key[DataKey::ENTITY_TAG_SIZE_USIZE] = 0xFF;

    let bytes = encode_test_single_row_payload_from_parts(
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
    assert!(
        err.message.contains("data key corrupted"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message.contains("invalid primary key"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn commit_marker_rejects_row_op_with_oversized_payload() {
    let marker = CommitMarker {
        id: [6u8; 16],
        row_ops: vec![CommitRowOp::new(
            "test::Entity",
            raw_data_key(9),
            Some(vec![0u8; MAX_ROW_BYTES as usize + 1]),
            None,
            [0u8; 16],
        )],
    };

    let bytes = encode_test_marker_payload(&marker);
    let err = RawCommitMarker(bytes)
        .try_decode()
        .expect_err("row op with oversized payload should be rejected");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains("payload exceeds max size"),
        "unexpected error: {err:?}"
    );
}
