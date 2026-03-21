//! Module: db::codec::tests
//! Responsibility: module-local ownership and contracts for db::codec::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    error::{ErrorClass, ErrorOrigin},
    serialize::{SerializeError, serialize},
};

use super::{
    MAX_ROW_BYTES, ROW_FORMAT_VERSION_CURRENT, deserialize_persisted_payload,
    deserialize_protocol_payload, deserialize_row, map_deserialize_error, serialize_row_payload,
    serialize_row_payload_with_version,
};

#[test]
fn map_deserialize_error_classifies_size_limit_as_corruption() {
    let err = map_deserialize_error(
        SerializeError::DeserializeSizeLimitExceeded {
            len: 32,
            max_bytes: 16,
        },
        "row",
    );

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert!(
        err.message.contains("row decode failed"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message.contains("payload size 32 exceeds limit 16"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn deserialize_row_oversized_payload_fails_as_corruption() {
    let bytes = vec![0u8; MAX_ROW_BYTES as usize + 1];
    let err = deserialize_row::<u8>(&bytes).expect_err("oversized persisted row payload must fail");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert!(
        err.message.contains("payload size"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn deserialize_row_current_version_succeeds() {
    let payload = serialize(&42_u32).expect("test payload encode should succeed");
    let bytes =
        serialize_row_payload(payload).expect("row envelope encode at current version should work");
    let decoded =
        deserialize_row::<u32>(&bytes).expect("current row format version decode should succeed");

    assert_eq!(decoded, 42_u32);
}

#[test]
fn deserialize_row_future_version_fails_closed() {
    let payload = serialize(&7_u8).expect("test payload encode should succeed");
    let future_version = ROW_FORMAT_VERSION_CURRENT.saturating_add(1);
    let bytes = serialize_row_payload_with_version(payload, future_version)
        .expect("future-version envelope encode should succeed for decode test");
    let err =
        deserialize_row::<u8>(&bytes).expect_err("future row format versions must fail closed");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
}

#[test]
fn deserialize_row_older_version_fails_closed() {
    let payload = serialize(&9_u8).expect("test payload encode should succeed");
    let older_version = ROW_FORMAT_VERSION_CURRENT.saturating_sub(1);
    let bytes = serialize_row_payload_with_version(payload, older_version)
        .expect("older-version envelope encode should succeed for decode test");
    let err = deserialize_row::<u8>(&bytes).expect_err("older row format versions must fail");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
}

#[test]
fn map_deserialize_error_uses_stable_kind_labels() {
    let deserialize_err = map_deserialize_error(
        SerializeError::Deserialize("backend text changed".into()),
        "row",
    );
    assert!(
        deserialize_err.message.ends_with(": deserialize"),
        "deserialize mapping should not depend on backend error text: {deserialize_err:?}"
    );

    let serialize_err = map_deserialize_error(
        SerializeError::Serialize("unexpected backend text".into()),
        "row",
    );
    assert!(
        serialize_err.message.ends_with(": serialize"),
        "serialize mapping should not depend on backend error text: {serialize_err:?}"
    );
}

#[test]
fn deserialize_protocol_payload_preserves_size_limit_error_for_untrusted_input() {
    let oversized = vec![0u8; 33];
    let err = deserialize_protocol_payload::<u8>(&oversized, 32)
        .expect_err("protocol payload size-limit failures must stay at serialize boundary");

    assert!(
        matches!(
            err,
            SerializeError::DeserializeSizeLimitExceeded {
                len: 33,
                max_bytes: 32
            }
        ),
        "protocol decode should preserve deserialize size-limit context: {err:?}"
    );
}

#[test]
fn shared_deserialize_failures_are_classified_by_decode_boundary_context() {
    let malformed = [0xFF_u8];

    let persisted_err = deserialize_persisted_payload::<u8>(&malformed, 64, "row")
        .expect_err("persisted malformed payload must fail closed as corruption");
    assert_eq!(persisted_err.class, ErrorClass::Corruption);
    assert_eq!(persisted_err.origin, ErrorOrigin::Serialize);
    assert!(
        persisted_err.message.ends_with(": deserialize"),
        "persisted decode should map malformed payloads via stable deserialize label: {persisted_err:?}",
    );

    let protocol_err = deserialize_protocol_payload::<u8>(&malformed, 64)
        .expect_err("protocol malformed payload must stay at serialize boundary");
    assert!(
        matches!(protocol_err, SerializeError::Deserialize(_)),
        "protocol decode should preserve deserialize failure kind without corruption remap: {protocol_err:?}",
    );
}
