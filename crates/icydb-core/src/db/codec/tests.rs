use crate::{
    error::{ErrorClass, ErrorOrigin},
    serialize::SerializeError,
};

use super::{MAX_ROW_BYTES, deserialize_row, map_deserialize_error};

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
