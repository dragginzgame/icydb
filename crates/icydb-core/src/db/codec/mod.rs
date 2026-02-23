pub(crate) mod cursor;

use crate::{
    db::data::MAX_ROW_BYTES,
    error::InternalError,
    serialize::{SerializeError, SerializeErrorKind, deserialize_bounded},
};
use serde::de::DeserializeOwned;

///
/// DB Codec
///
/// Database-specific decode wrappers over generic serialization helpers.
///
/// Policy lives here:
/// - payload size limits for engine storage formats
/// - error classification/origin for persisted payload failures
///
/// Format logic lives in `crate::serialize`.
///

/// Deserialize one persisted row payload using the DB row-size policy.
pub(in crate::db) fn deserialize_row<T>(bytes: &[u8]) -> Result<T, InternalError>
where
    T: DeserializeOwned,
{
    deserialize_persisted_payload(bytes, MAX_ROW_BYTES as usize, "row")
}

/// Deserialize one DB-owned persisted payload under an explicit size policy.
///
/// This is the canonical DB boundary for persisted payload decoding.
/// Engine modules should use this helper instead of calling
/// `serialize::deserialize_bounded` directly.
pub(in crate::db) fn deserialize_persisted_payload<T>(
    bytes: &[u8],
    max_bytes: usize,
    payload_label: &'static str,
) -> Result<T, InternalError>
where
    T: DeserializeOwned,
{
    deserialize_bounded(bytes, max_bytes)
        .map_err(|source| map_deserialize_error(source, payload_label))
}

// Convert format-level deserialize errors into DB engine classification.
fn map_deserialize_error(source: SerializeError, payload_label: &'static str) -> InternalError {
    match source {
        // DB codec only decodes engine-owned persisted payloads.
        // Size-limit breaches indicate persisted bytes violate DB storage policy.
        SerializeError::DeserializeSizeLimitExceeded { len, max_bytes } => {
            InternalError::serialize_corruption(format!(
                "{payload_label} decode failed: payload size {len} exceeds limit {max_bytes}"
            ))
        }
        SerializeError::Deserialize(_) => InternalError::serialize_corruption(format!(
            "{payload_label} decode failed: {}",
            SerializeErrorKind::Deserialize
        )),
        SerializeError::Serialize(_) => InternalError::serialize_corruption(format!(
            "{payload_label} decode failed: {}",
            SerializeErrorKind::Serialize
        )),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::data::MAX_ROW_BYTES,
        error::{ErrorClass, ErrorOrigin},
        serialize::SerializeError,
    };

    use super::{deserialize_row, map_deserialize_error};

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
        let err =
            deserialize_row::<u8>(&bytes).expect_err("oversized persisted row payload must fail");

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
}
