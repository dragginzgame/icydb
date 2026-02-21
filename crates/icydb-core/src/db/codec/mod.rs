pub(crate) mod cursor;

use crate::{
    db::data::MAX_ROW_BYTES,
    error::InternalError,
    serialize::{SerializeError, deserialize_bounded},
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
    deserialize_with_limit(bytes, MAX_ROW_BYTES as usize, "row")
}

// Shared bounded decode wrapper for DB-owned payload policy.
fn deserialize_with_limit<T>(
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
    InternalError::serialize_corruption(format!("{payload_label} decode failed: {source}"))
}
