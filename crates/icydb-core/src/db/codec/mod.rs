//! DB codec boundary for engine payload decoding/encoding policy.
//!
//! This module is the only DB-level boundary allowed to call
//! `crate::serialize::deserialize_bounded` directly.
//! All other DB modules must decode via codec helpers.

pub(crate) mod cursor;
#[cfg(test)]
mod tests;

use crate::{
    error::InternalError,
    serialize::{SerializeError, SerializeErrorKind, deserialize_bounded},
};
use serde::de::DeserializeOwned;

/// Max serialized bytes for a single row (protocol-level limit).
pub(crate) const MAX_ROW_BYTES: u32 = 4 * 1024 * 1024;

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

/// Deserialize one non-persisted DB protocol payload under an explicit size policy.
///
/// This helper is for bounded decode of transport/user-facing DB payloads
/// (for example continuation tokens), not stable-memory persisted rows.
pub(in crate::db) fn deserialize_protocol_payload<T>(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<T, SerializeError>
where
    T: DeserializeOwned,
{
    deserialize_bounded(bytes, max_bytes)
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
