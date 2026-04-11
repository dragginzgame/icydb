//! Module: db::codec
//! Responsibility: db-scoped payload encode/decode policy and hash-stream helpers.
//! Does not own: generic serialization formats outside the database boundary.
//! Boundary: the only db-level layer allowed to decode persisted payload bytes directly.
//!
//! DB codec boundary for engine payload decoding/encoding policy.
//!
//! This module is the only DB-level boundary allowed to call
//! `crate::serialize::deserialize_bounded` directly.
//! All other DB modules must decode via codec helpers.

pub(crate) mod cursor;
mod hash_stream;
#[cfg(test)]
mod tests;

use crate::{
    error::InternalError,
    serialize::{SerializeError, deserialize_bounded, serialize},
};
use serde::de::DeserializeOwned;

#[cfg(test)]
pub(in crate::db) use hash_stream::new_hash_sha256;
pub(in crate::db) use hash_stream::{
    finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_tag_u8,
    write_hash_u32, write_hash_u64,
};

/// Max serialized bytes for a single row (protocol-level limit).
pub(crate) const MAX_ROW_BYTES: u32 = 4 * 1024 * 1024;
/// Current persisted row format version.
pub(in crate::db) const ROW_FORMAT_VERSION_CURRENT: u8 = 1;

// Persisted row envelope payload: (format_version, encoded_row_bytes).
#[cfg(test)]
type PersistedRowEnvelope = (u8, Vec<u8>);

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
#[cfg(test)]
pub(in crate::db) fn deserialize_row<T>(bytes: &[u8]) -> Result<T, InternalError>
where
    T: DeserializeOwned,
{
    let (format_version, payload) =
        match deserialize_bounded::<PersistedRowEnvelope>(bytes, MAX_ROW_BYTES as usize) {
            Ok(envelope) => envelope,
            Err(SerializeError::DeserializeSizeLimitExceeded { len, max_bytes }) => {
                return Err(InternalError::serialize_payload_decode_failed(
                    SerializeError::DeserializeSizeLimitExceeded { len, max_bytes },
                    "row",
                ));
            }
            Err(source) => {
                return Err(InternalError::serialize_payload_decode_failed(
                    source, "row",
                ));
            }
        };

    validate_row_format_version(format_version)?;

    deserialize_persisted_payload(&payload, MAX_ROW_BYTES as usize, "row")
}

/// Wrap an already-serialized entity payload in the canonical persisted row envelope.
pub(in crate::db) fn serialize_row_payload(payload: Vec<u8>) -> Result<Vec<u8>, InternalError> {
    serialize_row_payload_with_version(payload, ROW_FORMAT_VERSION_CURRENT)
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
        .map_err(|source| InternalError::serialize_payload_decode_failed(source, payload_label))
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

// Validate persisted row format version against the single supported slot format.
#[cfg(test)]
fn validate_row_format_version(format_version: u8) -> Result<(), InternalError> {
    if format_version == ROW_FORMAT_VERSION_CURRENT {
        return Ok(());
    }

    Err(InternalError::serialize_incompatible_persisted_format(
        format!(
            "row format version {format_version} is unsupported by runtime version {ROW_FORMAT_VERSION_CURRENT}",
        ),
    ))
}

// Encode one persisted row envelope at an explicit format version.
fn serialize_row_payload_with_version(
    payload: Vec<u8>,
    format_version: u8,
) -> Result<Vec<u8>, InternalError> {
    serialize(&(format_version, payload)).map_err(InternalError::persisted_row_encode_failed)
}
