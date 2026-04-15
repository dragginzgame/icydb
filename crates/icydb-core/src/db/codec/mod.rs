//! Module: db::codec
//! Responsibility: db-scoped payload encode/decode policy and hash-stream helpers.
//! Does not own: generic serialization formats outside the database boundary.
//! Boundary: the only db-level layer allowed to decode persisted payload bytes directly.
//!
//! DB codec boundary for engine payload decoding/encoding policy.
//!
//! This module owns the outer persisted-row envelope plus the DB-scoped
//! bounded decode wrappers used beneath it.
//! All other DB modules must decode via codec helpers.

pub(crate) mod cursor;
mod hash_stream;
#[cfg(test)]
mod tests;

use crate::error::InternalError;
use serde::de::DeserializeOwned;
use serde_cbor::from_slice;
use std::{
    borrow::Cow,
    panic::{AssertUnwindSafe, catch_unwind},
};

pub(in crate::db) use hash_stream::{
    finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_tag_u8,
    write_hash_u32, write_hash_u64,
};

/// Max serialized bytes for a single row (protocol-level limit).
pub(crate) const MAX_ROW_BYTES: u32 = 4 * 1024 * 1024;
/// Current persisted row format version.
pub(in crate::db) const ROW_FORMAT_VERSION_CURRENT: u8 = 2;

const ROW_ENVELOPE_MAGIC: [u8; 2] = *b"IR";
const ROW_ENVELOPE_HEADER_LEN: usize = 2 + 1 + 4;

///
/// DB Codec
///
/// Database-specific decode wrappers over generic serialization helpers.
///
/// Policy lives here:
/// - payload size limits for engine storage formats
/// - error classification/origin for persisted payload failures
///
/// The row-envelope format itself is DB-owned and intentionally does not route
/// through the generic serializer.
///

/// Deserialize one persisted row payload using the DB row-size policy.
#[cfg(test)]
pub(in crate::db) fn deserialize_row<T>(bytes: &[u8]) -> Result<T, InternalError>
where
    T: DeserializeOwned,
{
    let payload = decode_row_payload_bytes(bytes)?;

    deserialize_persisted_payload(payload.as_ref(), MAX_ROW_BYTES as usize, "row")
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
    decode_bounded_persisted_cbor_payload(bytes, max_bytes, payload_label)
}

/// Decode one canonical row envelope into its owned-or-borrowed payload bytes.
pub(in crate::db) fn decode_row_payload_bytes(
    bytes: &[u8],
) -> Result<Cow<'_, [u8]>, InternalError> {
    // Phase 1: validate the fixed-width row-envelope header.
    if bytes.len() > MAX_ROW_BYTES as usize {
        return Err(InternalError::serialize_corruption(format!(
            "row decode: payload size {} exceeds limit {}",
            bytes.len(),
            MAX_ROW_BYTES
        )));
    }

    let magic = bytes
        .get(..ROW_ENVELOPE_MAGIC.len())
        .ok_or_else(|| InternalError::serialize_corruption("row decode: truncated row envelope"))?;
    if magic != ROW_ENVELOPE_MAGIC {
        return Err(InternalError::serialize_corruption(
            "row decode: invalid row envelope magic",
        ));
    }

    let format_version = *bytes.get(ROW_ENVELOPE_MAGIC.len()).ok_or_else(|| {
        InternalError::serialize_corruption("row decode: missing row format version")
    })?;
    validate_row_format_version(format_version)?;

    let payload_len_offset = ROW_ENVELOPE_MAGIC.len() + 1;
    let payload_len_bytes = bytes
        .get(payload_len_offset..payload_len_offset + 4)
        .ok_or_else(|| {
            InternalError::serialize_corruption("row decode: truncated row payload length")
        })?;
    let payload_len = usize::try_from(u32::from_be_bytes([
        payload_len_bytes[0],
        payload_len_bytes[1],
        payload_len_bytes[2],
        payload_len_bytes[3],
    ]))
    .map_err(|_| InternalError::serialize_corruption("row decode: payload length out of range"))?;

    // Phase 2: validate the declared payload span and borrow it directly.
    let payload_start = ROW_ENVELOPE_HEADER_LEN;
    let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
        InternalError::serialize_corruption("row decode: payload length overflow")
    })?;
    if payload_end != bytes.len() {
        return Err(InternalError::serialize_corruption(
            "row decode: payload length does not match row envelope",
        ));
    }

    Ok(Cow::Borrowed(&bytes[payload_start..payload_end]))
}

// Decode one DB-owned persisted CBOR payload under an explicit byte limit.
//
// This keeps the bounded persisted decode contract local to `db::codec`
// instead of routing engine-owned payloads back through the generic
// serialize facade.
fn decode_bounded_persisted_cbor_payload<T>(
    bytes: &[u8],
    max_bytes: usize,
    payload_label: &'static str,
) -> Result<T, InternalError>
where
    T: DeserializeOwned,
{
    // Phase 1: reject oversized persisted payloads before CBOR decode begins.
    if bytes.len() > max_bytes {
        return Err(InternalError::serialize_corruption(format!(
            "{payload_label} decode failed: payload size {} exceeds limit {max_bytes}",
            bytes.len(),
        )));
    }

    // Phase 2: decode under a panic boundary so persisted bytes never unwind
    // through the database runtime.
    let result = catch_unwind(AssertUnwindSafe(|| from_slice(bytes)));
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) | Err(_) => Err(InternalError::serialize_corruption(format!(
            "{payload_label} decode failed: deserialize"
        ))),
    }
}

// Validate persisted row format version against the single supported slot format.
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
pub(in crate::db) fn serialize_row_payload_with_version(
    payload: Vec<u8>,
    format_version: u8,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: validate the payload against the bounded row envelope budget.
    let total_len = ROW_ENVELOPE_HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(|| {
            InternalError::persisted_row_encode_failed("row envelope length overflow")
        })?;
    if total_len > MAX_ROW_BYTES as usize {
        return Err(InternalError::persisted_row_encode_failed(format!(
            "row envelope exceeds max size: {total_len} bytes (limit {MAX_ROW_BYTES})",
        )));
    }

    // Phase 2: write the fixed-width row envelope header and payload bytes.
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(&ROW_ENVELOPE_MAGIC);
    encoded.push(format_version);
    encoded.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| {
                InternalError::persisted_row_encode_failed("row payload exceeds u32 length")
            })?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&payload);

    Ok(encoded)
}
