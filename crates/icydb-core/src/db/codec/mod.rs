//! Module: db::codec
//! Responsibility: db-scoped payload encode/decode policy and hash-stream helpers.
//! Does not own: generic serialization formats outside the database boundary.
//! Boundary: the only db-level layer allowed to decode persisted payload bytes directly.

mod hash_stream;
pub(in crate::db) mod hex;

use crate::{db::schema::RowLayoutVersion, error::InternalError};
use std::borrow::Cow;

pub(in crate::db) use hash_stream::{
    finalize_hash_sha256, new_hash_sha256, new_hash_sha256_prefixed, write_hash_len_u32,
    write_hash_str_u32, write_hash_tag_u8, write_hash_u32, write_hash_u64,
};

/// Max serialized bytes for a single row (protocol-level limit).
pub(crate) const MAX_ROW_BYTES: u32 = 4 * 1024 * 1024;
/// Current persisted row format version.
pub(in crate::db) const ROW_FORMAT_VERSION_CURRENT: u8 = 1;

const ROW_ENVELOPE_MAGIC: [u8; 2] = *b"IY";
const ROW_ENVELOPE_HEADER_LEN: usize = 2 + 1 + 4 + 4;

///
/// DecodedRowPayload
///
/// Borrowed current-form row payload paired with the physical layout identity
/// that must govern exact slot-count validation.
///

pub(in crate::db) struct DecodedRowPayload<'a> {
    layout_version: RowLayoutVersion,
    payload: Cow<'a, [u8]>,
}

impl<'a> DecodedRowPayload<'a> {
    /// Return the persisted physical row-layout identity.
    #[must_use]
    pub(in crate::db) const fn layout_version(&self) -> RowLayoutVersion {
        self.layout_version
    }

    /// Consume the envelope into its borrowed canonical slot container.
    #[must_use]
    pub(in crate::db) fn into_payload(self) -> Cow<'a, [u8]> {
        self.payload
    }

    /// Borrow the canonical slot container.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

/// Wrap an already-serialized entity payload in the canonical persisted row envelope.
pub(in crate::db) fn serialize_row_payload(
    layout_version: RowLayoutVersion,
    payload: Vec<u8>,
) -> Result<Vec<u8>, InternalError> {
    serialize_row_payload_with_version(layout_version, payload, ROW_FORMAT_VERSION_CURRENT)
}

/// Decode one canonical row envelope into borrowed payload bytes.
///
/// Enforces the DB row-envelope budget, magic bytes, format version, and
/// declared payload length before returning the payload slice.
pub(in crate::db) fn decode_row_payload_bytes(
    bytes: &[u8],
) -> Result<DecodedRowPayload<'_>, InternalError> {
    // Phase 1: validate the fixed-width row-envelope header.
    if bytes.len() > MAX_ROW_BYTES as usize {
        return Err(InternalError::persisted_row_decode_corruption());
    }

    let magic = bytes
        .get(..ROW_ENVELOPE_MAGIC.len())
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    if magic != ROW_ENVELOPE_MAGIC {
        return Err(InternalError::persisted_row_decode_corruption());
    }

    let format_version = *bytes
        .get(ROW_ENVELOPE_MAGIC.len())
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    validate_row_format_version(format_version)?;

    let layout_version_offset = ROW_ENVELOPE_MAGIC.len() + 1;
    let layout_version_bytes = bytes
        .get(layout_version_offset..layout_version_offset + 4)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let layout_version = RowLayoutVersion::new(u32::from_be_bytes([
        layout_version_bytes[0],
        layout_version_bytes[1],
        layout_version_bytes[2],
        layout_version_bytes[3],
    ]))
    .ok_or_else(InternalError::persisted_row_decode_corruption)?;

    let payload_len_offset = layout_version_offset + 4;
    let payload_len_bytes = bytes
        .get(payload_len_offset..payload_len_offset + 4)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let payload_len = usize::try_from(u32::from_be_bytes([
        payload_len_bytes[0],
        payload_len_bytes[1],
        payload_len_bytes[2],
        payload_len_bytes[3],
    ]))
    .map_err(|_| InternalError::persisted_row_decode_corruption())?;

    // Phase 2: validate the declared payload span and borrow it directly.
    let payload_start = ROW_ENVELOPE_HEADER_LEN;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    if payload_end != bytes.len() {
        return Err(InternalError::persisted_row_decode_corruption());
    }

    Ok(DecodedRowPayload {
        layout_version,
        payload: Cow::Borrowed(&bytes[payload_start..payload_end]),
    })
}

// Validate persisted row format version against the single supported slot format.
fn validate_row_format_version(format_version: u8) -> Result<(), InternalError> {
    if format_version == ROW_FORMAT_VERSION_CURRENT {
        return Ok(());
    }

    Err(InternalError::serialize_incompatible_persisted_format())
}

/// Encode one persisted row envelope at an explicit format version.
///
/// The version parameter is intentionally exposed inside the DB boundary so
/// the current writer and malformed-envelope tests share one bounded encoder.
pub(in crate::db) fn serialize_row_payload_with_version(
    layout_version: RowLayoutVersion,
    payload: Vec<u8>,
    format_version: u8,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: validate the payload against the bounded row envelope budget.
    let total_len = ROW_ENVELOPE_HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(InternalError::persisted_row_encode_internal)?;
    if total_len > MAX_ROW_BYTES as usize {
        return Err(InternalError::persisted_row_encode_internal());
    }

    // Phase 2: write the fixed-width row envelope header and payload bytes.
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(&ROW_ENVELOPE_MAGIC);
    encoded.push(format_version);
    encoded.extend_from_slice(&layout_version.get().to_be_bytes());
    encoded.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| InternalError::persisted_row_encode_internal())?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&payload);

    Ok(encoded)
}
