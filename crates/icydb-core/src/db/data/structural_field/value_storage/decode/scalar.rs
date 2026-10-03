//! Module: data::structural_field::value_storage::decode::scalar
//! Responsibility: cursor and direct scalar decode helpers for value-storage bytes.
//! Does not own: collection traversal, local tag dispatch, or row decode.
//! Boundary: decodes scalar payloads after value-storage root/tag routing selects this lane.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{CompleteBinaryValue, TAG_BYTES, TAG_INT64, TAG_NAT64, TAG_TEXT},
    primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
    value_storage::decode::ValueStorageSlice,
};

// Decode one top-level i64 scalar without wrapping it in a runtime `Value`.
pub(super) fn decode_binary_i64_scalar(
    slice: &ValueStorageSlice<'_>,
) -> Result<i64, FieldDecodeError> {
    decode_i64_payload_bytes(slice.scalar_payload(TAG_INT64, Some(8))?)
}

// Decode one top-level u64 scalar without wrapping it in a runtime `Value`.
pub(super) fn decode_binary_u64_scalar(
    slice: &ValueStorageSlice<'_>,
) -> Result<u64, FieldDecodeError> {
    decode_u64_payload_bytes(slice.scalar_payload(TAG_NAT64, Some(8))?)
}

// Decode one top-level text scalar without allocating an owned `String`.
pub(super) fn decode_binary_text_scalar<'a>(
    slice: &ValueStorageSlice<'a>,
) -> Result<&'a str, FieldDecodeError> {
    std::str::from_utf8(slice.scalar_payload(TAG_TEXT, None)?).map_err(|_| FieldDecodeError::new())
}

// Decode one top-level blob scalar without allocating owned bytes.
pub(super) fn decode_binary_blob_scalar<'a>(
    slice: &ValueStorageSlice<'a>,
) -> Result<&'a [u8], FieldDecodeError> {
    slice.scalar_payload(TAG_BYTES, None)
}

// Borrow the payload bytes for one top-level text scalar without validating
// UTF-8. This is only for byte-key comparisons where the caller already owns a
// valid UTF-8 query segment and only needs exact byte equality.
pub(super) fn decode_binary_text_payload_bytes_if_text(
    raw_bytes: &[u8],
) -> Result<Option<&[u8]>, FieldDecodeError> {
    let root = CompleteBinaryValue::parse(raw_bytes)?;
    if root.tag() != TAG_TEXT {
        return Ok(None);
    }

    root.scalar_payload().map(Some)
}
