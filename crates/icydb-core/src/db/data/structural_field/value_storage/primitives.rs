//! Module: data::structural_field::value_storage::primitives
//! Responsibility: primitive decode/split helpers for structural value-storage bytes.
//! Does not own: runtime `Value` materialization, map/list traversal policy, or row decode.
//! Boundary: exposes bounded scalar and tuple helpers to value-storage decode paths.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        TAG_BYTES, TAG_INT64, TAG_LIST, TAG_NAT64, TAG_TEXT,
        decode_text_scalar_bytes as decode_binary_text_scalar_bytes, parse_binary_head,
        payload_bytes as binary_payload_bytes, skip_binary_value,
    },
    primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
    value_storage::skip::skip_value_storage_binary_value,
};

type BinarySkipFn = fn(&[u8], usize) -> Result<usize, FieldDecodeError>;

// === Scalar Decode Helpers (non-parsed) ===

// Validate one complete generic Structural Binary scalar payload and return the
// byte range metadata needed by the concrete scalar decoder.
fn parse_required_binary_payload(
    raw_bytes: &[u8],
    expected_tag: u8,
    expected_len: Option<u32>,
) -> Result<(u32, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != expected_tag || expected_len.is_some_and(|v| len != v) {
        return Err(FieldDecodeError::new());
    }

    Ok((len, payload_start))
}

// Decode one required binary bytes payload.
pub(super) fn decode_binary_required_bytes(raw_bytes: &[u8]) -> Result<&[u8], FieldDecodeError> {
    let (len, payload_start) = parse_required_binary_payload(raw_bytes, TAG_BYTES, None)?;

    binary_payload_bytes(raw_bytes, len, payload_start)
}

// Decode one required binary text payload.
pub(super) fn decode_binary_required_text(raw_bytes: &[u8]) -> Result<&str, FieldDecodeError> {
    let (len, payload_start) = parse_required_binary_payload(raw_bytes, TAG_TEXT, None)?;

    decode_binary_text_scalar_bytes(raw_bytes, len, payload_start)
}

// Decode one required binary i64 payload.
pub(super) fn decode_binary_required_i64(raw_bytes: &[u8]) -> Result<i64, FieldDecodeError> {
    let (len, payload_start) = parse_required_binary_payload(raw_bytes, TAG_INT64, Some(8))?;

    decode_i64_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)
}

// Decode one required binary u64 payload.
pub(super) fn decode_binary_required_u64(raw_bytes: &[u8]) -> Result<u64, FieldDecodeError> {
    let (len, payload_start) = parse_required_binary_payload(raw_bytes, TAG_NAT64, Some(8))?;

    decode_u64_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)
}

// === Tuple Splitting Helpers ===

// Split a two-item tuple whose items are generic Structural Binary values
// without staging borrowed item slices in a heap-backed Vec.
pub(super) fn split_binary_tuple_2(raw_bytes: &[u8]) -> Result<[&[u8]; 2], FieldDecodeError> {
    split_tuple_2(raw_bytes, skip_binary_value)
}

// Split a three-item tuple whose items are nested `Value` envelopes without
// staging borrowed item slices in a heap-backed Vec.
pub(super) fn split_value_storage_tuple_3(
    raw_bytes: &[u8],
) -> Result<[&[u8]; 3], FieldDecodeError> {
    split_tuple_3(raw_bytes, skip_value_storage_binary_value)
}

// Shared fixed-arity tuple head validation. Error wording intentionally
// matches the Vec-based splitter exactly.
fn parse_fixed_tuple_head(raw_bytes: &[u8], expected_len: u32) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST || len != expected_len {
        return Err(FieldDecodeError::new());
    }

    Ok(payload_start)
}

// Split a two-item tuple with the caller-selected item skip authority.
fn split_tuple_2(
    raw_bytes: &[u8],
    skip_item: BinarySkipFn,
) -> Result<[&[u8]; 2], FieldDecodeError> {
    let mut cursor = parse_fixed_tuple_head(raw_bytes, 2)?;

    let first_start = cursor;
    cursor = skip_item(raw_bytes, cursor)?;
    let first = &raw_bytes[first_start..cursor];

    let second_start = cursor;
    cursor = skip_item(raw_bytes, cursor)?;
    let second = &raw_bytes[second_start..cursor];

    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok([first, second])
}

// Split a three-item tuple with the caller-selected item skip authority.
fn split_tuple_3(
    raw_bytes: &[u8],
    skip_item: BinarySkipFn,
) -> Result<[&[u8]; 3], FieldDecodeError> {
    let mut cursor = parse_fixed_tuple_head(raw_bytes, 3)?;

    let first_start = cursor;
    cursor = skip_item(raw_bytes, cursor)?;
    let first = &raw_bytes[first_start..cursor];

    let second_start = cursor;
    cursor = skip_item(raw_bytes, cursor)?;
    let second = &raw_bytes[second_start..cursor];

    let third_start = cursor;
    cursor = skip_item(raw_bytes, cursor)?;
    let third = &raw_bytes[third_start..cursor];

    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok([first, second, third])
}

// === Payload Extraction Helpers ===

// Extract the single nested payload carried by one local `Value` binary tag.
pub(super) fn decode_value_storage_binary_payload(
    raw_bytes: &[u8],
    expected_tag: u8,
) -> Result<&[u8], FieldDecodeError> {
    let Some((&tag, _)) = raw_bytes.split_first() else {
        return Err(FieldDecodeError::new());
    };
    if tag != expected_tag {
        return Err(FieldDecodeError::new());
    }

    let payload_end = skip_value_storage_binary_value(raw_bytes, 1)?;
    if payload_end != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    raw_bytes
        .get(1..payload_end)
        .ok_or_else(FieldDecodeError::new)
}
