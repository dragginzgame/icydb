//! Module: data::structural_field::value_storage::walk
//! Responsibility: decode-oriented value-storage collection materialization.
//! Does not own: scalar decode, runtime row policy, or value-storage encoding.
//! Boundary: advances nested decode cursors for recursive `Value` materialization.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{TAG_LIST, TAG_MAP, parse_binary_head},
};
use crate::value::Value;

// Alias the cursor-returning decoder used by single-pass recursive collection
// materialization.
type ValueBinaryDecodeFn = fn(&[u8], usize) -> Result<(Value, usize), FieldDecodeError>;

// Decode one binary value list directly into runtime `Value` items while
// advancing the same cursor that identifies each nested payload boundary.
pub(super) fn decode_value_storage_binary_list_items_single_pass(
    raw_bytes: &[u8],
    offset: usize,
    shape_label: &'static str,
    trailing_label: Option<&'static str>,
    decode_value: ValueBinaryDecodeFn,
) -> Result<(Vec<Value>, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value list payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut cursor = payload_start;
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (item, next_cursor) = decode_value(raw_bytes, cursor)?;
        cursor = next_cursor;
        items.push(item);
    }
    if let Some(trailing_label) = trailing_label
        && cursor != raw_bytes.len()
    {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok((items, cursor))
}

// Decode one binary value map directly into runtime entry pairs while
// advancing the same cursor that identifies each nested key/value boundary.
pub(super) fn decode_value_storage_binary_map_entries_single_pass(
    raw_bytes: &[u8],
    offset: usize,
    shape_label: &'static str,
    trailing_label: Option<&'static str>,
    decode_value: ValueBinaryDecodeFn,
) -> Result<(Vec<(Value, Value)>, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value map payload",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut cursor = payload_start;
    let mut entries = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (key, value_start) = decode_value(raw_bytes, cursor)?;
        let (value, next_cursor) = decode_value(raw_bytes, value_start)?;
        cursor = next_cursor;
        entries.push((key, value));
    }
    if let Some(trailing_label) = trailing_label
        && cursor != raw_bytes.len()
    {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok((entries, cursor))
}
