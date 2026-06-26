//! Module: data::structural_field::value_storage::walk
//! Responsibility: decode-oriented value-storage collection materialization.
//! Does not own: scalar decode, runtime row policy, or value-storage encoding.
//! Boundary: advances nested decode cursors for recursive `Value` materialization.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{TAG_LIST, TAG_MAP, parse_binary_head},
    value_storage::{next_value_storage_decode_depth, reserve_one_value_storage_item},
};
use crate::value::Value;

// Alias the cursor-returning decoder used by single-pass recursive collection
// materialization.
type ValueBinaryDecodeFn = fn(&[u8], usize, usize) -> Result<(Value, usize), FieldDecodeError>;

// Decode one binary value list directly into runtime `Value` items while
// advancing the same cursor that identifies each nested payload boundary.
pub(super) fn decode_value_storage_binary_list_items_single_pass(
    raw_bytes: &[u8],
    offset: usize,
    enforce_trailing: bool,
    depth: usize,
    decode_value: ValueBinaryDecodeFn,
) -> Result<(Vec<Value>, usize), FieldDecodeError> {
    let depth = next_value_storage_decode_depth(depth)?;
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    let mut items = Vec::new();
    for _ in 0..len {
        reserve_one_value_storage_item(&mut items)?;
        let (item, next_cursor) = decode_value(raw_bytes, cursor, depth)?;
        cursor = next_cursor;
        items.push(item);
    }
    if enforce_trailing && cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok((items, cursor))
}

// Decode one binary value map directly into runtime entry pairs while
// advancing the same cursor that identifies each nested key/value boundary.
pub(super) fn decode_value_storage_binary_map_entries_single_pass(
    raw_bytes: &[u8],
    offset: usize,
    enforce_trailing: bool,
    depth: usize,
    decode_value: ValueBinaryDecodeFn,
) -> Result<(Vec<(Value, Value)>, usize), FieldDecodeError> {
    let depth = next_value_storage_decode_depth(depth)?;
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    let mut entries = Vec::new();
    for _ in 0..len {
        reserve_one_value_storage_item(&mut entries)?;
        let (key, value_start) = decode_value(raw_bytes, cursor, depth)?;
        let (value, next_cursor) = decode_value(raw_bytes, value_start, depth)?;
        cursor = next_cursor;
        entries.push((key, value));
    }
    if enforce_trailing && cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok((entries, cursor))
}
