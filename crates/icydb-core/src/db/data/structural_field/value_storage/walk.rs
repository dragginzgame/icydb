//! Traversal helpers for structural value-storage collection payloads.
//!
//! Skip-based traversal is the authoritative structural validation model in
//! this module. It walks bytes with `skip_value_storage_binary_value`, proves
//! each nested item boundary, and yields borrowed slices without constructing a
//! runtime `Value`. The visitor and retained callback walkers use that model.
//!
//! Decode-based traversal is a materialization model for recursive
//! `Value::List` and `Value::Map` decode. It advances the cursor as each nested
//! value is decoded and may assume the current collection frame owns the byte
//! range being traversed. It does not replace skip as the general boundary
//! authority for borrowed structural slices.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{TAG_LIST, TAG_MAP, parse_binary_head},
    value_storage::skip::skip_value_storage_binary_value,
};
use crate::value::Value;

// Alias the callback shape for binary value-map walkers.
pub(super) type ValueBinaryMapEntryFn =
    unsafe fn(&[u8], &[u8], *mut ()) -> Result<(), FieldDecodeError>;

// Alias the cursor-returning decoder used by single-pass recursive collection
// materialization.
pub(super) type ValueBinaryDecodeFn = fn(&[u8], usize) -> Result<(Value, usize), FieldDecodeError>;

// Visit one binary value list as borrowed nested item slices without forcing
// callers to stage the slices in a Vec.
pub(super) fn visit_value_storage_list_items<'a, T, Init, Visit>(
    raw_bytes: &'a [u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    init: Init,
    mut visit_item: Visit,
) -> Result<T, FieldDecodeError>
where
    Init: FnOnce(usize) -> T,
    Visit: FnMut(&mut T, &'a [u8]) -> Result<(), FieldDecodeError>,
{
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value list payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut state = init(len as usize);
    let mut cursor = payload_start;
    for _ in 0..len {
        let item_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        visit_item(&mut state, &raw_bytes[item_start..cursor])?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(state)
}

// Walk one binary value list and yield each nested `Value` item slice.
#[expect(
    dead_code,
    reason = "existing borrowed-slice walker API is intentionally retained"
)]
pub(super) fn walk_value_storage_binary_list_items(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_item: unsafe fn(&[u8], *mut ()) -> Result<(), FieldDecodeError>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value list payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        let item_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        unsafe { on_item(&raw_bytes[item_start..cursor], context)? };
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}

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

// Visit one binary value map as borrowed nested key/value slice pairs without
// forcing callers to stage the pairs in a Vec.
pub(super) fn visit_value_storage_map_entries<'a, T, Init, Visit>(
    raw_bytes: &'a [u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    init: Init,
    mut visit_entry: Visit,
) -> Result<T, FieldDecodeError>
where
    Init: FnOnce(usize) -> T,
    Visit: FnMut(&mut T, &'a [u8], &'a [u8]) -> Result<(), FieldDecodeError>,
{
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value map payload",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut state = init(len as usize);
    let mut cursor = payload_start;
    for _ in 0..len {
        let key_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        visit_entry(
            &mut state,
            &raw_bytes[key_start..value_start],
            &raw_bytes[value_start..cursor],
        )?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(state)
}

// Walk one binary value map and yield each nested key/value slice pair.
#[expect(
    dead_code,
    reason = "existing borrowed-slice walker API is intentionally retained"
)]
pub(super) fn walk_value_storage_binary_map_entries(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_entry: ValueBinaryMapEntryFn,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value map payload",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        let key_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        unsafe {
            on_entry(
                &raw_bytes[key_start..value_start],
                &raw_bytes[value_start..cursor],
                context,
            )?;
        };
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
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
