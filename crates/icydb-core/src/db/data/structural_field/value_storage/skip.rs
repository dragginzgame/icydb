//! Module: data::structural_field::value_storage::skip
//! Responsibility: non-materializing value-storage byte skipping and boundary validation.
//! Does not own: runtime `Value` construction, field-kind routing, or row decode.
//! Boundary: proves where one value-storage payload ends before borrowed decode inspects it.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NAT64, TAG_NULL, TAG_TEXT,
        TAG_TRUE, TAG_UNIT, parse_binary_head, skip_binary_value,
    },
    value_storage::{next_value_storage_decode_depth, tags::is_local_value_storage_tag},
};

// Skip one binary `Value` envelope without delegating nested `Value` items
// back to the generic Structural Binary walker.
pub(super) fn skip_value_storage_binary_value(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    skip_value_storage_binary_value_at_depth(raw_bytes, offset, 0)
}

pub(super) fn skip_value_storage_binary_value_at_depth(
    raw_bytes: &[u8],
    offset: usize,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let depth = next_value_storage_decode_depth(depth)?;
    let Some(&tag) = raw_bytes.get(offset) else {
        return Err(FieldDecodeError::new());
    };

    match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE | TAG_INT64 | TAG_NAT64 | TAG_TEXT
        | TAG_BYTES => skip_binary_value(raw_bytes, offset),
        TAG_LIST => skip_value_storage_binary_list(raw_bytes, offset, depth),
        TAG_MAP => skip_value_storage_binary_map(raw_bytes, offset, depth),
        other if is_local_value_storage_tag(other) => {
            skip_value_storage_binary_value_at_depth(raw_bytes, offset + 1, depth)
        }
        _ => Err(FieldDecodeError::new()),
    }
}

// Skip one binary value list by recursing through nested `Value` items.
fn skip_value_storage_binary_list(
    raw_bytes: &[u8],
    offset: usize,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        cursor = skip_value_storage_binary_value_at_depth(raw_bytes, cursor, depth)?;
    }

    Ok(cursor)
}

// Skip one binary value map by recursing through nested `Value` keys and
// values.
fn skip_value_storage_binary_map(
    raw_bytes: &[u8],
    offset: usize,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        cursor = skip_value_storage_binary_value_at_depth(raw_bytes, cursor, depth)?;
        cursor = skip_value_storage_binary_value_at_depth(raw_bytes, cursor, depth)?;
    }

    Ok(cursor)
}
