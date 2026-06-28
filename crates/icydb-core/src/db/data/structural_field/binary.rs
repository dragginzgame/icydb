//! Module: data::structural_field::binary
//! Responsibility: low-level bounded Structural Binary v1 parsing and raw-slice walkers.
//! Does not own: field semantics, runtime `Value` reconstruction, or row-level policy.
//! Boundary: higher structural-field owners will move here one contract at a time as the
//! old structural grammar is retired.

use crate::db::data::structural_field::{
    FieldDecodeError,
    primitive::{
        encode_f32_payload_bytes, encode_f64_payload_bytes, encode_i64_payload_bytes,
        encode_u64_payload_bytes,
    },
};

pub(super) const TAG_NULL: u8 = 0x00;
pub(super) const TAG_UNIT: u8 = 0x01;
pub(super) const TAG_FALSE: u8 = 0x02;
pub(super) const TAG_TRUE: u8 = 0x03;
pub(super) const TAG_NAT64: u8 = 0x10;
pub(super) const TAG_INT64: u8 = 0x11;
pub(super) const TAG_TEXT: u8 = 0x12;
pub(super) const TAG_BYTES: u8 = 0x13;
pub(super) const TAG_FLOAT32: u8 = 0x14;
pub(super) const TAG_FLOAT64: u8 = 0x15;
pub(super) const TAG_LIST: u8 = 0x20;
pub(super) const TAG_MAP: u8 = 0x21;
pub(super) const TAG_VARIANT_UNIT: u8 = 0x30;
pub(super) const TAG_VARIANT_PAYLOAD: u8 = 0x31;

const WORD32_LEN: usize = 4;
const WORD64_LEN: usize = 8;
const MAX_STRUCTURAL_BINARY_SKIP_DEPTH: usize = 64;

/// Append one tag-only Structural Binary v1 value.
pub(super) fn push_binary_tag(out: &mut Vec<u8>, tag: u8) {
    out.push(tag);
}

/// Append one `null` Structural Binary v1 value.
pub(super) fn push_binary_null(out: &mut Vec<u8>) {
    push_binary_tag(out, TAG_NULL);
}

/// Append one `unit` Structural Binary v1 value.
pub(super) fn push_binary_unit(out: &mut Vec<u8>) {
    push_binary_tag(out, TAG_UNIT);
}

/// Append one `bool` Structural Binary v1 value.
pub(super) fn push_binary_bool(out: &mut Vec<u8>, value: bool) {
    push_binary_tag(out, if value { TAG_TRUE } else { TAG_FALSE });
}

/// Append one fixed-width `u64` Structural Binary v1 value.
pub(super) fn push_binary_nat64(out: &mut Vec<u8>, value: u64) {
    out.push(TAG_NAT64);
    out.extend_from_slice(&encode_u64_payload_bytes(value));
}

/// Append one fixed-width `i64` Structural Binary v1 value.
pub(super) fn push_binary_int64(out: &mut Vec<u8>, value: i64) {
    out.push(TAG_INT64);
    out.extend_from_slice(&encode_i64_payload_bytes(value));
}

/// Append one fixed-width `f32` Structural Binary v1 value.
pub(super) fn push_binary_float32(out: &mut Vec<u8>, value: f32) {
    out.push(TAG_FLOAT32);
    out.extend_from_slice(&encode_f32_payload_bytes(value));
}

/// Append one fixed-width `f64` Structural Binary v1 value.
pub(super) fn push_binary_float64(out: &mut Vec<u8>, value: f64) {
    out.push(TAG_FLOAT64);
    out.extend_from_slice(&encode_f64_payload_bytes(value));
}

/// Append one length-prefixed UTF-8 string Structural Binary v1 value.
pub(super) fn push_binary_text(out: &mut Vec<u8>, value: &str) {
    out.push(TAG_TEXT);
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("structural binary invariant")
            .to_be_bytes(),
    );
    out.extend_from_slice(value.as_bytes());
}

/// Append one length-prefixed raw-byte Structural Binary v1 value.
pub(super) fn push_binary_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.push(TAG_BYTES);
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("structural binary invariant")
            .to_be_bytes(),
    );
    out.extend_from_slice(value);
}

/// Append one list header with the given item count.
pub(super) fn push_binary_list_len(out: &mut Vec<u8>, len: usize) {
    out.push(TAG_LIST);
    out.extend_from_slice(
        &u32::try_from(len)
            .expect("structural binary invariant")
            .to_be_bytes(),
    );
}

/// Append one map header with the given entry count.
pub(super) fn push_binary_map_len(out: &mut Vec<u8>, len: usize) {
    out.push(TAG_MAP);
    out.extend_from_slice(
        &u32::try_from(len)
            .expect("structural binary invariant")
            .to_be_bytes(),
    );
}

/// Append one unit variant envelope containing only the variant label.
pub(super) fn push_binary_variant_unit(out: &mut Vec<u8>, label: &str) {
    out.push(TAG_VARIANT_UNIT);
    out.extend_from_slice(
        &u32::try_from(label.len())
            .expect("structural binary invariant")
            .to_be_bytes(),
    );
    out.extend_from_slice(label.as_bytes());
}

/// Append one payload-bearing variant envelope containing the variant label
/// followed by one nested payload.
pub(super) fn push_binary_variant_payload(out: &mut Vec<u8>, label: &str, payload: &[u8]) {
    out.push(TAG_VARIANT_PAYLOAD);
    out.extend_from_slice(
        &u32::try_from(label.len())
            .expect("structural binary invariant")
            .to_be_bytes(),
    );
    out.extend_from_slice(label.as_bytes());
    out.extend_from_slice(payload);
}

type ListItemVisitor<'a> = dyn FnMut(&[u8]) -> Result<(), FieldDecodeError> + 'a;
type MapEntryVisitor<'a> = dyn FnMut(&[u8], &[u8]) -> Result<(), FieldDecodeError> + 'a;

///
/// BinaryHead
///
/// BinaryHead captures one parsed Structural Binary v1 value head.
/// Higher layers use it to distinguish fixed-width scalar forms from
/// length-prefixed or recursively traversable forms without rebuilding a
/// generic tree.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BinaryHead {
    payload_offset: usize,
    tag: u8,
    len: u32,
}

// Parse one Structural Binary v1 head from the provided byte offset.
pub(super) fn parse_binary_head(
    bytes: &[u8],
    offset: usize,
) -> Result<Option<(u8, u32, usize)>, FieldDecodeError> {
    let Some(&tag) = bytes.get(offset) else {
        return Ok(None);
    };
    let payload_offset = offset.checked_add(1).ok_or_else(FieldDecodeError::new)?;

    let len = match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE => 0,
        TAG_NAT64 | TAG_INT64 | TAG_FLOAT64 => {
            u32::try_from(WORD64_LEN).expect("structural binary invariant")
        }
        TAG_FLOAT32 => u32::try_from(WORD32_LEN).expect("structural binary invariant"),
        TAG_TEXT | TAG_BYTES | TAG_LIST | TAG_MAP | TAG_VARIANT_UNIT | TAG_VARIANT_PAYLOAD => {
            decode_u32(bytes, payload_offset)?
        }
        _ => {
            return Err(FieldDecodeError::new());
        }
    };

    let payload_offset = match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE | TAG_NAT64 | TAG_INT64 | TAG_FLOAT32
        | TAG_FLOAT64 => payload_offset,
        TAG_TEXT | TAG_BYTES | TAG_LIST | TAG_MAP | TAG_VARIANT_UNIT | TAG_VARIANT_PAYLOAD => {
            payload_offset
                .checked_add(WORD32_LEN)
                .ok_or_else(FieldDecodeError::new)?
        }
        _ => unreachable!("unknown tags are rejected above"),
    };

    Ok(Some((tag, len, payload_offset)))
}

// Skip one self-contained Structural Binary v1 value without decoding it.
pub(super) fn skip_binary_value(bytes: &[u8], offset: usize) -> Result<usize, FieldDecodeError> {
    skip_binary_value_at_depth(bytes, offset, 0)
}

fn skip_binary_value_at_depth(
    bytes: &[u8],
    offset: usize,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    if depth >= MAX_STRUCTURAL_BINARY_SKIP_DEPTH {
        return Err(FieldDecodeError::new());
    }
    let depth = depth.saturating_add(1);
    let Some((tag, len, payload_offset)) = parse_binary_head(bytes, offset)? else {
        return Err(FieldDecodeError::new());
    };
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    match head.tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE => Ok(head.payload_offset),
        TAG_FLOAT32 => checked_advance(bytes, head.payload_offset, WORD32_LEN),
        TAG_NAT64 | TAG_INT64 | TAG_FLOAT64 => {
            checked_advance(bytes, head.payload_offset, WORD64_LEN)
        }
        TAG_TEXT | TAG_BYTES => checked_advance(
            bytes,
            head.payload_offset,
            usize::try_from(head.len).map_err(|_| FieldDecodeError::new())?,
        ),
        TAG_LIST => skip_list_payload(bytes, head, depth),
        TAG_MAP => skip_map_payload(bytes, head, depth),
        TAG_VARIANT_UNIT => skip_variant_unit_payload(bytes, head),
        TAG_VARIANT_PAYLOAD => skip_variant_payload(bytes, head, depth),
        _ => unreachable!("unknown tags are rejected above"),
    }
}

// Walk one Structural Binary v1 list and yield each raw item slice to the caller.
pub(super) fn walk_binary_list_items(
    raw_bytes: &[u8],
    on_item: &mut ListItemVisitor<'_>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        let item_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        on_item(&raw_bytes[item_start..cursor])?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

// Walk one Structural Binary v1 map and yield each raw key/value slice pair to the caller.
pub(super) fn walk_binary_map_entries(
    raw_bytes: &[u8],
    on_entry: &mut MapEntryVisitor<'_>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new());
    }
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        let key_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        on_entry(
            &raw_bytes[key_start..value_start],
            &raw_bytes[value_start..cursor],
        )?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

// Split one tagged variant envelope into its ASCII variant label and optional payload slice.
pub(super) fn split_binary_variant_payload(
    raw_bytes: &[u8],
) -> Result<(&[u8], Option<&[u8]>), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    match head.tag {
        TAG_VARIANT_UNIT => {
            let label = decode_variant_label_bytes(raw_bytes, head)?;
            if variant_payload_end(head, label.len())? != raw_bytes.len() {
                return Err(FieldDecodeError::new());
            }

            Ok((label, None))
        }
        TAG_VARIANT_PAYLOAD => {
            let label = decode_variant_label_bytes(raw_bytes, head)?;
            let payload_start = variant_payload_end(head, label.len())?;
            let payload_end = skip_binary_value_at_depth(raw_bytes, payload_start, 1)?;
            if payload_end != raw_bytes.len() {
                return Err(FieldDecodeError::new());
            }

            Ok((label, Some(&raw_bytes[payload_start..payload_end])))
        }
        _ => Err(FieldDecodeError::new()),
    }
}

// Decode one big-endian u32 from the requested byte offset.
fn decode_u32(bytes: &[u8], offset: usize) -> Result<u32, FieldDecodeError> {
    let slice = bytes
        .get(offset..offset + WORD32_LEN)
        .ok_or_else(FieldDecodeError::new)?;

    Ok(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

// Advance one cursor by the requested number of bytes and prove the resulting
// slice still fits inside the provided buffer.
fn checked_advance(bytes: &[u8], offset: usize, len: usize) -> Result<usize, FieldDecodeError> {
    let end = offset.checked_add(len).ok_or_else(FieldDecodeError::new)?;
    if end > bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(end)
}

// Skip one list payload by recursively skipping its declared item count.
fn skip_list_payload(
    bytes: &[u8],
    head: BinaryHead,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        cursor = skip_binary_value_at_depth(bytes, cursor, depth)?;
    }

    Ok(cursor)
}

// Skip one map payload by recursively skipping its declared key/value entry pairs.
fn skip_map_payload(
    bytes: &[u8],
    head: BinaryHead,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        cursor = skip_binary_value_at_depth(bytes, cursor, depth)?;
        cursor = skip_binary_value_at_depth(bytes, cursor, depth)?;
    }

    Ok(cursor)
}

// Skip one unit-variant payload containing only its label bytes.
fn skip_variant_unit_payload(bytes: &[u8], head: BinaryHead) -> Result<usize, FieldDecodeError> {
    let label_len = usize::try_from(head.len).map_err(|_| FieldDecodeError::new())?;

    checked_advance(bytes, head.payload_offset, label_len)
}

// Skip one payload-bearing variant by advancing over the label bytes and then one nested payload.
fn skip_variant_payload(
    bytes: &[u8],
    head: BinaryHead,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let label_len = usize::try_from(head.len).map_err(|_| FieldDecodeError::new())?;
    let payload_start = checked_advance(bytes, head.payload_offset, label_len)?;

    skip_binary_value_at_depth(bytes, payload_start, depth)
}

// Decode one raw variant label slice from a previously parsed variant head.
fn decode_variant_label_bytes(bytes: &[u8], head: BinaryHead) -> Result<&[u8], FieldDecodeError> {
    let label_len = usize::try_from(head.len).map_err(|_| FieldDecodeError::new())?;
    let label_end = checked_advance(bytes, head.payload_offset, label_len)?;

    bytes
        .get(head.payload_offset..label_end)
        .ok_or_else(FieldDecodeError::new)
}

// Compute the payload start immediately after the previously decoded variant label.
fn variant_payload_end(head: BinaryHead, label_len: usize) -> Result<usize, FieldDecodeError> {
    head.payload_offset
        .checked_add(label_len)
        .ok_or_else(FieldDecodeError::new)
}

// Decode one definite-length Structural Binary text payload from the enclosing field bytes.
pub(super) fn decode_text_scalar_bytes(
    bytes: &[u8],
    len: u32,
    payload_start: usize,
) -> Result<&str, FieldDecodeError> {
    let text_len = usize::try_from(len).map_err(|_| FieldDecodeError::new())?;
    let payload_end = payload_start
        .checked_add(text_len)
        .ok_or_else(FieldDecodeError::new)?;
    let payload = bytes
        .get(payload_start..payload_end)
        .ok_or_else(FieldDecodeError::new)?;

    std::str::from_utf8(payload).map_err(|_| FieldDecodeError::new())
}

// Decode one raw payload slice from a definite-length Structural Binary byte payload.
pub(super) fn payload_bytes(
    raw_bytes: &[u8],
    len: u32,
    payload_start: usize,
) -> Result<&[u8], FieldDecodeError> {
    let payload_len = usize::try_from(len).map_err(|_| FieldDecodeError::new())?;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(FieldDecodeError::new)?;

    raw_bytes
        .get(payload_start..payload_end)
        .ok_or_else(FieldDecodeError::new)
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
