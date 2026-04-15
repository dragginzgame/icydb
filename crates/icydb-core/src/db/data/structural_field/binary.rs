//! Module: data::structural_field::binary
//! Responsibility: low-level bounded Structural Binary v1 parsing and raw-slice walkers.
//! Does not own: field semantics, runtime `Value` reconstruction, or row-level policy.
//! Boundary: higher structural-field owners will move here one contract at a time as the
//! old structural grammar is retired.

// Phase A lands the walker before production owners switch to it, so the
// non-test build must tolerate this module being present but not yet wired in.
#![cfg_attr(not(test), allow(dead_code))]

use crate::db::data::structural_field::FieldDecodeError;

pub(super) const TAG_NULL: u8 = 0x00;
pub(super) const TAG_UNIT: u8 = 0x01;
pub(super) const TAG_FALSE: u8 = 0x02;
pub(super) const TAG_TRUE: u8 = 0x03;
pub(super) const TAG_UINT64: u8 = 0x10;
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
pub(super) fn push_binary_uint64(out: &mut Vec<u8>, value: u64) {
    out.push(TAG_UINT64);
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append one fixed-width `i64` Structural Binary v1 value.
pub(super) fn push_binary_int64(out: &mut Vec<u8>, value: i64) {
    out.push(TAG_INT64);
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append one fixed-width `f32` Structural Binary v1 value.
pub(super) fn push_binary_float32(out: &mut Vec<u8>, value: f32) {
    out.push(TAG_FLOAT32);
    out.extend_from_slice(&value.to_bits().to_be_bytes());
}

/// Append one fixed-width `f64` Structural Binary v1 value.
pub(super) fn push_binary_float64(out: &mut Vec<u8>, value: f64) {
    out.push(TAG_FLOAT64);
    out.extend_from_slice(&value.to_bits().to_be_bytes());
}

/// Append one length-prefixed UTF-8 string Structural Binary v1 value.
pub(super) fn push_binary_text(out: &mut Vec<u8>, value: &str) {
    out.push(TAG_TEXT);
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("text length fits in Structural Binary v1 len")
            .to_be_bytes(),
    );
    out.extend_from_slice(value.as_bytes());
}

/// Append one length-prefixed raw-byte Structural Binary v1 value.
pub(super) fn push_binary_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.push(TAG_BYTES);
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("byte payload length fits in Structural Binary v1 len")
            .to_be_bytes(),
    );
    out.extend_from_slice(value);
}

/// Append one list header with the given item count.
pub(super) fn push_binary_list_len(out: &mut Vec<u8>, len: usize) {
    out.push(TAG_LIST);
    out.extend_from_slice(
        &u32::try_from(len)
            .expect("list item count fits in Structural Binary v1 len")
            .to_be_bytes(),
    );
}

/// Append one map header with the given entry count.
pub(super) fn push_binary_map_len(out: &mut Vec<u8>, len: usize) {
    out.push(TAG_MAP);
    out.extend_from_slice(
        &u32::try_from(len)
            .expect("map entry count fits in Structural Binary v1 len")
            .to_be_bytes(),
    );
}

/// Append one unit variant envelope containing only the variant label.
pub(super) fn push_binary_variant_unit(out: &mut Vec<u8>, label: &str) {
    out.push(TAG_VARIANT_UNIT);
    out.extend_from_slice(
        &u32::try_from(label.len())
            .expect("variant label length fits in Structural Binary v1 len")
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
            .expect("variant label length fits in Structural Binary v1 len")
            .to_be_bytes(),
    );
    out.extend_from_slice(label.as_bytes());
    out.extend_from_slice(payload);
}

// Alias the callback shape for Structural Binary v1 list walkers.
type ListItemDecodeFn = unsafe fn(&[u8], *mut ()) -> Result<(), FieldDecodeError>;

// Alias the callback shape for Structural Binary v1 map walkers.
type MapEntryDecodeFn = unsafe fn(&[u8], &[u8], *mut ()) -> Result<(), FieldDecodeError>;

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
    let payload_offset = offset
        .checked_add(1)
        .ok_or_else(|| FieldDecodeError::new("structural binary: head offset overflow"))?;

    let len = match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE => 0,
        TAG_UINT64 | TAG_INT64 | TAG_FLOAT64 => u32::try_from(WORD64_LEN)
            .expect("fixed-width scalar length fits in structural binary len"),
        TAG_FLOAT32 => u32::try_from(WORD32_LEN)
            .expect("fixed-width scalar length fits in structural binary len"),
        TAG_TEXT | TAG_BYTES | TAG_LIST | TAG_MAP | TAG_VARIANT_UNIT | TAG_VARIANT_PAYLOAD => {
            decode_u32(
                bytes,
                payload_offset,
                "structural binary: truncated length prefix",
            )?
        }
        other => {
            return Err(FieldDecodeError::new(format!(
                "structural binary: unknown tag 0x{other:02X}"
            )));
        }
    };

    let payload_offset = match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE | TAG_UINT64 | TAG_INT64 | TAG_FLOAT32
        | TAG_FLOAT64 => payload_offset,
        TAG_TEXT | TAG_BYTES | TAG_LIST | TAG_MAP | TAG_VARIANT_UNIT | TAG_VARIANT_PAYLOAD => {
            payload_offset.checked_add(WORD32_LEN).ok_or_else(|| {
                FieldDecodeError::new("structural binary: payload offset overflow")
            })?
        }
        _ => unreachable!("unknown tags are rejected above"),
    };

    Ok(Some((tag, len, payload_offset)))
}

// Skip one self-contained Structural Binary v1 value without decoding it.
pub(super) fn skip_binary_value(bytes: &[u8], offset: usize) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    match head.tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE => Ok(head.payload_offset),
        TAG_FLOAT32 => checked_advance(
            bytes,
            head.payload_offset,
            WORD32_LEN,
            "structural binary: truncated fixed-width scalar payload",
        ),
        TAG_UINT64 | TAG_INT64 | TAG_FLOAT64 => checked_advance(
            bytes,
            head.payload_offset,
            WORD64_LEN,
            "structural binary: truncated fixed-width scalar payload",
        ),
        TAG_TEXT | TAG_BYTES => checked_advance(
            bytes,
            head.payload_offset,
            usize::try_from(head.len)
                .map_err(|_| FieldDecodeError::new("structural binary: scalar length too large"))?,
            "structural binary: truncated scalar payload",
        ),
        TAG_LIST => skip_list_payload(bytes, head),
        TAG_MAP => skip_map_payload(bytes, head),
        TAG_VARIANT_UNIT => skip_variant_unit_payload(bytes, head),
        TAG_VARIANT_PAYLOAD => skip_variant_payload(bytes, head),
        _ => unreachable!("unknown tags are rejected above"),
    }
}

// Walk one Structural Binary v1 list and yield each raw item slice to the caller.
//
// Safety:
// `context` must point at the state type expected by `on_item` for the full
// duration of this call.
pub(super) fn walk_binary_list_items(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_item: ListItemDecodeFn,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(shape_label));
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
        // Safety: the caller pairs `context` with the matching callback, so the
        // callback sees the concrete state type it expects.
        unsafe { on_item(&raw_bytes[item_start..cursor], context)? };
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Walk one Structural Binary v1 map and yield each raw key/value slice pair to the caller.
//
// Safety:
// `context` must point at the state type expected by `on_entry` for the full
// duration of this call.
pub(super) fn walk_binary_map_entries(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_entry: MapEntryDecodeFn,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(shape_label));
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
        // Safety: the caller pairs `context` with the matching callback, so the
        // callback sees the concrete state type it expects.
        unsafe {
            on_entry(
                &raw_bytes[key_start..value_start],
                &raw_bytes[value_start..cursor],
                context,
            )?;
        }
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Split one tagged variant envelope into its ASCII variant label and optional payload slice.
pub(super) fn split_binary_variant_payload<'a>(
    raw_bytes: &'a [u8],
    truncated_label: &'static str,
    variant_label: &'static str,
    trailing_label: &'static str,
) -> Result<(&'a [u8], Option<&'a [u8]>), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(truncated_label));
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
                return Err(FieldDecodeError::new(trailing_label));
            }

            Ok((label, None))
        }
        TAG_VARIANT_PAYLOAD => {
            let label = decode_variant_label_bytes(raw_bytes, head)?;
            let payload_start = variant_payload_end(head, label.len())?;
            let payload_end = skip_binary_value(raw_bytes, payload_start)?;
            if payload_end != raw_bytes.len() {
                return Err(FieldDecodeError::new(trailing_label));
            }

            Ok((label, Some(&raw_bytes[payload_start..payload_end])))
        }
        _ => Err(FieldDecodeError::new(variant_label)),
    }
}

// Decode one big-endian u32 from the requested byte offset.
fn decode_u32(
    bytes: &[u8],
    offset: usize,
    truncated_label: &'static str,
) -> Result<u32, FieldDecodeError> {
    let slice = bytes
        .get(offset..offset + WORD32_LEN)
        .ok_or_else(|| FieldDecodeError::new(truncated_label))?;

    Ok(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

// Advance one cursor by the requested number of bytes and prove the resulting
// slice still fits inside the provided buffer.
fn checked_advance(
    bytes: &[u8],
    offset: usize,
    len: usize,
    truncated_label: &'static str,
) -> Result<usize, FieldDecodeError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| FieldDecodeError::new("structural binary: length overflow"))?;
    if end > bytes.len() {
        return Err(FieldDecodeError::new(truncated_label));
    }

    Ok(end)
}

// Skip one list payload by recursively skipping its declared item count.
fn skip_list_payload(bytes: &[u8], head: BinaryHead) -> Result<usize, FieldDecodeError> {
    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        cursor = skip_binary_value(bytes, cursor)?;
    }

    Ok(cursor)
}

// Skip one map payload by recursively skipping its declared key/value entry pairs.
fn skip_map_payload(bytes: &[u8], head: BinaryHead) -> Result<usize, FieldDecodeError> {
    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        cursor = skip_binary_value(bytes, cursor)?;
        cursor = skip_binary_value(bytes, cursor)?;
    }

    Ok(cursor)
}

// Skip one unit-variant payload containing only its label bytes.
fn skip_variant_unit_payload(bytes: &[u8], head: BinaryHead) -> Result<usize, FieldDecodeError> {
    let label_len = usize::try_from(head.len)
        .map_err(|_| FieldDecodeError::new("structural binary: variant label too large"))?;

    checked_advance(
        bytes,
        head.payload_offset,
        label_len,
        "structural binary: truncated variant label",
    )
}

// Skip one payload-bearing variant by advancing over the label bytes and then one nested payload.
fn skip_variant_payload(bytes: &[u8], head: BinaryHead) -> Result<usize, FieldDecodeError> {
    let label_len = usize::try_from(head.len)
        .map_err(|_| FieldDecodeError::new("structural binary: variant label too large"))?;
    let payload_start = checked_advance(
        bytes,
        head.payload_offset,
        label_len,
        "structural binary: truncated variant label",
    )?;

    skip_binary_value(bytes, payload_start)
}

// Decode one raw variant label slice from a previously parsed variant head.
fn decode_variant_label_bytes(bytes: &[u8], head: BinaryHead) -> Result<&[u8], FieldDecodeError> {
    let label_len = usize::try_from(head.len)
        .map_err(|_| FieldDecodeError::new("structural binary: variant label too large"))?;
    let label_end = checked_advance(
        bytes,
        head.payload_offset,
        label_len,
        "structural binary: truncated variant label",
    )?;

    bytes
        .get(head.payload_offset..label_end)
        .ok_or_else(|| FieldDecodeError::new("structural binary: truncated variant label"))
}

// Compute the payload start immediately after the previously decoded variant label.
fn variant_payload_end(head: BinaryHead, label_len: usize) -> Result<usize, FieldDecodeError> {
    head.payload_offset
        .checked_add(label_len)
        .ok_or_else(|| FieldDecodeError::new("structural binary: variant label overflow"))
}

// Decode one definite-length Structural Binary text payload from the enclosing field bytes.
pub(super) fn decode_text_scalar_bytes(
    bytes: &[u8],
    len: u32,
    payload_start: usize,
) -> Result<&str, FieldDecodeError> {
    let text_len = usize::try_from(len)
        .map_err(|_| FieldDecodeError::new("structural binary: text too large"))?;
    let payload_end = payload_start
        .checked_add(text_len)
        .ok_or_else(|| FieldDecodeError::new("structural binary: text length overflow"))?;
    let payload = bytes
        .get(payload_start..payload_end)
        .ok_or_else(|| FieldDecodeError::new("structural binary: truncated text payload"))?;

    std::str::from_utf8(payload)
        .map_err(|_| FieldDecodeError::new("structural binary: non-utf8 text string"))
}

// Decode one raw payload slice from a definite-length Structural Binary byte payload.
pub(super) fn payload_bytes<'a>(
    raw_bytes: &'a [u8],
    len: u32,
    payload_start: usize,
    expected: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let payload_len = usize::try_from(len)
        .map_err(|_| FieldDecodeError::new(format!("structural binary: {expected} too large")))?;
    let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
        FieldDecodeError::new(format!("structural binary: {expected} length overflow"))
    })?;

    raw_bytes.get(payload_start..payload_end).ok_or_else(|| {
        FieldDecodeError::new(format!("structural binary: truncated {expected} payload"))
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NULL, TAG_TEXT, TAG_TRUE, TAG_UINT64,
        TAG_VARIANT_PAYLOAD, TAG_VARIANT_UNIT, parse_binary_head, push_binary_bool,
        skip_binary_value, split_binary_variant_payload, walk_binary_list_items,
        walk_binary_map_entries,
    };
    use crate::db::data::structural_field::FieldDecodeError;

    type ListState = Vec<Vec<u8>>;
    type MapState = Vec<(Vec<u8>, Vec<u8>)>;

    fn encode_null() -> Vec<u8> {
        vec![TAG_NULL]
    }

    fn encode_bool(value: bool) -> Vec<u8> {
        vec![if value { TAG_TRUE } else { TAG_FALSE }]
    }

    fn encode_uint64(value: u64) -> Vec<u8> {
        let mut out = vec![TAG_UINT64];
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    fn encode_int64(value: i64) -> Vec<u8> {
        let mut out = vec![TAG_INT64];
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    fn encode_text(value: &str) -> Vec<u8> {
        let mut out = vec![TAG_TEXT];
        out.extend_from_slice(
            &u32::try_from(value.len())
                .expect("text len fits u32")
                .to_be_bytes(),
        );
        out.extend_from_slice(value.as_bytes());
        out
    }

    fn encode_list(items: &[Vec<u8>]) -> Vec<u8> {
        let mut out = vec![TAG_LIST];
        out.extend_from_slice(
            &u32::try_from(items.len())
                .expect("item count fits u32")
                .to_be_bytes(),
        );
        for item in items {
            out.extend_from_slice(item);
        }
        out
    }

    fn encode_map(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
        let mut out = vec![TAG_MAP];
        out.extend_from_slice(
            &u32::try_from(entries.len())
                .expect("entry count fits u32")
                .to_be_bytes(),
        );
        for (key, value) in entries {
            out.extend_from_slice(key);
            out.extend_from_slice(value);
        }
        out
    }

    fn encode_variant_unit(label: &str) -> Vec<u8> {
        let mut out = vec![TAG_VARIANT_UNIT];
        out.extend_from_slice(
            &u32::try_from(label.len())
                .expect("label len fits u32")
                .to_be_bytes(),
        );
        out.extend_from_slice(label.as_bytes());
        out
    }

    fn encode_variant_payload(label: &str, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![TAG_VARIANT_PAYLOAD];
        out.extend_from_slice(
            &u32::try_from(label.len())
                .expect("label len fits u32")
                .to_be_bytes(),
        );
        out.extend_from_slice(label.as_bytes());
        out.extend_from_slice(payload);
        out
    }

    // Match the production walker callback contract even though this fixture
    // callback itself cannot fail.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "test callback keeps the same fallible signature as the production walker"
    )]
    fn push_list_item(item_bytes: &[u8], context: *mut ()) -> Result<(), FieldDecodeError> {
        let state = unsafe { &mut *context.cast::<ListState>() };
        state.push(item_bytes.to_vec());

        Ok(())
    }

    // Match the production walker callback contract even though this fixture
    // callback itself cannot fail.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "test callback keeps the same fallible signature as the production walker"
    )]
    fn push_map_entry(
        key_bytes: &[u8],
        value_bytes: &[u8],
        context: *mut (),
    ) -> Result<(), FieldDecodeError> {
        let state = unsafe { &mut *context.cast::<MapState>() };
        state.push((key_bytes.to_vec(), value_bytes.to_vec()));

        Ok(())
    }

    #[test]
    fn parse_binary_head_reports_tag_len_and_payload_offset() {
        let bytes = encode_text("icy");
        let head = parse_binary_head(&bytes, 0)
            .expect("head parse should succeed")
            .expect("text head should exist");

        assert_eq!(head.0, TAG_TEXT);
        assert_eq!(head.1, 3);
        assert_eq!(head.2, 5);
    }

    #[test]
    fn push_binary_bool_emits_tag_only_bool_form() {
        let mut bytes = Vec::new();
        push_binary_bool(&mut bytes, true);

        assert_eq!(bytes, encode_bool(true));
    }

    #[test]
    fn skip_binary_value_skips_nested_list_payloads() {
        let bytes = encode_list(&[
            encode_text("left"),
            encode_list(&[encode_uint64(7), encode_bool(true)]),
            encode_int64(-5),
        ]);

        assert_eq!(
            skip_binary_value(&bytes, 0).expect("list skip should succeed"),
            bytes.len(),
        );
    }

    #[test]
    fn walk_binary_list_items_yields_raw_item_slices() {
        let left = encode_text("left");
        let right = encode_uint64(9);
        let bytes = encode_list(&[left.clone(), right.clone()]);
        let mut state: ListState = Vec::new();

        walk_binary_list_items(
            &bytes,
            "expected Structural Binary list",
            "structural binary: trailing bytes after list",
            (&raw mut state).cast(),
            push_list_item,
        )
        .expect("list walk should succeed");

        assert_eq!(state, vec![left, right]);
    }

    #[test]
    fn walk_binary_map_entries_yields_raw_entry_slices() {
        let left_key = encode_text("left");
        let left_value = encode_uint64(1);
        let right_key = encode_text("right");
        let right_value = encode_uint64(2);
        let bytes = encode_map(&[
            (left_key.clone(), left_value.clone()),
            (right_key.clone(), right_value.clone()),
        ]);
        let mut state: MapState = Vec::new();

        walk_binary_map_entries(
            &bytes,
            "expected Structural Binary map",
            "structural binary: trailing bytes after map",
            (&raw mut state).cast(),
            push_map_entry,
        )
        .expect("map walk should succeed");

        assert_eq!(
            state,
            vec![(left_key, left_value), (right_key, right_value)],
        );
    }

    #[test]
    fn split_binary_variant_payload_handles_unit_and_payload_variants() {
        let unit = encode_variant_unit("Loaded");
        let payload_value = encode_uint64(7);
        let payload = encode_variant_payload("Loaded", &payload_value);

        let (unit_label, unit_payload) = split_binary_variant_payload(
            &unit,
            "structural binary: truncated variant",
            "expected Structural Binary variant",
            "structural binary: trailing bytes after variant",
        )
        .expect("unit variant split should succeed");
        let (payload_label, payload_payload) = split_binary_variant_payload(
            &payload,
            "structural binary: truncated variant",
            "expected Structural Binary variant",
            "structural binary: trailing bytes after variant",
        )
        .expect("payload variant split should succeed");

        assert_eq!(unit_label, b"Loaded");
        assert!(unit_payload.is_none());
        assert_eq!(payload_label, b"Loaded");
        assert_eq!(payload_payload, Some(payload_value.as_slice()));
    }

    #[test]
    fn split_binary_variant_payload_rejects_trailing_bytes() {
        let mut bytes = encode_variant_unit("Loaded");
        bytes.extend_from_slice(&encode_null());

        let err = split_binary_variant_payload(
            &bytes,
            "structural binary: truncated variant",
            "expected Structural Binary variant",
            "structural binary: trailing bytes after variant",
        )
        .expect_err("trailing bytes must fail closed");

        assert_eq!(
            err.to_string(),
            "structural binary: trailing bytes after variant"
        );
    }
}
