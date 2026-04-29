use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        TAG_BYTES, TAG_INT64, TAG_LIST, TAG_TEXT, TAG_UINT64,
        decode_text_scalar_bytes as decode_binary_text_scalar_bytes, parse_binary_head,
        payload_bytes as binary_payload_bytes, skip_binary_value,
    },
    primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
    value_storage::skip::skip_value_storage_binary_value,
};

type BinarySkipFn = fn(&[u8], usize) -> Result<usize, FieldDecodeError>;

// === Scalar Decode Helpers (non-parsed) ===

// Decode one required binary bytes payload.
pub(super) fn decode_binary_required_bytes<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_BYTES {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    binary_payload_bytes(raw_bytes, len, payload_start, label)
}

// Decode one required binary text payload.
pub(super) fn decode_binary_required_text<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a str, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_TEXT {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    decode_binary_text_scalar_bytes(raw_bytes, len, payload_start)
}

// Decode one required binary i64 payload.
pub(super) fn decode_binary_required_i64(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<i64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    decode_i64_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, label)?,
        label,
    )
}

// Decode one required binary u64 payload.
pub(super) fn decode_binary_required_u64(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<u64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_UINT64 || len != 8 {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    decode_u64_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, label)?,
        label,
    )
}

// === Tuple Splitting Helpers ===

// Split a fixed-length binary tuple into borrowed item slices. The caller
// supplies the skip function so generic Structural Binary tuples and nested
// value-storage tuples share one traversal implementation without changing
// which authority validates each item.
pub(super) fn split_binary_tuple_items<'a>(
    raw_bytes: &'a [u8],
    expected_len: u32,
    label: &'static str,
    skip_item: BinarySkipFn,
) -> Result<Vec<&'a [u8]>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    if tag != TAG_LIST || len != expected_len {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    // TODO(value-storage zero-copy): fixed tuples currently allocate a Vec of
    // borrowed slices. Future tuple-specific decoders can return stack-shaped
    // arrays for known arities without changing the wire contract.
    let mut cursor = payload_start;
    let mut items = Vec::with_capacity(expected_len as usize);
    for _ in 0..expected_len {
        let item_start = cursor;
        cursor = skip_item(raw_bytes, cursor)?;
        items.push(&raw_bytes[item_start..cursor]);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label}"
        )));
    }

    Ok(items)
}

// Split a fixed-length tuple whose items are generic Structural Binary values.
#[expect(dead_code, reason = "kept for variable-length tuple follow-up work")]
pub(super) fn split_binary_generic_tuple_items<'a>(
    raw_bytes: &'a [u8],
    expected_len: u32,
    label: &'static str,
) -> Result<Vec<&'a [u8]>, FieldDecodeError> {
    split_binary_tuple_items(raw_bytes, expected_len, label, skip_binary_value)
}

// Split a fixed-length tuple whose items are nested `Value` envelopes.
#[expect(dead_code, reason = "kept for variable-length tuple follow-up work")]
pub(super) fn split_binary_value_storage_tuple_items<'a>(
    raw_bytes: &'a [u8],
    expected_len: u32,
    label: &'static str,
) -> Result<Vec<&'a [u8]>, FieldDecodeError> {
    split_binary_tuple_items(
        raw_bytes,
        expected_len,
        label,
        skip_value_storage_binary_value,
    )
}

// Split a two-item tuple whose items are generic Structural Binary values
// without staging borrowed item slices in a heap-backed Vec.
pub(super) fn split_binary_tuple_2<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<[&'a [u8]; 2], FieldDecodeError> {
    split_tuple_2(raw_bytes, label, skip_binary_value)
}

// Split a three-item tuple whose items are generic Structural Binary values
// without staging borrowed item slices in a heap-backed Vec.
#[expect(dead_code, reason = "kept for fixed-arity generic tuple symmetry")]
pub(super) fn split_binary_tuple_3<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<[&'a [u8]; 3], FieldDecodeError> {
    split_tuple_3(raw_bytes, label, skip_binary_value)
}

// Split a two-item tuple whose items are nested `Value` envelopes without
// staging borrowed item slices in a heap-backed Vec.
#[expect(dead_code, reason = "kept for fixed-arity value tuple symmetry")]
pub(super) fn split_value_storage_tuple_2<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<[&'a [u8]; 2], FieldDecodeError> {
    split_tuple_2(raw_bytes, label, skip_value_storage_binary_value)
}

// Split a three-item tuple whose items are nested `Value` envelopes without
// staging borrowed item slices in a heap-backed Vec.
pub(super) fn split_value_storage_tuple_3<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<[&'a [u8]; 3], FieldDecodeError> {
    split_tuple_3(raw_bytes, label, skip_value_storage_binary_value)
}

// Shared fixed-arity tuple head validation. Error wording intentionally
// matches the Vec-based splitter exactly.
fn parse_fixed_tuple_head(
    raw_bytes: &[u8],
    expected_len: u32,
    label: &'static str,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    if tag != TAG_LIST || len != expected_len {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    Ok(payload_start)
}

// Split a two-item tuple with the caller-selected item skip authority.
fn split_tuple_2<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
    skip_item: BinarySkipFn,
) -> Result<[&'a [u8]; 2], FieldDecodeError> {
    let mut cursor = parse_fixed_tuple_head(raw_bytes, 2, label)?;

    let first_start = cursor;
    cursor = skip_item(raw_bytes, cursor)?;
    let first = &raw_bytes[first_start..cursor];

    let second_start = cursor;
    cursor = skip_item(raw_bytes, cursor)?;
    let second = &raw_bytes[second_start..cursor];

    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label}"
        )));
    }

    Ok([first, second])
}

// Split a three-item tuple with the caller-selected item skip authority.
fn split_tuple_3<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
    skip_item: BinarySkipFn,
) -> Result<[&'a [u8]; 3], FieldDecodeError> {
    let mut cursor = parse_fixed_tuple_head(raw_bytes, 3, label)?;

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
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label}"
        )));
    }

    Ok([first, second, third])
}

// === Payload Extraction Helpers ===

// Extract the single nested payload carried by one local `Value` binary tag.
pub(super) fn decode_value_storage_binary_payload<'a>(
    raw_bytes: &'a [u8],
    expected_tag: u8,
    label: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let Some((&tag, _)) = raw_bytes.split_first() else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label} payload"
        )));
    };
    if tag != expected_tag {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label} payload"
        )));
    }

    let payload_end = skip_value_storage_binary_value(raw_bytes, 1)?;
    if payload_end != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label} payload"
        )));
    }

    raw_bytes.get(1..payload_end).ok_or_else(|| {
        FieldDecodeError::new(format!("structural binary: truncated {label} payload"))
    })
}
