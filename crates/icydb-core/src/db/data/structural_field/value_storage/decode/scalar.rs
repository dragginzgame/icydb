use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, TAG_INT64, TAG_TEXT, TAG_UINT64,
            decode_text_scalar_bytes as decode_binary_text_scalar_bytes, parse_binary_head,
        },
        primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
        value_storage::decode::cursor::{
            enforce_optional_trailing_label, parsed_value_payload_end,
        },
    },
    value::Value,
};

// Decode one nested i64 scalar while advancing by its fixed-width payload.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_i64_value_at(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<(Value, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let (value, cursor) = decode_binary_i64_from_parsed(raw_bytes, tag, len, payload_start, None)?;

    Ok((Value::Int(value), cursor))
}

// Decode one nested u64 scalar while advancing by its fixed-width payload.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_u64_value_at(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<(Value, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let (value, cursor) = decode_binary_u64_from_parsed(raw_bytes, tag, len, payload_start, None)?;

    Ok((Value::Uint(value), cursor))
}

// Decode one nested text scalar while advancing by its length-prefixed payload.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_text_value_at(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<(Value, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated text payload",
        ));
    };
    let (value, cursor) = decode_binary_text_from_parsed(raw_bytes, tag, len, payload_start, None)?;

    Ok((Value::Text(value.to_owned()), cursor))
}

// Decode one nested byte scalar while advancing by its length-prefixed payload.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_blob_value_at(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<(Value, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated byte payload",
        ));
    };
    let (value, cursor) = decode_binary_blob_from_parsed(raw_bytes, tag, len, payload_start, None)?;

    Ok((Value::Blob(value.to_vec()), cursor))
}

// Decode one top-level i64 generic binary value.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_i64_value(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let value = decode_binary_i64_scalar(raw_bytes)?;

    Ok(Value::Int(value))
}

// Decode one top-level u64 generic binary value.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_u64_value(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let value = decode_binary_u64_scalar(raw_bytes)?;

    Ok(Value::Uint(value))
}

// Decode one top-level i64 scalar without wrapping it in a runtime `Value`.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_i64_scalar(
    raw_bytes: &[u8],
) -> Result<i64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let (value, _) = decode_binary_i64_from_parsed(
        raw_bytes,
        tag,
        len,
        payload_start,
        Some("structural binary: trailing bytes after integer payload"),
    )?;

    Ok(value)
}

// Decode one top-level u64 scalar without wrapping it in a runtime `Value`.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_u64_scalar(
    raw_bytes: &[u8],
) -> Result<u64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let (value, _) = decode_binary_u64_from_parsed(
        raw_bytes,
        tag,
        len,
        payload_start,
        Some("structural binary: trailing bytes after integer payload"),
    )?;

    Ok(value)
}

// Decode one top-level text generic binary value.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_text_value(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let value = decode_binary_text_scalar(raw_bytes)?;

    Ok(Value::Text(value.to_owned()))
}

// Decode one top-level text scalar without allocating an owned `String`.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_text_scalar(
    raw_bytes: &[u8],
) -> Result<&str, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated text payload",
        ));
    };
    let (value, _) = decode_binary_text_from_parsed(
        raw_bytes,
        tag,
        len,
        payload_start,
        Some("structural binary: trailing bytes after text payload"),
    )?;

    Ok(value)
}

// Decode one top-level bytes generic binary value.
pub(in crate::db::data::structural_field::value_storage::decode) fn decode_binary_blob_value(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated byte payload",
        ));
    };
    let (value, _) = decode_binary_blob_from_parsed(
        raw_bytes,
        tag,
        len,
        payload_start,
        Some("structural binary: trailing bytes after byte payload"),
    )?;

    Ok(Value::Blob(value.to_vec()))
}

// Decode one parsed i64 scalar after the caller has already read the structural
// head. Top-level callers pass a trailing label, while nested callers use the
// returned cursor to continue walking the enclosing payload.
fn decode_binary_i64_from_parsed(
    raw_bytes: &[u8],
    tag: u8,
    len: u32,
    payload_start: usize,
    trailing_label: Option<&'static str>,
) -> Result<(i64, usize), FieldDecodeError> {
    if tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected i64 integer payload",
        ));
    }
    let cursor = parsed_value_payload_end(
        raw_bytes,
        len,
        payload_start,
        "structural binary: truncated fixed-width scalar payload",
    )?;
    enforce_optional_trailing_label(cursor, raw_bytes.len(), trailing_label)?;
    let value = decode_i64_payload_bytes(&raw_bytes[payload_start..cursor], "i64")?;

    Ok((value, cursor))
}

// Decode one parsed u64 scalar after the caller has already read the structural
// head. The optional trailing label lets the same core serve both root and
// nested scalar decode without weakening either boundary rule.
fn decode_binary_u64_from_parsed(
    raw_bytes: &[u8],
    tag: u8,
    len: u32,
    payload_start: usize,
    trailing_label: Option<&'static str>,
) -> Result<(u64, usize), FieldDecodeError> {
    if tag != TAG_UINT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected u64 integer payload",
        ));
    }
    let cursor = parsed_value_payload_end(
        raw_bytes,
        len,
        payload_start,
        "structural binary: truncated fixed-width scalar payload",
    )?;
    enforce_optional_trailing_label(cursor, raw_bytes.len(), trailing_label)?;
    let value = decode_u64_payload_bytes(&raw_bytes[payload_start..cursor], "u64")?;

    Ok((value, cursor))
}

// Decode one parsed text scalar after the caller has already read the
// structural head. Trailing enforcement intentionally happens before UTF-8
// decoding for root values to preserve the previous error ordering.
fn decode_binary_text_from_parsed<'a>(
    raw_bytes: &'a [u8],
    tag: u8,
    len: u32,
    payload_start: usize,
    trailing_label: Option<&'static str>,
) -> Result<(&'a str, usize), FieldDecodeError> {
    if tag != TAG_TEXT {
        return Err(FieldDecodeError::new(
            "structural binary: expected text payload",
        ));
    }
    let cursor = parsed_value_payload_end(
        raw_bytes,
        len,
        payload_start,
        "structural binary: truncated scalar payload",
    )?;
    enforce_optional_trailing_label(cursor, raw_bytes.len(), trailing_label)?;
    let value = decode_binary_text_scalar_bytes(raw_bytes, len, payload_start)?;

    Ok((value, cursor))
}

// Decode one parsed byte scalar after the caller has already read the
// structural head. The borrowed slice stays bounded by the parsed payload
// cursor, so nested callers can continue from the exact following byte.
fn decode_binary_blob_from_parsed<'a>(
    raw_bytes: &'a [u8],
    tag: u8,
    len: u32,
    payload_start: usize,
    trailing_label: Option<&'static str>,
) -> Result<(&'a [u8], usize), FieldDecodeError> {
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected byte payload",
        ));
    }
    let cursor = parsed_value_payload_end(
        raw_bytes,
        len,
        payload_start,
        "structural binary: truncated scalar payload",
    )?;
    enforce_optional_trailing_label(cursor, raw_bytes.len(), trailing_label)?;
    let value = raw_bytes
        .get(payload_start..cursor)
        .ok_or_else(|| FieldDecodeError::new("structural binary: truncated scalar payload"))?;

    Ok((value, cursor))
}
