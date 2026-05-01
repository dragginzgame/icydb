//! Module: data::structural_field::scalar
//! Responsibility: direct scalar leaf decoding for `ByKind` fields that do not need composite recursion.
//! Does not own: container traversal, typed wrapper payloads, or `Value` storage decode.
//! Boundary: the structural-field root dispatches here before falling back to composite or typed wrapper lanes.

use crate::db::data::structural_field::FieldDecodeError;
use crate::db::data::structural_field::binary::{
    TAG_BYTES, TAG_FALSE, TAG_FLOAT32, TAG_FLOAT64, TAG_INT64, TAG_TEXT, TAG_TRUE, TAG_UINT64,
    decode_text_scalar_bytes as decode_binary_text_scalar_bytes,
    parse_binary_head as parse_structural_binary_head, payload_bytes as binary_payload_bytes,
    push_binary_bool, push_binary_bytes, push_binary_float32, push_binary_float64,
    push_binary_int64, push_binary_null, push_binary_text, push_binary_uint64,
    skip_binary_value as skip_structural_binary_value,
};
use crate::db::data::structural_field::primitive::{
    decode_i64_payload_bytes, decode_u64_payload_bytes,
};
use crate::db::data::structural_field::typed::{
    decode_float32_payload_bytes, decode_float64_payload_bytes, decode_int128_payload_bytes,
    decode_nat128_payload_bytes, decode_ulid_payload_bytes, encode_int128_payload_bytes,
    encode_nat128_payload_bytes, encode_ulid_payload_bytes,
};
use crate::{
    error::InternalError,
    model::field::FieldKind,
    types::{Blob, Float32, Float64, Int128, Nat128},
    value::Value,
};

/// Keep the scalar fast path aligned with the Structural Binary v1 lane so the
/// structural-field root can hard-cut scalar owners without widening authority
/// over leaf or composite contracts.
pub(super) const fn supports_scalar_binary_fast_path(kind: FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::Blob
            | FieldKind::Bool
            | FieldKind::Float32
            | FieldKind::Float64
            | FieldKind::Int
            | FieldKind::Int128
            | FieldKind::Text { .. }
            | FieldKind::Uint
            | FieldKind::Uint128
            | FieldKind::Ulid
    )
}

/// Decode one scalar field through the canonical Structural Binary v1 scalar
/// lane.
pub(super) fn decode_scalar_fast_path_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    decode_scalar_fast_path_binary_bytes(raw_bytes, kind)
}

/// Decode one scalar field directly from Structural Binary v1 bytes.
pub(super) fn decode_scalar_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    if !supports_scalar_binary_fast_path(kind) {
        return Ok(None);
    }

    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after scalar payload",
        ));
    }
    if tag == crate::db::data::structural_field::binary::TAG_NULL {
        return Ok(Some(Value::Null));
    }

    let value = match kind {
        FieldKind::Blob | FieldKind::Int128 | FieldKind::Uint128 | FieldKind::Ulid => {
            decode_scalar_fast_path_binary_bytes_kind(raw_bytes, kind, tag, len, payload_start)?
        }
        FieldKind::Text { .. } => {
            decode_scalar_fast_path_binary_text_kind(raw_bytes, kind, tag, len, payload_start)?
        }
        FieldKind::Bool
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Uint => {
            decode_scalar_fast_path_binary_numeric_kind(raw_bytes, kind, tag, len, payload_start)?
        }
        _ => return Ok(None),
    };

    Ok(Some(value))
}

/// Validate one Structural Binary v1 scalar fast-path payload.
pub(super) fn validate_scalar_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<bool, FieldDecodeError> {
    if !supports_scalar_binary_fast_path(kind) {
        return Ok(false);
    }

    decode_scalar_fast_path_binary_bytes(raw_bytes, kind)?;
    Ok(true)
}

/// Encode one scalar field directly into Structural Binary v1 bytes.
pub(super) fn encode_scalar_fast_path_binary_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<u8>>, InternalError> {
    if !supports_scalar_binary_fast_path(kind) {
        return Ok(None);
    }

    let mut encoded = Vec::new();
    match (kind, value) {
        (_, Value::Null) => push_binary_null(&mut encoded),
        (FieldKind::Blob, Value::Blob(value)) => push_binary_bytes(&mut encoded, value.as_slice()),
        (FieldKind::Bool, Value::Bool(value)) => push_binary_bool(&mut encoded, *value),
        (FieldKind::Float32, Value::Float32(value)) => {
            push_binary_float32(&mut encoded, value.get());
        }
        (FieldKind::Float64, Value::Float64(value)) => {
            push_binary_float64(&mut encoded, value.get());
        }
        (FieldKind::Int, Value::Int(value)) => push_binary_int64(&mut encoded, *value),
        (FieldKind::Int128, Value::Int128(value)) => {
            push_binary_bytes(&mut encoded, &encode_int128_payload_bytes(*value));
        }
        (FieldKind::Text { .. }, Value::Text(value)) => push_binary_text(&mut encoded, value),
        (FieldKind::Uint, Value::Uint(value)) => push_binary_uint64(&mut encoded, *value),
        (FieldKind::Uint128, Value::Uint128(value)) => {
            push_binary_bytes(&mut encoded, &encode_nat128_payload_bytes(*value));
        }
        (FieldKind::Ulid, Value::Ulid(value)) => {
            push_binary_bytes(&mut encoded, &encode_ulid_payload_bytes(*value));
        }
        _ => {
            return Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                format!("field kind {kind:?} does not accept runtime value {value:?}"),
            ));
        }
    }

    Ok(Some(encoded))
}

/// Encode one direct bool leaf through the Structural Binary v1 scalar lane.
pub(super) fn encode_bool_fast_path_binary_bytes(
    value: bool,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Bool) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept bool"),
        ));
    }

    let mut encoded = Vec::new();
    push_binary_bool(&mut encoded, value);
    Ok(encoded)
}

/// Decode one direct bool leaf through the Structural Binary v1 scalar lane.
pub(super) fn decode_bool_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<bool>, FieldDecodeError> {
    match decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        Some(Value::Bool(value)) => Ok(Some(value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(FieldDecodeError::new(
            "scalar field unexpectedly decoded as non-bool value",
        )),
        None => Err(FieldDecodeError::new(
            "field kind is not owned by the scalar bool fast path",
        )),
    }
}

/// Encode one direct text leaf through the Structural Binary v1 scalar lane.
pub(super) fn encode_text_fast_path_binary_bytes(
    value: &str,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Text { .. }) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept text"),
        ));
    }

    let mut encoded = Vec::new();
    push_binary_text(&mut encoded, value);
    Ok(encoded)
}

/// Decode one direct text leaf through the Structural Binary v1 scalar lane.
pub(super) fn decode_text_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<String>, FieldDecodeError> {
    match decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        Some(Value::Text(value)) => Ok(Some(value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(FieldDecodeError::new(
            "scalar field unexpectedly decoded as non-text value",
        )),
        None => Err(FieldDecodeError::new(
            "field kind is not owned by the scalar text fast path",
        )),
    }
}

/// Encode one direct blob leaf through the Structural Binary v1 scalar lane.
pub(super) fn encode_blob_fast_path_binary_bytes(
    value: &Blob,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Blob) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept blob"),
        ));
    }

    let mut encoded = Vec::new();
    push_binary_bytes(&mut encoded, value.as_slice());
    Ok(encoded)
}

/// Decode one direct blob leaf through the Structural Binary v1 scalar lane.
pub(super) fn decode_blob_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Blob>, FieldDecodeError> {
    match decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        Some(Value::Blob(value)) => Ok(Some(Blob::from(value))),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(FieldDecodeError::new(
            "scalar field unexpectedly decoded as non-blob value",
        )),
        None => Err(FieldDecodeError::new(
            "field kind is not owned by the scalar blob fast path",
        )),
    }
}

/// Encode one direct float32 leaf through the Structural Binary v1 scalar lane.
pub(super) fn encode_float32_fast_path_binary_bytes(
    value: Float32,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Float32) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept float32"),
        ));
    }

    let mut encoded = Vec::new();
    push_binary_float32(&mut encoded, value.get());
    Ok(encoded)
}

/// Decode one direct float32 leaf through the Structural Binary v1 scalar lane.
pub(super) fn decode_float32_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Float32>, FieldDecodeError> {
    match decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        Some(Value::Float32(value)) => Ok(Some(value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(FieldDecodeError::new(
            "scalar field unexpectedly decoded as non-float32 value",
        )),
        None => Err(FieldDecodeError::new(
            "field kind is not owned by the scalar float32 fast path",
        )),
    }
}

/// Encode one direct float64 leaf through the Structural Binary v1 scalar lane.
pub(super) fn encode_float64_fast_path_binary_bytes(
    value: Float64,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Float64) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept float64"),
        ));
    }

    let mut encoded = Vec::new();
    push_binary_float64(&mut encoded, value.get());
    Ok(encoded)
}

/// Decode one direct float64 leaf through the Structural Binary v1 scalar lane.
pub(super) fn decode_float64_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Float64>, FieldDecodeError> {
    match decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        Some(Value::Float64(value)) => Ok(Some(value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(FieldDecodeError::new(
            "scalar field unexpectedly decoded as non-float64 value",
        )),
        None => Err(FieldDecodeError::new(
            "field kind is not owned by the scalar float64 fast path",
        )),
    }
}

/// Encode one direct int128 leaf through the Structural Binary v1 scalar lane.
pub(super) fn encode_int128_fast_path_binary_bytes(
    value: Int128,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Int128) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept int128"),
        ));
    }

    let mut encoded = Vec::new();
    push_binary_bytes(&mut encoded, &encode_int128_payload_bytes(value));
    Ok(encoded)
}

/// Decode one direct int128 leaf through the Structural Binary v1 scalar lane.
pub(super) fn decode_int128_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Int128>, FieldDecodeError> {
    match decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        Some(Value::Int128(value)) => Ok(Some(value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(FieldDecodeError::new(
            "scalar field unexpectedly decoded as non-int128 value",
        )),
        None => Err(FieldDecodeError::new(
            "field kind is not owned by the scalar int128 fast path",
        )),
    }
}

/// Encode one direct nat128 leaf through the Structural Binary v1 scalar lane.
pub(super) fn encode_nat128_fast_path_binary_bytes(
    value: Nat128,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Uint128) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept nat128"),
        ));
    }

    let mut encoded = Vec::new();
    push_binary_bytes(&mut encoded, &encode_nat128_payload_bytes(value));
    Ok(encoded)
}

/// Decode one direct nat128 leaf through the Structural Binary v1 scalar lane.
pub(super) fn decode_nat128_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Nat128>, FieldDecodeError> {
    match decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        Some(Value::Uint128(value)) => Ok(Some(value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(FieldDecodeError::new(
            "scalar field unexpectedly decoded as non-nat128 value",
        )),
        None => Err(FieldDecodeError::new(
            "field kind is not owned by the scalar nat128 fast path",
        )),
    }
}

// Decode one binary scalar fast-path payload whose persisted shape is bytes.
fn decode_scalar_fast_path_binary_bytes_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    tag: u8,
    len: u32,
    payload_start: usize,
) -> Result<Value, FieldDecodeError> {
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: invalid type, expected bytes",
        ));
    }

    match kind {
        FieldKind::Blob => Ok(Value::Blob(
            binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?.to_vec(),
        )),
        FieldKind::Int128 => Ok(Value::Int128(decode_int128_payload_bytes(
            binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?,
        )?)),
        FieldKind::Uint128 => Ok(Value::Uint128(decode_nat128_payload_bytes(
            binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?,
        )?)),
        FieldKind::Ulid => Ok(Value::Ulid(decode_ulid_payload_bytes(
            binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?,
        )?)),
        _ => Err(FieldDecodeError::new(
            "scalar field unexpectedly routed to binary byte fast-path helper",
        )),
    }
}

// Decode one binary scalar fast-path payload whose persisted shape is text.
fn decode_scalar_fast_path_binary_text_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    tag: u8,
    len: u32,
    payload_start: usize,
) -> Result<Value, FieldDecodeError> {
    if tag != TAG_TEXT {
        return Err(FieldDecodeError::new(
            "structural binary: invalid type, expected text",
        ));
    }

    let text = decode_binary_text_scalar_bytes(raw_bytes, len, payload_start)?;
    match kind {
        FieldKind::Text { .. } => Ok(Value::Text(text.to_string())),
        _ => Err(FieldDecodeError::new(
            "scalar field unexpectedly routed to binary text fast-path helper",
        )),
    }
}

// Decode one binary scalar fast-path payload whose persisted shape is numeric
// or bool.
fn decode_scalar_fast_path_binary_numeric_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    tag: u8,
    len: u32,
    payload_start: usize,
) -> Result<Value, FieldDecodeError> {
    match kind {
        FieldKind::Bool => match tag {
            TAG_FALSE => Ok(Value::Bool(false)),
            TAG_TRUE => Ok(Value::Bool(true)),
            _ => Err(FieldDecodeError::new(
                "structural binary: invalid type, expected bool",
            )),
        },
        FieldKind::Float32 => {
            if tag != TAG_FLOAT32 || len != 4 {
                return Err(FieldDecodeError::new(
                    "structural binary: expected f32 float payload",
                ));
            }

            let value = decode_float32_payload_bytes(binary_payload_bytes(
                raw_bytes,
                len,
                payload_start,
                "float32",
            )?)?;

            Ok(Value::Float32(value))
        }
        FieldKind::Float64 => {
            if tag != TAG_FLOAT64 || len != 8 {
                return Err(FieldDecodeError::new(
                    "structural binary: expected f64 float payload",
                ));
            }

            let value = decode_float64_payload_bytes(binary_payload_bytes(
                raw_bytes,
                len,
                payload_start,
                "float64",
            )?)?;

            Ok(Value::Float64(value))
        }
        FieldKind::Int => {
            if tag != TAG_INT64 || len != 8 {
                return Err(FieldDecodeError::new(
                    "structural binary: expected i64 integer payload",
                ));
            }

            Ok(Value::Int(decode_i64_payload_bytes(
                binary_payload_bytes(raw_bytes, len, payload_start, "integer")?,
                "i64",
            )?))
        }
        FieldKind::Uint => {
            if tag != TAG_UINT64 || len != 8 {
                return Err(FieldDecodeError::new(
                    "structural binary: expected u64 integer payload",
                ));
            }

            Ok(Value::Uint(decode_u64_payload_bytes(
                binary_payload_bytes(raw_bytes, len, payload_start, "integer")?,
                "u64",
            )?))
        }
        _ => Err(FieldDecodeError::new(
            "scalar field unexpectedly routed to binary numeric fast-path helper",
        )),
    }
}
