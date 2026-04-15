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
use crate::{
    error::InternalError,
    model::field::FieldKind,
    types::{Float32, Float64, Int128, Nat128, Ulid},
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
            | FieldKind::Text
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
        FieldKind::Text => {
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

    let _ = decode_scalar_fast_path_binary_bytes(raw_bytes, kind)?;
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
            push_binary_bytes(&mut encoded, &value.get().to_be_bytes());
        }
        (FieldKind::Text, Value::Text(value)) => push_binary_text(&mut encoded, value),
        (FieldKind::Uint, Value::Uint(value)) => push_binary_uint64(&mut encoded, *value),
        (FieldKind::Uint128, Value::Uint128(value)) => {
            push_binary_bytes(&mut encoded, &value.get().to_be_bytes());
        }
        (FieldKind::Ulid, Value::Ulid(value)) => {
            push_binary_bytes(&mut encoded, &value.to_bytes());
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
        FieldKind::Int128 => {
            let bytes: [u8; 16] =
                binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?
                    .try_into()
                    .map_err(|_| FieldDecodeError::new("structural binary: expected 16 bytes"))?;

            Ok(Value::Int128(Int128::from(i128::from_be_bytes(bytes))))
        }
        FieldKind::Uint128 => {
            let bytes: [u8; 16] =
                binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?
                    .try_into()
                    .map_err(|_| FieldDecodeError::new("structural binary: expected 16 bytes"))?;

            Ok(Value::Uint128(Nat128::from(u128::from_be_bytes(bytes))))
        }
        FieldKind::Ulid => {
            let bytes: [u8; 16] =
                binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?
                    .try_into()
                    .map_err(|_| FieldDecodeError::new("structural binary: expected 16 bytes"))?;

            Ok(Value::Ulid(Ulid::from_bytes(bytes)))
        }
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
        FieldKind::Text => Ok(Value::Text(text.to_string())),
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

            let value = Float32::try_from_bytes(binary_payload_bytes(
                raw_bytes,
                len,
                payload_start,
                "float32",
            )?)
            .map_err(|_| FieldDecodeError::new("structural binary: non-finite f32 payload"))?;

            Ok(Value::Float32(value))
        }
        FieldKind::Float64 => {
            if tag != TAG_FLOAT64 || len != 8 {
                return Err(FieldDecodeError::new(
                    "structural binary: expected f64 float payload",
                ));
            }

            let value = Float64::try_from_bytes(binary_payload_bytes(
                raw_bytes,
                len,
                payload_start,
                "float64",
            )?)
            .map_err(|_| FieldDecodeError::new("structural binary: non-finite f64 payload"))?;

            Ok(Value::Float64(value))
        }
        FieldKind::Int => {
            if tag != TAG_INT64 || len != 8 {
                return Err(FieldDecodeError::new(
                    "structural binary: expected i64 integer payload",
                ));
            }
            let bytes: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
                .try_into()
                .map_err(|_| FieldDecodeError::new("structural binary: invalid i64 payload"))?;

            Ok(Value::Int(i64::from_be_bytes(bytes)))
        }
        FieldKind::Uint => {
            if tag != TAG_UINT64 || len != 8 {
                return Err(FieldDecodeError::new(
                    "structural binary: expected u64 integer payload",
                ));
            }
            let bytes: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
                .try_into()
                .map_err(|_| FieldDecodeError::new("structural binary: invalid u64 payload"))?;

            Ok(Value::Uint(u64::from_be_bytes(bytes)))
        }
        _ => Err(FieldDecodeError::new(
            "scalar field unexpectedly routed to binary numeric fast-path helper",
        )),
    }
}
