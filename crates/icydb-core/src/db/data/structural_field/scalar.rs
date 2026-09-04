//! Module: data::structural_field::scalar
//! Responsibility: direct scalar leaf decoding for `ByKind` fields that do not need composite recursion.
//! Does not own: container traversal, typed wrapper payloads, or `Value` storage decode.
//! Boundary: the structural-field root dispatches here before falling back to composite or typed wrapper lanes.

use crate::db::data::structural_field::FieldDecodeError;
use crate::db::data::structural_field::binary::{
    TAG_BYTES, TAG_FALSE, TAG_FLOAT32, TAG_FLOAT64, TAG_INT64, TAG_NAT64, TAG_TEXT, TAG_TRUE,
    decode_text_scalar_bytes as decode_binary_text_scalar_bytes,
    parse_complete_binary_value as parse_complete_structural_binary_value,
    payload_bytes as binary_payload_bytes, push_binary_bool, push_binary_bytes,
    push_binary_float32, push_binary_float64, push_binary_int64, push_binary_nat64,
    push_binary_null, push_binary_text,
};
use crate::db::data::structural_field::primitive::{
    decode_i64_payload_bytes, decode_u64_payload_bytes,
};
use crate::db::data::structural_field::typed::{
    decode_float32_payload_bytes, decode_float64_payload_bytes, decode_int128_payload_bytes,
    decode_nat128_payload_bytes, decode_ulid_payload_bytes, encode_int128_payload_bytes,
    encode_nat128_payload_bytes, encode_ulid_payload_bytes,
};
use crate::{db::schema::AcceptedFieldKind, error::InternalError, value::Value};

/// Keep the scalar fast path aligned with the Structural Binary v1 lane so the
/// structural-field root can hard-cut scalar owners without widening authority
/// over leaf or composite contracts.
pub(super) const fn supports_scalar_binary_fast_path(kind: &AcceptedFieldKind) -> bool {
    matches!(
        kind,
        AcceptedFieldKind::Blob { .. }
            | AcceptedFieldKind::Bool
            | AcceptedFieldKind::Float32
            | AcceptedFieldKind::Float64
            | AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64
            | AcceptedFieldKind::Int128
            | AcceptedFieldKind::Text { .. }
            | AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64
            | AcceptedFieldKind::Nat128
            | AcceptedFieldKind::Ulid
    )
}

/// Decode one scalar field through the canonical Structural Binary v1 scalar
/// lane.
pub(super) fn decode_scalar_fast_path_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    decode_scalar_fast_path_binary_bytes(raw_bytes, kind)
}

/// Decode one scalar field directly from Structural Binary v1 bytes.
pub(super) fn decode_scalar_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    if !supports_scalar_binary_fast_path(kind) {
        return Ok(None);
    }

    let (tag, len, payload_start) = parse_complete_structural_binary_value(raw_bytes)?;
    if tag == crate::db::data::structural_field::binary::TAG_NULL {
        return Ok(Some(Value::Null));
    }

    let value = match kind {
        AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::Ulid => {
            decode_scalar_fast_path_binary_bytes_kind(raw_bytes, kind, tag, len, payload_start)?
        }
        AcceptedFieldKind::Text { .. } => {
            decode_scalar_fast_path_binary_text_kind(raw_bytes, kind, tag, len, payload_start)?
        }
        AcceptedFieldKind::Bool
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => {
            decode_scalar_fast_path_binary_numeric_kind(raw_bytes, kind, tag, len, payload_start)?
        }
        _ => return Ok(None),
    };

    Ok(Some(value))
}

/// Validate one Structural Binary v1 scalar fast-path payload.
pub(super) fn validate_scalar_fast_path_binary_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<bool, FieldDecodeError> {
    if !supports_scalar_binary_fast_path(kind) {
        return Ok(false);
    }

    decode_scalar_fast_path_binary_bytes(raw_bytes, kind)?;
    Ok(true)
}

/// Encode one scalar field directly into Structural Binary v1 bytes.
pub(super) fn encode_scalar_fast_path_binary_bytes(
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<u8>>, InternalError> {
    if !supports_scalar_binary_fast_path(kind) {
        return Ok(None);
    }

    let mut encoded = Vec::new();
    match (kind, value) {
        (_, Value::Null) => push_binary_null(&mut encoded),
        (AcceptedFieldKind::Blob { .. }, Value::Blob(value)) => {
            push_binary_bytes(&mut encoded, value.as_slice());
        }
        (AcceptedFieldKind::Bool, Value::Bool(value)) => push_binary_bool(&mut encoded, *value),
        (AcceptedFieldKind::Float32, Value::Float32(value)) => {
            push_binary_float32(&mut encoded, value.get());
        }
        (AcceptedFieldKind::Float64, Value::Float64(value)) => {
            push_binary_float64(&mut encoded, value.get());
        }
        (AcceptedFieldKind::Int64, Value::Int64(value)) => {
            push_binary_int64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Int8, Value::Int64(value)) if i8::try_from(*value).is_ok() => {
            push_binary_int64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Int16, Value::Int64(value)) if i16::try_from(*value).is_ok() => {
            push_binary_int64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Int32, Value::Int64(value)) if i32::try_from(*value).is_ok() => {
            push_binary_int64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Int128, Value::Int128(value)) => {
            push_binary_bytes(&mut encoded, &encode_int128_payload_bytes(*value));
        }
        (AcceptedFieldKind::Text { .. }, Value::Text(value)) => {
            push_binary_text(&mut encoded, value);
        }
        (AcceptedFieldKind::Nat64, Value::Nat64(value)) => {
            push_binary_nat64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Nat8, Value::Nat64(value)) if u8::try_from(*value).is_ok() => {
            push_binary_nat64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Nat16, Value::Nat64(value)) if u16::try_from(*value).is_ok() => {
            push_binary_nat64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Nat32, Value::Nat64(value)) if u32::try_from(*value).is_ok() => {
            push_binary_nat64(&mut encoded, *value);
        }
        (AcceptedFieldKind::Nat128, Value::Nat128(value)) => {
            push_binary_bytes(&mut encoded, &encode_nat128_payload_bytes(*value));
        }
        (AcceptedFieldKind::Ulid, Value::Ulid(value)) => {
            push_binary_bytes(&mut encoded, &encode_ulid_payload_bytes(*value));
        }
        _ => {
            return Err(InternalError::persisted_row_field_encode_internal(
                field_name,
            ));
        }
    }

    Ok(Some(encoded))
}

// Decode one binary scalar fast-path payload whose persisted shape is bytes.
fn decode_scalar_fast_path_binary_bytes_kind(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
    tag: u8,
    len: u32,
    payload_start: usize,
) -> Result<Value, FieldDecodeError> {
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }

    match kind {
        AcceptedFieldKind::Blob { .. } => Ok(Value::Blob(
            binary_payload_bytes(raw_bytes, len, payload_start)?.to_vec(),
        )),
        AcceptedFieldKind::Int128 => Ok(Value::Int128(decode_int128_payload_bytes(
            binary_payload_bytes(raw_bytes, len, payload_start)?,
        )?)),
        AcceptedFieldKind::Nat128 => Ok(Value::Nat128(decode_nat128_payload_bytes(
            binary_payload_bytes(raw_bytes, len, payload_start)?,
        )?)),
        AcceptedFieldKind::Ulid => Ok(Value::Ulid(decode_ulid_payload_bytes(
            binary_payload_bytes(raw_bytes, len, payload_start)?,
        )?)),
        _ => Err(FieldDecodeError::new()),
    }
}

const fn fixed_int_kind_accepts_value(kind: &AcceptedFieldKind, value: i64) -> bool {
    match kind {
        AcceptedFieldKind::Int64 => true,
        AcceptedFieldKind::Int8 => value >= i8::MIN as i64 && value <= i8::MAX as i64,
        AcceptedFieldKind::Int16 => value >= i16::MIN as i64 && value <= i16::MAX as i64,
        AcceptedFieldKind::Int32 => value >= i32::MIN as i64 && value <= i32::MAX as i64,
        _ => false,
    }
}

const fn fixed_nat_kind_accepts_value(kind: &AcceptedFieldKind, value: u64) -> bool {
    match kind {
        AcceptedFieldKind::Nat64 => true,
        AcceptedFieldKind::Nat8 => value <= u8::MAX as u64,
        AcceptedFieldKind::Nat16 => value <= u16::MAX as u64,
        AcceptedFieldKind::Nat32 => value <= u32::MAX as u64,
        _ => false,
    }
}

// Decode one binary scalar fast-path payload whose persisted shape is text.
fn decode_scalar_fast_path_binary_text_kind(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
    tag: u8,
    len: u32,
    payload_start: usize,
) -> Result<Value, FieldDecodeError> {
    if tag != TAG_TEXT {
        return Err(FieldDecodeError::new());
    }

    let text = decode_binary_text_scalar_bytes(raw_bytes, len, payload_start)?;
    match kind {
        AcceptedFieldKind::Text { .. } => Ok(Value::Text(text.to_string())),
        _ => Err(FieldDecodeError::new()),
    }
}

// Decode one binary scalar fast-path payload whose persisted shape is numeric
// or bool.
fn decode_scalar_fast_path_binary_numeric_kind(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
    tag: u8,
    len: u32,
    payload_start: usize,
) -> Result<Value, FieldDecodeError> {
    match kind {
        AcceptedFieldKind::Bool => match tag {
            TAG_FALSE => Ok(Value::Bool(false)),
            TAG_TRUE => Ok(Value::Bool(true)),
            _ => Err(FieldDecodeError::new()),
        },
        AcceptedFieldKind::Float32 => {
            if tag != TAG_FLOAT32 || len != 4 {
                return Err(FieldDecodeError::new());
            }

            let value =
                decode_float32_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)?;

            Ok(Value::Float32(value))
        }
        AcceptedFieldKind::Float64 => {
            if tag != TAG_FLOAT64 || len != 8 {
                return Err(FieldDecodeError::new());
            }

            let value =
                decode_float64_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)?;

            Ok(Value::Float64(value))
        }
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64 => {
            if tag != TAG_INT64 || len != 8 {
                return Err(FieldDecodeError::new());
            }

            let value =
                decode_i64_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)?;
            if !fixed_int_kind_accepts_value(kind, value) {
                return Err(FieldDecodeError::new());
            }

            Ok(Value::Int64(value))
        }
        AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => {
            if tag != TAG_NAT64 || len != 8 {
                return Err(FieldDecodeError::new());
            }

            let value =
                decode_u64_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)?;
            if !fixed_nat_kind_accepts_value(kind, value) {
                return Err(FieldDecodeError::new());
            }

            Ok(Value::Nat64(value))
        }
        _ => Err(FieldDecodeError::new()),
    }
}
