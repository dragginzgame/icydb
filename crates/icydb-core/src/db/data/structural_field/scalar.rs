//! Module: data::structural_field::scalar
//! Responsibility: direct scalar leaf decoding for `ByKind` fields that do not need composite recursion.
//! Does not own: container traversal, typed wrapper payloads, or `Value` storage decode.
//! Boundary: the structural-field root dispatches here before falling back to composite or typed wrapper lanes.

use crate::db::data::structural_field::StructuralFieldDecodeError;
use crate::db::data::structural_field::cbor::{
    decode_cbor_float, decode_cbor_integer, decode_text_scalar_bytes, parse_tagged_cbor_head,
    payload_bytes, skip_cbor_value,
};
use crate::{
    model::field::FieldKind,
    traits::NumFromPrimitive,
    types::{Float32, Float64, Int128, Nat128, Ulid},
    value::Value,
};

// Keep one narrow list/map fast-path whitelist so composite decode only skips
// the generic field dispatcher for truly direct scalar cases.
const fn supports_scalar_fast_path(kind: FieldKind) -> bool {
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

// Decode one scalar field directly from persisted CBOR bytes without
// rebuilding an intermediate `CborValue`.
pub(super) fn decode_scalar_fast_path_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Value>, StructuralFieldDecodeError> {
    if !supports_scalar_fast_path(kind) {
        return Ok(None);
    }

    // Phase 1: parse one bounded scalar payload and preserve explicit nulls.
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after scalar payload",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(Some(Value::Null));
    }

    // Phase 2: decode the declared scalar kind directly from the payload bytes.
    decode_scalar_fast_path_value(raw_bytes, kind, major, argument, payload_start)
}

// Decode one non-null scalar fast-path payload by scalar family.
fn decode_scalar_fast_path_value(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Option<Value>, StructuralFieldDecodeError> {
    let value = match kind {
        FieldKind::Blob | FieldKind::Int128 | FieldKind::Uint128 => {
            decode_scalar_fast_path_bytes_kind(raw_bytes, kind, major, argument, payload_start)?
        }
        FieldKind::Text | FieldKind::Ulid => {
            decode_scalar_fast_path_text_kind(raw_bytes, kind, major, argument, payload_start)?
        }
        FieldKind::Bool
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Uint => {
            decode_scalar_fast_path_numeric_kind(raw_bytes, kind, major, argument, payload_start)?
        }
        _ => return Ok(None),
    };

    Ok(Some(value))
}

// Decode one scalar fast-path payload whose persisted shape is bytes.
fn decode_scalar_fast_path_bytes_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 2 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a byte string",
        ));
    }

    match kind {
        FieldKind::Blob => Ok(Value::Blob(
            payload_bytes(raw_bytes, argument, payload_start, "byte string")?.to_vec(),
        )),
        FieldKind::Int128 => {
            let bytes: [u8; 16] = payload_bytes(raw_bytes, argument, payload_start, "byte string")?
                .try_into()
                .map_err(|_| {
                    StructuralFieldDecodeError::new("typed CBOR decode failed: expected 16 bytes")
                })?;

            Ok(Value::Int128(Int128::from(i128::from_be_bytes(bytes))))
        }
        FieldKind::Uint128 => {
            let bytes: [u8; 16] = payload_bytes(raw_bytes, argument, payload_start, "byte string")?
                .try_into()
                .map_err(|_| {
                    StructuralFieldDecodeError::new("typed CBOR decode failed: expected 16 bytes")
                })?;

            Ok(Value::Uint128(Nat128::from(u128::from_be_bytes(bytes))))
        }
        _ => Err(StructuralFieldDecodeError::new(
            "scalar field unexpectedly routed to byte fast-path helper",
        )),
    }
}

// Decode one scalar fast-path payload whose persisted shape is text.
fn decode_scalar_fast_path_text_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a text string",
        ));
    }

    let text = decode_text_scalar_bytes(raw_bytes, argument, payload_start)?;
    match kind {
        FieldKind::Text => Ok(Value::Text(text.to_string())),
        FieldKind::Ulid => Ok(Value::Ulid(Ulid::from_str(text).map_err(|_| {
            StructuralFieldDecodeError::new("typed CBOR decode failed: invalid ulid string")
        })?)),
        _ => Err(StructuralFieldDecodeError::new(
            "scalar field unexpectedly routed to text fast-path helper",
        )),
    }
}

// Decode one scalar fast-path payload whose persisted shape is numeric or bool.
fn decode_scalar_fast_path_numeric_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    match kind {
        FieldKind::Bool => match (major, argument) {
            (7, 20) => Ok(Value::Bool(false)),
            (7, 21) => Ok(Value::Bool(true)),
            _ => Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: invalid type, expected a bool",
            )),
        },
        FieldKind::Float32 => {
            decode_scalar_fast_path_float32(raw_bytes, major, argument, payload_start)
        }
        FieldKind::Float64 => {
            decode_scalar_fast_path_float64(raw_bytes, major, argument, payload_start)
        }
        FieldKind::Int => {
            let integer = decode_cbor_integer(major, argument)?;
            Ok(Value::Int(i64::try_from(integer).map_err(|_| {
                StructuralFieldDecodeError::new(format!(
                    "typed CBOR decode failed: integer {integer} out of range for i64",
                ))
            })?))
        }
        FieldKind::Uint => {
            let integer = decode_cbor_integer(major, argument)?;
            Ok(Value::Uint(u64::try_from(integer).map_err(|_| {
                StructuralFieldDecodeError::new(format!(
                    "typed CBOR decode failed: integer {integer} out of range for u64",
                ))
            })?))
        }
        _ => Err(StructuralFieldDecodeError::new(
            "scalar field unexpectedly routed to numeric fast-path helper",
        )),
    }
}

// Decode one float32 scalar fast-path payload.
fn decode_scalar_fast_path_float32(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 7 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a float",
        ));
    }

    let value = decode_cbor_float(raw_bytes, argument, payload_start)?;
    if value < f64::from(f32::MIN) || value > f64::from(f32::MAX) {
        return Err(StructuralFieldDecodeError::new(
            "CBOR float payload out of range for float32",
        ));
    }

    Ok(Value::Float32(Float32::from_f64(value).ok_or_else(
        || StructuralFieldDecodeError::new("non-finite CBOR float payload"),
    )?))
}

// Decode one float64 scalar fast-path payload.
fn decode_scalar_fast_path_float64(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 7 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a float",
        ));
    }

    Ok(Value::Float64(
        Float64::try_new(decode_cbor_float(raw_bytes, argument, payload_start)?)
            .ok_or_else(|| StructuralFieldDecodeError::new("non-finite CBOR float payload"))?,
    ))
}
