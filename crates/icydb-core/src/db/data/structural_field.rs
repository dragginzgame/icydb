//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

use crate::{
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    traits::NumFromPrimitive,
    types::{Float32, Float64, Int, Int128, Nat, Nat128, Ulid},
    value::{Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use serde_cbor::{
    Value as CborValue, from_slice as cbor_from_slice, value::from_value as cbor_from_value,
};
use std::str;
use thiserror::Error as ThisError;

///
/// StructuralFieldDecodeError
///
/// StructuralFieldDecodeError captures one persisted-field structural decode
/// failure.
/// It keeps structural decode diagnostics local to the field boundary so row
/// and relation callers can map them into taxonomy-correct higher-level errors.
///

#[derive(Clone, Debug, ThisError)]
#[error("{message}")]
pub(in crate::db) struct StructuralFieldDecodeError {
    message: String,
}

impl StructuralFieldDecodeError {
    // Build one structural field-decode failure message.
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Decode one persisted field payload using the runtime storage-decode contract.
pub(in crate::db) fn decode_structural_field_value(
    raw_value: &CborValue,
    kind: FieldKind,
    storage_decode: FieldStorageDecode,
) -> Result<Value, StructuralFieldDecodeError> {
    let raw_value = super::unwrap_structural_row_cbor_tags(raw_value.clone());
    if matches!(raw_value, CborValue::Null) && !matches!(kind, FieldKind::Unit) {
        return Ok(Value::Null);
    }

    if matches!(storage_decode, FieldStorageDecode::Value) {
        return cbor_from_value::<Value>(raw_value).map_err(|err| {
            StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}"))
        });
    }

    match kind {
        FieldKind::Account => decode_typed_cbor_value(raw_value, Value::Account),
        FieldKind::Blob => decode_blob_value(raw_value),
        FieldKind::Bool => decode_bool_value(raw_value),
        FieldKind::Date => decode_typed_cbor_value(raw_value, Value::Date),
        FieldKind::Decimal { .. } => decode_typed_cbor_value(raw_value, Value::Decimal),
        FieldKind::Duration => decode_typed_cbor_value(raw_value, Value::Duration),
        FieldKind::Enum { path, variants } => decode_enum_value(raw_value, path, variants),
        FieldKind::Float32 => decode_float32_value(raw_value),
        FieldKind::Float64 => decode_float64_value(raw_value),
        FieldKind::Int => decode_int64_value(raw_value),
        FieldKind::Int128 => decode_int128_value(raw_value),
        FieldKind::IntBig => decode_typed_cbor_value(raw_value, Value::IntBig),
        FieldKind::Principal => decode_typed_cbor_value(raw_value, Value::Principal),
        FieldKind::Subaccount => decode_typed_cbor_value(raw_value, Value::Subaccount),
        FieldKind::Text => decode_text_value(raw_value),
        FieldKind::Timestamp => decode_typed_cbor_value(raw_value, Value::Timestamp),
        FieldKind::Uint => decode_uint64_value(raw_value),
        FieldKind::Uint128 => decode_uint128_value(raw_value),
        FieldKind::UintBig => decode_typed_cbor_value(raw_value, Value::UintBig),
        FieldKind::Ulid => decode_ulid_value(raw_value),
        FieldKind::Unit => decode_typed_cbor_value(raw_value, |()| Value::Unit),
        FieldKind::Relation { key_kind, .. } => {
            decode_structural_field_value(&raw_value, *key_kind, FieldStorageDecode::ByKind)
        }
        FieldKind::List(inner) | FieldKind::Set(inner) => decode_list_value(raw_value, *inner),
        FieldKind::Map { key, value } => decode_map_value(raw_value, *key, *value),
        FieldKind::Structured { .. } => Ok(Value::Null),
    }
}

/// Decode one encoded persisted field payload using the runtime storage-decode contract.
pub(in crate::db) fn decode_structural_field_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
    storage_decode: FieldStorageDecode,
) -> Result<Value, StructuralFieldDecodeError> {
    if !matches!(storage_decode, FieldStorageDecode::Value) {
        match kind {
            FieldKind::Enum { path, variants } => {
                return decode_enum_bytes(raw_bytes, path, variants);
            }
            FieldKind::List(inner) | FieldKind::Set(inner) => {
                return decode_list_bytes(raw_bytes, *inner);
            }
            FieldKind::Map { key, value } => {
                return decode_map_bytes(raw_bytes, *key, *value);
            }
            FieldKind::Relation { key_kind, .. } => {
                return decode_structural_field_bytes(
                    raw_bytes,
                    *key_kind,
                    FieldStorageDecode::ByKind,
                );
            }
            _ => {}
        }
    }

    let raw_value = cbor_from_slice::<CborValue>(raw_bytes).map_err(|err| {
        StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}"))
    })?;

    decode_structural_field_value(&raw_value, kind, storage_decode)
}

// Decode one list/set field directly from CBOR bytes and recurse only through
// the declared item contract.
fn decode_list_bytes(
    raw_bytes: &[u8],
    inner: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != 4 {
        return Err(StructuralFieldDecodeError::new(
            "expected CBOR array for list/set field",
        ));
    }

    let item_count = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("expected bounded CBOR array length"))?;
    let mut items = Vec::with_capacity(item_count);

    for _ in 0..item_count {
        let item_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        items.push(decode_structural_field_bytes(
            &raw_bytes[item_start..cursor],
            inner,
            FieldStorageDecode::ByKind,
        )?);
    }

    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after list/set field",
        ));
    }

    Ok(Value::List(items))
}

// Decode one map field directly from CBOR bytes and recurse only through the
// declared key/value contracts.
fn decode_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != 5 {
        return Err(StructuralFieldDecodeError::new(
            "expected CBOR map for map field",
        ));
    }

    let entry_count = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("expected bounded CBOR map length"))?;
    let mut entries = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        let key_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;

        entries.push((
            decode_structural_field_bytes(
                &raw_bytes[key_start..value_start],
                key_kind,
                FieldStorageDecode::ByKind,
            )?,
            decode_structural_field_bytes(
                &raw_bytes[value_start..cursor],
                value_kind,
                FieldStorageDecode::ByKind,
            )?,
        ));
    }

    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after map field",
        ));
    }

    Ok(normalize_map_entries_or_preserve(entries))
}

// Decode one enum field directly from CBOR bytes using the schema-declared
// variant payload contract when available.
fn decode_enum_bytes(
    raw_bytes: &[u8],
    path: &'static str,
    variants: &'static [EnumVariantModel],
) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    match major {
        3 => {
            let variant = decode_text_scalar_bytes(raw_bytes, argument, cursor)?;
            let text_len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: text too large")
            })?;
            cursor = cursor.checked_add(text_len).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
            })?;
            if cursor != raw_bytes.len() {
                return Err(StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: trailing bytes after enum field",
                ));
            }

            Ok(Value::Enum(ValueEnum::new(variant, Some(path))))
        }
        5 => {
            if argument != 1 {
                return Err(StructuralFieldDecodeError::new(
                    "expected one-entry CBOR map for enum payload variant",
                ));
            }

            let (variant, next_cursor) = parse_text_scalar_at(raw_bytes, cursor)?;
            cursor = next_cursor;
            let payload_start = cursor;
            cursor = skip_cbor_value(raw_bytes, cursor)?;
            if cursor != raw_bytes.len() {
                return Err(StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: trailing bytes after enum field",
                ));
            }
            let payload_bytes = &raw_bytes[payload_start..cursor];
            let payload =
                if let Some(variant_model) = variants.iter().find(|item| item.ident() == variant) {
                    if let Some(payload_kind) = variant_model.payload_kind() {
                        decode_structural_field_bytes(
                            payload_bytes,
                            *payload_kind,
                            variant_model.payload_storage_decode(),
                        )?
                    } else {
                        decode_untyped_enum_payload_bytes(payload_bytes)?
                    }
                } else {
                    decode_untyped_enum_payload_bytes(payload_bytes)?
                };

            Ok(Value::Enum(
                ValueEnum::new(variant, Some(path)).with_payload(payload),
            ))
        }
        _ => Err(StructuralFieldDecodeError::new(
            "expected text or one-entry CBOR map for enum field",
        )),
    }
}

// Decode one conservative enum payload directly from bytes.
//
// This keeps the fallback shallow: scalar payloads decode directly, and
// composite payloads decode only one structural level before degrading nested
// composites to `Null`.
fn decode_untyped_enum_payload_bytes(
    raw_bytes: &[u8],
) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    if let Some(value) = decode_untyped_scalar_bytes(raw_bytes, major, argument, payload_start)? {
        return Ok(value);
    }

    match major {
        4 => decode_untyped_list_bytes(raw_bytes, argument, payload_start),
        5 => decode_untyped_map_bytes(raw_bytes, argument, payload_start),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported enum payload CBOR shape",
        )),
    }
}

// Parse one tagged CBOR head into `(major, argument, payload_start)`.
fn parse_tagged_cbor_head(
    bytes: &[u8],
    mut cursor: usize,
) -> Result<Option<(u8, u64, usize)>, StructuralFieldDecodeError> {
    let Some((mut major, mut argument, mut next_cursor)) = parse_cbor_head(bytes, cursor)? else {
        return Ok(None);
    };

    while major == 6 {
        cursor = next_cursor;
        let Some((inner_major, inner_argument, inner_next_cursor)) =
            parse_cbor_head(bytes, cursor)?
        else {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: truncated tagged CBOR value",
            ));
        };
        major = inner_major;
        argument = inner_argument;
        next_cursor = inner_next_cursor;
    }

    Ok(Some((major, argument, next_cursor)))
}

// Parse one definite-length CBOR head.
fn parse_cbor_head(
    bytes: &[u8],
    cursor: usize,
) -> Result<Option<(u8, u64, usize)>, StructuralFieldDecodeError> {
    let Some(&first) = bytes.get(cursor) else {
        return Ok(None);
    };
    let major = first >> 5;
    let additional = first & 0x1f;
    let mut next_cursor = cursor + 1;

    let argument = match additional {
        value @ 0..=23 => u64::from(value),
        24 => {
            let value = *bytes.get(next_cursor).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 1;

            u64::from(value)
        }
        25 => {
            let payload = bytes.get(next_cursor..next_cursor + 2).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 2;

            u64::from(u16::from_be_bytes([payload[0], payload[1]]))
        }
        26 => {
            let payload = bytes.get(next_cursor..next_cursor + 4).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 4;

            u64::from(u32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))
        }
        27 => {
            let payload = bytes.get(next_cursor..next_cursor + 8).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 8;

            u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ])
        }
        31 => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: indefinite-length CBOR is unsupported",
            ));
        }
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: invalid CBOR additional info",
            ));
        }
    };

    Ok(Some((major, argument, next_cursor)))
}

// Skip one tagged CBOR value without rebuilding a `CborValue`.
fn skip_cbor_value(bytes: &[u8], cursor: usize) -> Result<usize, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    match major {
        0 | 1 | 7 => Ok(cursor),
        2 | 3 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR scalar too large")
            })?;
            cursor = cursor.checked_add(len).ok_or_else(|| {
                StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: CBOR scalar length overflow",
                )
            })?;
            if cursor > bytes.len() {
                return Err(StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: truncated CBOR scalar payload",
                ));
            }

            Ok(cursor)
        }
        4 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR array too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        5 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR map too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: unsupported CBOR major type",
        )),
    }
}

// Parse one tagged CBOR text scalar in place.
fn parse_text_scalar_at(
    bytes: &[u8],
    cursor: usize,
) -> Result<(&str, usize), StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: missing text scalar",
        ));
    };
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected a text string",
        ));
    }

    let text = decode_text_scalar_bytes(bytes, argument, payload_start)?;
    let text_len = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let next_cursor = payload_start.checked_add(text_len).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
    })?;

    Ok((text, next_cursor))
}

// Decode one definite-length CBOR text payload from the enclosing field bytes.
fn decode_text_scalar_bytes(
    bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<&str, StructuralFieldDecodeError> {
    let text_len = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let payload_end = payload_start.checked_add(text_len).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
    })?;
    let payload = bytes.get(payload_start..payload_end).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: truncated text payload")
    })?;

    str::from_utf8(payload).map_err(|_| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: non-utf8 text string")
    })
}

// Decode one typed persisted CBOR payload and map it into one runtime `Value`.
fn decode_typed_cbor_value<T>(
    raw_value: CborValue,
    into_value: impl FnOnce(T) -> Value,
) -> Result<Value, StructuralFieldDecodeError>
where
    T: serde::de::DeserializeOwned,
{
    cbor_from_value::<T>(raw_value)
        .map(into_value)
        .map_err(|err| StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}")))
}

// Decode one blob field using the persisted CBOR byte codec.
fn decode_blob_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Bytes(value) => Ok(Value::Blob(value)),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a byte string",
        ))),
    }
}

// Decode one text field directly from the persisted CBOR string scalar.
fn decode_text_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Text(value) => Ok(Value::Text(value)),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a text string",
        ))),
    }
}

// Decode one ULID field directly from the persisted CBOR text scalar.
fn decode_ulid_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Text(value) => Ulid::from_str(&value).map(Value::Ulid).map_err(|_| {
            StructuralFieldDecodeError::new("typed CBOR decode failed: invalid ulid string")
        }),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a text string",
        ))),
    }
}

// Decode one bool field directly from the persisted CBOR scalar.
fn decode_bool_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Bool(value) => Ok(Value::Bool(value)),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a bool",
        ))),
    }
}

// Decode one signed 64-bit integer field directly from the persisted CBOR scalar.
fn decode_int64_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Integer(value) => i64::try_from(value).map(Value::Int).map_err(|_| {
            StructuralFieldDecodeError::new(format!(
                "typed CBOR decode failed: integer {value} out of range for i64",
            ))
        }),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected an integer",
        ))),
    }
}

// Decode one unsigned 64-bit integer field directly from the persisted CBOR scalar.
fn decode_uint64_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Integer(value) => u64::try_from(value).map(Value::Uint).map_err(|_| {
            StructuralFieldDecodeError::new(format!(
                "typed CBOR decode failed: integer {value} out of range for u64",
            ))
        }),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected an integer",
        ))),
    }
}

// Decode one float32 field directly from the persisted CBOR float scalar.
fn decode_float32_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Float(value) => {
            if !value.is_finite() {
                return Err(StructuralFieldDecodeError::new(
                    "non-finite CBOR float payload",
                ));
            }
            if value < f64::from(f32::MIN) || value > f64::from(f32::MAX) {
                return Err(StructuralFieldDecodeError::new(
                    "CBOR float payload out of range for float32",
                ));
            }

            Float32::from_f64(value)
                .map(Value::Float32)
                .ok_or_else(|| StructuralFieldDecodeError::new("non-finite CBOR float payload"))
        }
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a float",
        ))),
    }
}

// Decode one float64 field directly from the persisted CBOR float scalar.
fn decode_float64_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Float(value) => Float64::try_new(value)
            .map(Value::Float64)
            .ok_or_else(|| StructuralFieldDecodeError::new("non-finite CBOR float payload")),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a float",
        ))),
    }
}

// Decode one signed 128-bit integer field directly from the persisted CBOR
// byte-string payload used by the wrapper type serialization contract.
fn decode_int128_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Bytes(value) => {
            let bytes: [u8; 16] = value.try_into().map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: expected 16 bytes")
            })?;

            Ok(Value::Int128(Int128::from(i128::from_be_bytes(bytes))))
        }
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a byte string",
        ))),
    }
}

// Decode one unsigned 128-bit integer field directly from the persisted CBOR
// byte-string payload used by the wrapper type serialization contract.
fn decode_uint128_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Bytes(value) => {
            let bytes: [u8; 16] = value.try_into().map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: expected 16 bytes")
            })?;

            Ok(Value::Uint128(Nat128::from(u128::from_be_bytes(bytes))))
        }
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a byte string",
        ))),
    }
}

// Decode one collection field into the canonical runtime list representation.
fn decode_list_value(
    raw_value: CborValue,
    inner: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let CborValue::Array(items) = raw_value else {
        return Err(StructuralFieldDecodeError::new(
            "expected CBOR array for list/set field",
        ));
    };

    // Keep repeated scalar containers on one direct path so list-heavy schema
    // shapes do not bounce through the full recursive field decoder for each
    // element.
    let items = match decode_scalar_list_items(&items, inner)? {
        Some(values) => values,
        None => items
            .iter()
            .map(|item| decode_structural_field_value(item, inner, FieldStorageDecode::ByKind))
            .collect::<Result<Vec<_>, _>>()?,
    };

    Ok(Value::List(items))
}

// Decode one persisted map field while preserving current `FieldValue::to_value`
// semantics: canonicalize when possible, but keep the raw decoded entry order
// when validation rejects the decoded entry shapes.
fn decode_map_value(
    raw_value: CborValue,
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let CborValue::Map(entries) = raw_value else {
        return Err(StructuralFieldDecodeError::new(
            "expected CBOR map for map field",
        ));
    };

    // Fast-path repeated scalar map shapes before falling back to the generic
    // per-entry recursive decode path.
    let entries = match decode_scalar_map_entries(&entries, key_kind, value_kind)? {
        Some(values) => values,
        None => entries
            .iter()
            .map(|(entry_key, entry_value)| {
                Ok((
                    decode_structural_field_value(entry_key, key_kind, FieldStorageDecode::ByKind)?,
                    decode_structural_field_value(
                        entry_value,
                        value_kind,
                        FieldStorageDecode::ByKind,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, StructuralFieldDecodeError>>()?,
    };

    Ok(normalize_map_entries_or_preserve(entries))
}

// Decode repeated scalar list/set shapes without re-entering the generic field
// decoder for every element.
fn decode_scalar_list_items(
    items: &[CborValue],
    inner: FieldKind,
) -> Result<Option<Vec<Value>>, StructuralFieldDecodeError> {
    if !supports_scalar_fast_path(inner) {
        return Ok(None);
    }

    items
        .iter()
        .map(|item| decode_scalar_fast_path_value(item.clone(), inner))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

// Decode repeated scalar map shapes without re-entering the generic field
// decoder for every key/value pair.
fn decode_scalar_map_entries(
    entries: &std::collections::BTreeMap<CborValue, CborValue>,
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Option<Vec<(Value, Value)>>, StructuralFieldDecodeError> {
    if !supports_scalar_fast_path(key_kind) || !supports_scalar_fast_path(value_kind) {
        return Ok(None);
    }

    entries
        .iter()
        .map(|(entry_key, entry_value)| {
            Ok((
                decode_scalar_fast_path_value(entry_key.clone(), key_kind)?,
                decode_scalar_fast_path_value(entry_value.clone(), value_kind)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

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

// Decode one scalar item using the direct-CBOR fast path shared by composite
// structural decode.
fn decode_scalar_fast_path_value(
    raw_value: CborValue,
    kind: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    match kind {
        FieldKind::Blob => decode_blob_value(raw_value),
        FieldKind::Bool => decode_bool_value(raw_value),
        FieldKind::Float32 => decode_float32_value(raw_value),
        FieldKind::Float64 => decode_float64_value(raw_value),
        FieldKind::Int => decode_int64_value(raw_value),
        FieldKind::Int128 => decode_int128_value(raw_value),
        FieldKind::Text => decode_text_value(raw_value),
        FieldKind::Uint => decode_uint64_value(raw_value),
        FieldKind::Uint128 => decode_uint128_value(raw_value),
        FieldKind::Ulid => decode_ulid_value(raw_value),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported scalar fast-path field kind",
        )),
    }
}

// Decode one enum field using the schema path plus the persisted CBOR enum
// shape. Unit variants arrive as text; data-carrying variants arrive as the
// canonical externally-tagged one-entry map.
fn decode_enum_value(
    raw_value: CborValue,
    path: &'static str,
    variants: &'static [EnumVariantModel],
) -> Result<Value, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Text(variant) => Ok(Value::Enum(ValueEnum::new(&variant, Some(path)))),
        CborValue::Map(entries) => {
            if entries.len() != 1 {
                return Err(StructuralFieldDecodeError::new(
                    "expected one-entry CBOR map for enum payload variant",
                ));
            }

            let mut entries = entries.into_iter();
            let Some((variant, payload)) = entries.next() else {
                return Err(StructuralFieldDecodeError::new(
                    "expected one-entry CBOR map for enum payload variant",
                ));
            };
            let CborValue::Text(variant) = super::unwrap_structural_row_cbor_tags(variant) else {
                return Err(StructuralFieldDecodeError::new(
                    "expected text enum variant tag",
                ));
            };
            let payload = if let Some(variant_model) =
                variants.iter().find(|item| item.ident() == variant)
            {
                if let Some(payload_kind) = variant_model.payload_kind() {
                    decode_structural_field_value(
                        &payload,
                        *payload_kind,
                        variant_model.payload_storage_decode(),
                    )?
                } else {
                    decode_untyped_enum_payload_value(super::unwrap_structural_row_cbor_tags(
                        payload,
                    ))?
                }
            } else {
                decode_untyped_enum_payload_value(super::unwrap_structural_row_cbor_tags(payload))?
            };

            Ok(Value::Enum(
                ValueEnum::new(&variant, Some(path)).with_payload(payload),
            ))
        }
        other => Err(StructuralFieldDecodeError::new(format!(
            "expected text or one-entry CBOR map for enum field, found {other:?}",
        ))),
    }
}

// Decode one enum payload through the remaining conservative untyped fallback.
//
// This path is intentionally no longer universal: it accepts scalar payloads
// directly and only decodes one structural level for composite payloads.
fn decode_untyped_enum_payload_value(
    raw_value: CborValue,
) -> Result<Value, StructuralFieldDecodeError> {
    let raw_value = super::unwrap_structural_row_cbor_tags(raw_value);
    if let Some(value) = decode_untyped_scalar_value(&raw_value)? {
        return Ok(value);
    }

    match raw_value {
        CborValue::Array(values) => decode_untyped_list_value(values),
        CborValue::Map(entries) => decode_untyped_map_value(entries),
        CborValue::__Hidden => Err(StructuralFieldDecodeError::new(
            "unsupported hidden CBOR value variant",
        )),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported enum payload CBOR shape",
        )),
    }
}

// Decode one untyped scalar payload into the closest runtime value.
fn decode_untyped_scalar_value(
    raw_value: &CborValue,
) -> Result<Option<Value>, StructuralFieldDecodeError> {
    match raw_value {
        CborValue::Null => Ok(Some(Value::Null)),
        CborValue::Bool(value) => Ok(Some(Value::Bool(*value))),
        CborValue::Integer(value) => Ok(Some(decode_untyped_integer(*value))),
        CborValue::Bytes(value) => Ok(Some(Value::Blob(value.clone()))),
        CborValue::Text(value) => Ok(Some(Value::Text(value.clone()))),
        CborValue::Float(value) => Ok(Some(Value::Float64(
            Float64::try_new(*value)
                .ok_or_else(|| StructuralFieldDecodeError::new("non-finite CBOR float payload"))?,
        ))),
        CborValue::Tag(_, inner) => decode_untyped_scalar_value(inner),
        CborValue::Array(_) | CborValue::Map(_) | CborValue::__Hidden => Ok(None),
    }
}

// Decode one untyped scalar payload directly from bytes.
fn decode_untyped_scalar_bytes(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Option<Value>, StructuralFieldDecodeError> {
    let value = match major {
        0 | 1 => Some(decode_untyped_integer(decode_cbor_integer(
            major, argument,
        )?)),
        2 => Some(Value::Blob(
            payload_bytes(raw_bytes, argument, payload_start, "byte string")?.to_vec(),
        )),
        3 => Some(Value::Text(
            decode_text_scalar_bytes(raw_bytes, argument, payload_start)?.to_string(),
        )),
        7 => match argument {
            20 => Some(Value::Bool(false)),
            21 => Some(Value::Bool(true)),
            22 => Some(Value::Null),
            26 | 27 => Some(Value::Float64(
                Float64::try_new(decode_cbor_float(raw_bytes, argument, payload_start)?)
                    .ok_or_else(|| {
                        StructuralFieldDecodeError::new("non-finite CBOR float payload")
                    })?,
            )),
            _ => None,
        },
        _ => None,
    };

    Ok(value)
}

// Decode one untyped list payload one level deep.
//
// Nested composite items are intentionally degraded to `Null` so this fallback
// no longer recursively rebuilds arbitrary `Value` trees.
fn decode_untyped_list_value(values: Vec<CborValue>) -> Result<Value, StructuralFieldDecodeError> {
    let values = values
        .into_iter()
        .map(decode_untyped_shallow_value)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Value::List(values))
}

// Decode one untyped list payload one level deep directly from bytes.
fn decode_untyped_list_bytes(
    raw_bytes: &[u8],
    argument: u64,
    mut cursor: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    let item_count = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("expected bounded CBOR array length"))?;
    let mut values = Vec::with_capacity(item_count);

    for _ in 0..item_count {
        let item_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        values.push(decode_untyped_shallow_bytes(
            &raw_bytes[item_start..cursor],
        )?);
    }

    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after enum payload array",
        ));
    }

    Ok(Value::List(values))
}

// Decode one untyped map payload one level deep.
//
// Nested composite keys/values are intentionally degraded to `Null` so this
// fallback no longer recursively rebuilds arbitrary `Value` trees.
fn decode_untyped_map_value(
    entries: std::collections::BTreeMap<CborValue, CborValue>,
) -> Result<Value, StructuralFieldDecodeError> {
    let values = entries
        .into_iter()
        .map(|(entry_key, entry_value)| {
            Ok((
                decode_untyped_shallow_value(entry_key)?,
                decode_untyped_shallow_value(entry_value)?,
            ))
        })
        .collect::<Result<Vec<_>, StructuralFieldDecodeError>>()?;

    Ok(normalize_map_entries_or_preserve(values))
}

// Decode one untyped map payload one level deep directly from bytes.
fn decode_untyped_map_bytes(
    raw_bytes: &[u8],
    argument: u64,
    mut cursor: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    let entry_count = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("expected bounded CBOR map length"))?;
    let mut values = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        let key_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        values.push((
            decode_untyped_shallow_bytes(&raw_bytes[key_start..value_start])?,
            decode_untyped_shallow_bytes(&raw_bytes[value_start..cursor])?,
        ));
    }

    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after enum payload map",
        ));
    }

    Ok(normalize_map_entries_or_preserve(values))
}

// Decode one fallback payload item without recursing into nested composites.
fn decode_untyped_shallow_value(raw_value: CborValue) -> Result<Value, StructuralFieldDecodeError> {
    let raw_value = super::unwrap_structural_row_cbor_tags(raw_value);
    if let Some(value) = decode_untyped_scalar_value(&raw_value)? {
        return Ok(value);
    }

    match raw_value {
        CborValue::Array(_) | CborValue::Map(_) => Ok(Value::Null),
        CborValue::__Hidden => Err(StructuralFieldDecodeError::new(
            "unsupported hidden CBOR value variant",
        )),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported enum payload CBOR shape",
        )),
    }
}

// Decode one fallback payload item without rebuilding nested composites.
fn decode_untyped_shallow_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if let Some(value) = decode_untyped_scalar_bytes(raw_bytes, major, argument, payload_start)? {
        return Ok(value);
    }

    match major {
        4 | 5 => Ok(Value::Null),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported enum payload CBOR shape",
        )),
    }
}

// Normalize decoded map entries in place when they satisfy the runtime map
// invariants, but preserve the original decoded order when validation rejects
// the shape. This keeps current semantics without cloning the whole entry list.
fn normalize_map_entries_or_preserve(mut entries: Vec<(Value, Value)>) -> Value {
    if Value::validate_map_entries(&entries).is_err() {
        return Value::Map(entries);
    }

    Value::sort_map_entries_in_place(entries.as_mut_slice());

    for i in 1..entries.len() {
        let (left_key, _) = &entries[i - 1];
        let (right_key, _) = &entries[i];
        if Value::canonical_cmp_key(left_key, right_key) == std::cmp::Ordering::Equal {
            return Value::Map(entries);
        }
    }

    Value::Map(entries)
}

// Decode one untyped CBOR integer into the narrowest deterministic runtime value.
fn decode_untyped_integer(value: i128) -> Value {
    if let Ok(value) = u64::try_from(value) {
        return Value::Uint(value);
    }
    if let Ok(value) = i64::try_from(value) {
        return Value::Int(value);
    }

    if value.is_negative() {
        Value::IntBig(Int::from(WrappedInt::from(value)))
    } else {
        Value::UintBig(Nat::from(WrappedNat::from(value.cast_unsigned())))
    }
}

// Borrow one definite-length payload slice from the original CBOR bytes.
fn payload_bytes<'a>(
    bytes: &'a [u8],
    argument: u64,
    payload_start: usize,
    expected: &'static str,
) -> Result<&'a [u8], StructuralFieldDecodeError> {
    let payload_len = usize::try_from(argument).map_err(|_| {
        StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {expected} too large"))
    })?;
    let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
        StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: {expected} length overflow"
        ))
    })?;
    let payload = bytes.get(payload_start..payload_end).ok_or_else(|| {
        StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {expected} payload"
        ))
    })?;
    if payload_end != bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after scalar payload",
        ));
    }

    Ok(payload)
}

// Decode one CBOR integer head into the shared signed authority.
fn decode_cbor_integer(major: u8, argument: u64) -> Result<i128, StructuralFieldDecodeError> {
    match major {
        0 => Ok(i128::from(argument)),
        1 => Ok(-1 - i128::from(argument)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected an integer",
        )),
    }
}

// Decode one CBOR float payload into the shared `f64` authority.
fn decode_cbor_float(
    bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<f64, StructuralFieldDecodeError> {
    let value = match argument {
        26 => {
            let payload = payload_bytes(bytes, 4, payload_start, "float")?;

            f64::from(f32::from_bits(u32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ])))
        }
        27 => {
            let payload = payload_bytes(bytes, 8, payload_start, "float")?;

            f64::from_bits(u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ]))
        }
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: expected a float",
            ));
        }
    };
    if !value.is_finite() {
        return Err(StructuralFieldDecodeError::new(
            "non-finite CBOR float payload",
        ));
    }

    Ok(value)
}
