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
use serde_cbor::{Value as CborValue, value::from_value as cbor_from_value};
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

// Normalize decoded map entries in place when they satisfy the runtime map
// invariants, but preserve the original decoded order when validation rejects
// the shape. This keeps current semantics without cloning the whole entry list.
fn normalize_map_entries_or_preserve(mut entries: Vec<(Value, Value)>) -> Value {
    if Value::validate_map_entries(&entries).is_err() {
        return Value::Map(entries);
    }

    entries.sort_by(|(left_key, _), (right_key, _)| Value::canonical_cmp_key(left_key, right_key));

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
