//! Module: data::structural_field::composite
//! Responsibility: recursive composite `ByKind` decode for lists, maps, enums, and relation re-entry.
//! Does not own: low-level structural binary parsing, scalar fast paths, or non-recursive typed leaves.
//! Boundary: the structural-field root routes composite kinds here after scalar and leaf lanes are ruled out.

use crate::db::data::structural_field::binary::{
    push_binary_list_len, push_binary_map_len, push_binary_variant_payload,
    push_binary_variant_unit, split_binary_variant_payload, walk_binary_list_items,
    walk_binary_map_entries,
};
use crate::db::data::structural_field::scalar::{
    decode_scalar_fast_path_binary_bytes, encode_scalar_fast_path_binary_bytes,
    validate_scalar_fast_path_binary_bytes,
};
use crate::db::data::structural_field::storage_key::{
    decode_storage_key_binary_value_bytes, encode_storage_key_binary_value_bytes,
    validate_storage_key_binary_value_bytes,
};
use crate::db::data::structural_field::value_storage::{
    encode_structural_value_storage_bytes, normalize_map_entries_or_preserve,
    validate_structural_value_storage_bytes,
};
use crate::db::data::structural_field::{FieldDecodeError, decode_structural_value_storage_bytes};
use crate::{
    error::InternalError,
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    value::{Value, ValueEnum},
};
use std::str;

///
/// KindArrayDecodeState
///
/// KindArrayDecodeState carries the recursive list/set decode buffer together
/// with the declared inner field contract.
///

type KindArrayDecodeState = (Vec<Value>, FieldKind);

///
/// KindMapDecodeState
///
/// KindMapDecodeState carries the recursive map decode buffer together with
/// the declared key/value field contracts.
///

type KindMapDecodeState = (Vec<(Value, Value)>, FieldKind, FieldKind);

// Carry the declared item contract while the validate-only list/set walker
// checks each recursive element without allocating a `Vec<Value>`.
type KindArrayValidateState = FieldKind;

// Carry the declared key/value contracts while the validate-only map walker
// checks each recursive entry without allocating a `Vec<(Value, Value)>`.
type KindMapValidateState = (FieldKind, FieldKind);

// Push one binary by-kind list item into the decoded runtime value buffer.
//
// Safety:
// `context` must be a valid `KindArrayDecodeState`.
fn push_kind_binary_array_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<KindArrayDecodeState>() };
    state.0.push(decode_structural_binary_field_by_kind_bytes(
        item_bytes, state.1,
    )?);

    Ok(())
}

// Push one binary by-kind map entry into the decoded runtime entry buffer.
//
// Safety:
// `context` must be a valid `KindMapDecodeState`.
fn push_kind_binary_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<KindMapDecodeState>() };
    state.0.push((
        decode_structural_binary_field_by_kind_bytes(key_bytes, state.1)?,
        decode_structural_binary_field_by_kind_bytes(value_bytes, state.2)?,
    ));

    Ok(())
}

// Validate one binary by-kind list item recursively without allocating a
// decode buffer.
//
// Safety:
// `context` must be a valid `KindArrayValidateState`.
fn validate_kind_binary_array_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let kind = unsafe { *context.cast::<KindArrayValidateState>() };

    validate_structural_binary_field_by_kind_bytes(item_bytes, kind)
}

// Validate one binary by-kind map entry recursively without allocating
// decoded runtime keys or values.
//
// Safety:
// `context` must be a valid `KindMapValidateState`.
fn validate_kind_binary_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let (key_kind, value_kind) = unsafe { *context.cast::<KindMapValidateState>() };
    validate_structural_binary_field_by_kind_bytes(key_bytes, key_kind)?;
    validate_structural_binary_field_by_kind_bytes(value_bytes, value_kind)
}

// Decode one list/set field directly from Structural Binary v1 bytes.
fn decode_binary_list_bytes(raw_bytes: &[u8], inner: FieldKind) -> Result<Value, FieldDecodeError> {
    let mut state = (Vec::new(), inner);
    walk_binary_list_items(
        raw_bytes,
        "expected Structural Binary list for list/set field",
        "structural binary: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_kind_binary_array_item,
    )?;

    Ok(Value::List(state.0))
}

// Decode one map field directly from Structural Binary v1 bytes.
fn decode_binary_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut state = (Vec::new(), key_kind, value_kind);
    walk_binary_map_entries(
        raw_bytes,
        "expected Structural Binary map for map field",
        "structural binary: trailing bytes after map field",
        (&raw mut state).cast(),
        push_kind_binary_map_entry,
    )?;

    Ok(normalize_map_entries_or_preserve(state.0))
}

// Validate one list/set field directly from Structural Binary v1 bytes.
fn validate_binary_list_bytes(raw_bytes: &[u8], inner: FieldKind) -> Result<(), FieldDecodeError> {
    let mut state = inner;
    walk_binary_list_items(
        raw_bytes,
        "expected Structural Binary list for list/set field",
        "structural binary: trailing bytes after list/set field",
        (&raw mut state).cast(),
        validate_kind_binary_array_item,
    )
}

// Validate one map field directly from Structural Binary v1 bytes.
fn validate_binary_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    let mut state = (key_kind, value_kind);
    walk_binary_map_entries(
        raw_bytes,
        "expected Structural Binary map for map field",
        "structural binary: trailing bytes after map field",
        (&raw mut state).cast(),
        validate_kind_binary_map_entry,
    )
}

// Decode one enum field directly from Structural Binary v1 bytes using the
// schema-declared variant payload contract when available.
fn decode_binary_enum_bytes(
    raw_bytes: &[u8],
    path: &'static str,
    variants: &'static [EnumVariantModel],
) -> Result<Value, FieldDecodeError> {
    let (variant_bytes, payload_bytes) = split_binary_variant_payload(
        raw_bytes,
        "structural binary: truncated enum field",
        "expected Structural Binary variant for enum field",
        "structural binary: trailing bytes after enum field",
    )?;
    let variant = str::from_utf8(variant_bytes)
        .map_err(|_| FieldDecodeError::new("structural binary: enum label must be UTF-8"))?;

    if let Some(payload_bytes) = payload_bytes {
        let payload = if let Some(variant_model) =
            variants.iter().find(|item| item.ident() == variant)
        {
            if let Some(payload_kind) = variant_model.payload_kind() {
                match variant_model.payload_storage_decode() {
                    FieldStorageDecode::ByKind => {
                        decode_structural_binary_field_by_kind_bytes(payload_bytes, *payload_kind)?
                    }
                    FieldStorageDecode::Value => {
                        decode_structural_value_storage_bytes(payload_bytes)?
                    }
                }
            } else {
                return Err(FieldDecodeError::new(
                    "structural binary untyped enum payload is unsupported",
                ));
            }
        } else {
            return Err(FieldDecodeError::new(
                "structural binary untyped enum payload is unsupported",
            ));
        };

        Ok(Value::Enum(
            ValueEnum::new(variant, Some(path)).with_payload(payload),
        ))
    } else {
        Ok(Value::Enum(ValueEnum::new(variant, Some(path))))
    }
}

// Validate one enum field directly from Structural Binary v1 bytes.
fn validate_binary_enum_bytes(
    raw_bytes: &[u8],
    variants: &'static [EnumVariantModel],
) -> Result<(), FieldDecodeError> {
    let (variant_bytes, payload_bytes) = split_binary_variant_payload(
        raw_bytes,
        "structural binary: truncated enum field",
        "expected Structural Binary variant for enum field",
        "structural binary: trailing bytes after enum field",
    )?;
    let variant = str::from_utf8(variant_bytes)
        .map_err(|_| FieldDecodeError::new("structural binary: enum label must be UTF-8"))?;
    let Some(payload_bytes) = payload_bytes else {
        return Ok(());
    };

    if let Some(variant_model) = variants.iter().find(|item| item.ident() == variant)
        && let Some(payload_kind) = variant_model.payload_kind()
    {
        return match variant_model.payload_storage_decode() {
            FieldStorageDecode::ByKind => {
                validate_structural_binary_field_by_kind_bytes(payload_bytes, *payload_kind)
            }
            FieldStorageDecode::Value => validate_structural_value_storage_bytes(payload_bytes),
        };
    }

    Err(FieldDecodeError::new(
        "structural binary untyped enum payload is unsupported",
    ))
}

// Encode one recursive `ByKind` field payload into Structural Binary v1 bytes.
pub(in crate::db::data::structural_field) fn encode_composite_field_binary_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_structural_binary_field_by_kind_into(&mut encoded, kind, value, field_name)?;

    Ok(encoded)
}

// Decode one recursive composite `ByKind` field payload from Structural
// Binary v1 bytes.
pub(super) fn decode_composite_field_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    match kind {
        FieldKind::Enum { path, variants } => decode_binary_enum_bytes(raw_bytes, path, variants),
        FieldKind::List(inner) | FieldKind::Set(inner) => {
            decode_binary_list_bytes(raw_bytes, *inner)
        }
        FieldKind::Map { key, value } => decode_binary_map_bytes(raw_bytes, *key, *value),
        FieldKind::Relation { key_kind, .. } => {
            decode_structural_binary_field_by_kind_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Account
        | FieldKind::Blob { .. }
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text { .. }
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => Err(FieldDecodeError::new(
            "leaf field unexpectedly routed through binary composite decode",
        )),
    }
}

// Validate one recursive composite `ByKind` field payload from Structural
// Binary v1 bytes.
pub(super) fn validate_composite_field_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    match kind {
        FieldKind::Enum { variants, .. } => validate_binary_enum_bytes(raw_bytes, variants),
        FieldKind::List(inner) | FieldKind::Set(inner) => {
            validate_binary_list_bytes(raw_bytes, *inner)
        }
        FieldKind::Map { key, value } => validate_binary_map_bytes(raw_bytes, *key, *value),
        FieldKind::Relation { key_kind, .. } => {
            validate_structural_binary_field_by_kind_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Account
        | FieldKind::Blob { .. }
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text { .. }
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => Err(FieldDecodeError::new(
            "leaf field unexpectedly routed through binary composite validate",
        )),
    }
}

// Decode one field through the parallel Structural Binary v1 by-kind lane.
fn decode_structural_binary_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    if let Some(value) = decode_storage_key_binary_value_bytes(raw_bytes, kind)? {
        return Ok(value);
    }
    if let Some(value) = decode_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    decode_composite_field_binary_bytes(raw_bytes, kind)
}

// Validate one field through the parallel Structural Binary v1 by-kind lane.
fn validate_structural_binary_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    if validate_storage_key_binary_value_bytes(raw_bytes, kind)? {
        return Ok(());
    }
    if validate_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        return Ok(());
    }

    validate_composite_field_binary_bytes(raw_bytes, kind)
}

// Encode one field through the parallel Structural Binary v1 by-kind lane.
fn encode_structural_binary_field_by_kind_into(
    out: &mut Vec<u8>,
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    if let Some(encoded) = encode_storage_key_binary_value_bytes(kind, value, field_name)? {
        out.extend_from_slice(encoded.as_slice());
        return Ok(());
    }
    if let Some(encoded) = encode_scalar_fast_path_binary_bytes(kind, value, field_name)? {
        out.extend_from_slice(encoded.as_slice());
        return Ok(());
    }

    match kind {
        FieldKind::List(inner) | FieldKind::Set(inner) => {
            let Value::List(items) = value else {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    format!("field kind {kind:?} does not accept runtime value {value:?}"),
                ));
            };
            push_binary_list_len(out, items.len());
            for item in items {
                encode_structural_binary_field_by_kind_into(out, *inner, item, field_name)?;
            }
        }
        FieldKind::Map {
            key,
            value: value_kind,
        } => {
            let Value::Map(entries) = value else {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    format!("field kind {kind:?} does not accept runtime value {value:?}"),
                ));
            };
            push_binary_map_len(out, entries.len());
            for (entry_key, entry_value) in entries {
                encode_structural_binary_field_by_kind_into(out, *key, entry_key, field_name)?;
                encode_structural_binary_field_by_kind_into(
                    out,
                    *value_kind,
                    entry_value,
                    field_name,
                )?;
            }
        }
        FieldKind::Enum { path, variants } => {
            encode_binary_enum_payload(out, path, variants, value, field_name)?;
        }
        FieldKind::Relation { key_kind, .. } => {
            encode_structural_binary_field_by_kind_into(out, *key_kind, value, field_name)?;
        }
        _ => {
            return Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                format!("field kind {kind:?} is unsupported in binary composite lane"),
            ));
        }
    }

    Ok(())
}

// Encode one enum field into the parallel Structural Binary v1 lane.
fn encode_binary_enum_payload(
    out: &mut Vec<u8>,
    path: &'static str,
    variants: &'static [EnumVariantModel],
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Enum(value) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("enum field '{path}' does not accept runtime value {value:?}"),
        ));
    };

    if let Some(actual_path) = value.path()
        && actual_path != path
    {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("enum path mismatch: expected '{path}', found '{actual_path}'"),
        ));
    }

    let Some(payload) = value.payload() else {
        push_binary_variant_unit(out, value.variant());
        return Ok(());
    };

    let Some(variant_model) = variants.iter().find(|item| item.ident() == value.variant()) else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "unknown enum variant '{}' for path '{path}'",
                value.variant()
            ),
        ));
    };
    let Some(payload_kind) = variant_model.payload_kind() else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "enum variant '{}' does not accept a payload",
                value.variant()
            ),
        ));
    };
    if matches!(
        variant_model.payload_storage_decode(),
        FieldStorageDecode::Value
    ) {
        let payload_bytes = encode_structural_value_storage_bytes(payload)?;
        push_binary_variant_payload(out, value.variant(), payload_bytes.as_slice());

        return Ok(());
    }

    let mut payload_bytes = Vec::new();
    encode_structural_binary_field_by_kind_into(
        &mut payload_bytes,
        *payload_kind,
        payload,
        field_name,
    )?;
    push_binary_variant_payload(out, value.variant(), payload_bytes.as_slice());

    Ok(())
}

/// Decode one recursive composite `ByKind` field payload.
///
/// Composite decode owns all recursive re-entry back into the structural-field
/// boundary. Leaf kinds are intentionally rejected here so the root stays a
/// thin lane router instead of a mixed recursive hub.
pub(super) fn decode_composite_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    decode_composite_field_binary_bytes(raw_bytes, kind)
}

/// Validate one recursive composite `ByKind` field payload without eagerly
/// rebuilding its runtime `Value`.
pub(super) fn validate_composite_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    validate_composite_field_binary_bytes(raw_bytes, kind)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        decode_composite_field_binary_bytes, encode_composite_field_binary_bytes,
        validate_composite_field_binary_bytes,
    };
    use crate::{
        model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
        value::{Value, ValueEnum},
    };

    static STATE_VARIANTS: &[EnumVariantModel] = &[EnumVariantModel::new(
        "Loaded",
        Some(&FieldKind::Uint),
        FieldStorageDecode::ByKind,
    )];

    #[test]
    fn binary_composite_list_roundtrips_scalar_items() {
        let kind = FieldKind::List(&FieldKind::Text { max_len: None });
        let value = Value::List(vec![
            Value::Text("left".to_string()),
            Value::Text("right".to_string()),
        ]);
        let encoded = encode_composite_field_binary_bytes(kind, &value, "items")
            .expect("binary composite list should encode");
        let decoded = decode_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite list should decode");
        validate_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite list should validate");

        assert_eq!(decoded, value);
    }

    #[test]
    fn binary_composite_map_roundtrips_scalar_entries() {
        let kind = FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::Uint,
        };
        let value = Value::Map(vec![
            (Value::Text("alpha".to_string()), Value::Uint(1)),
            (Value::Text("beta".to_string()), Value::Uint(2)),
        ]);
        let encoded = encode_composite_field_binary_bytes(kind, &value, "entries")
            .expect("binary composite map should encode");
        let decoded = decode_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite map should decode");
        validate_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite map should validate");

        assert_eq!(decoded, value);
    }

    #[test]
    fn binary_composite_enum_roundtrips_typed_payload() {
        let kind = FieldKind::Enum {
            path: "State",
            variants: STATE_VARIANTS,
        };
        let value =
            Value::Enum(ValueEnum::new("Loaded", Some("State")).with_payload(Value::Uint(7)));
        let encoded = encode_composite_field_binary_bytes(kind, &value, "state")
            .expect("binary composite enum should encode");
        let decoded = decode_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite enum should decode");
        validate_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite enum should validate");

        assert_eq!(decoded, value);
    }
}
