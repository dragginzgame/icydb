//! Module: data::structural_field::composite
//! Responsibility: recursive composite `ByKind` decode for lists, maps, enums, and relation re-entry.
//! Does not own: low-level structural binary parsing, scalar fast paths, or non-recursive typed leaves.
//! Boundary: the structural-field root routes composite kinds here after scalar and leaf lanes are ruled out.

use crate::db::data::structural_field::FieldDecodeError;
use crate::db::data::structural_field::binary::{
    push_binary_list_len, push_binary_map_len, walk_binary_list_items, walk_binary_map_entries,
};
use crate::db::data::structural_field::primary_key_component::{
    decode_primary_key_component_binary_value_bytes,
    encode_primary_key_component_binary_value_bytes,
    validate_primary_key_component_binary_value_bytes,
};
use crate::db::data::structural_field::scalar::{
    decode_scalar_fast_path_binary_bytes, encode_scalar_fast_path_binary_bytes,
    validate_scalar_fast_path_binary_bytes,
};
use crate::db::data::structural_field::value_storage::normalize_map_entries_or_preserve;
use crate::{error::InternalError, model::field::FieldKind, value::Value};

// Decode one list/set field directly from Structural Binary v1 bytes.
fn decode_binary_list_bytes(raw_bytes: &[u8], inner: FieldKind) -> Result<Value, FieldDecodeError> {
    let mut items = Vec::new();
    walk_binary_list_items(raw_bytes, &mut |item_bytes| {
        items.push(decode_structural_binary_field_by_kind_bytes(
            item_bytes, inner,
        )?);

        Ok(())
    })?;

    Ok(Value::List(items))
}

// Decode one map field directly from Structural Binary v1 bytes.
fn decode_binary_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut entries = Vec::new();
    walk_binary_map_entries(raw_bytes, &mut |key_bytes, value_bytes| {
        entries.push((
            decode_structural_binary_field_by_kind_bytes(key_bytes, key_kind)?,
            decode_structural_binary_field_by_kind_bytes(value_bytes, value_kind)?,
        ));

        Ok(())
    })?;

    Ok(normalize_map_entries_or_preserve(entries))
}

// Validate one list/set field directly from Structural Binary v1 bytes.
fn validate_binary_list_bytes(raw_bytes: &[u8], inner: FieldKind) -> Result<(), FieldDecodeError> {
    walk_binary_list_items(raw_bytes, &mut |item_bytes| {
        validate_structural_binary_field_by_kind_bytes(item_bytes, inner)
    })
}

// Validate one map field directly from Structural Binary v1 bytes.
fn validate_binary_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    walk_binary_map_entries(raw_bytes, &mut |key_bytes, value_bytes| {
        validate_structural_binary_field_by_kind_bytes(key_bytes, key_kind)?;
        validate_structural_binary_field_by_kind_bytes(value_bytes, value_kind)
    })
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
        FieldKind::List(inner) | FieldKind::Set(inner) => {
            decode_binary_list_bytes(raw_bytes, *inner)
        }
        FieldKind::Map { key, value } => decode_binary_map_bytes(raw_bytes, *key, *value),
        FieldKind::Relation { key_kind, .. } => {
            decode_structural_binary_field_by_kind_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Enum { .. }
        | FieldKind::Account
        | FieldKind::Blob { .. }
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int8
        | FieldKind::Int16
        | FieldKind::Int32
        | FieldKind::Int64
        | FieldKind::Int128
        | FieldKind::IntBig { .. }
        | FieldKind::Principal
        | FieldKind::Composite { .. }
        | FieldKind::Subaccount
        | FieldKind::Text { .. }
        | FieldKind::Timestamp
        | FieldKind::Nat8
        | FieldKind::Nat16
        | FieldKind::Nat32
        | FieldKind::Nat64
        | FieldKind::Nat128
        | FieldKind::NatBig { .. }
        | FieldKind::Ulid
        | FieldKind::Unit => Err(FieldDecodeError::new()),
    }
}

// Validate one recursive composite `ByKind` field payload from Structural
// Binary v1 bytes.
pub(super) fn validate_composite_field_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    match kind {
        FieldKind::List(inner) | FieldKind::Set(inner) => {
            validate_binary_list_bytes(raw_bytes, *inner)
        }
        FieldKind::Map { key, value } => validate_binary_map_bytes(raw_bytes, *key, *value),
        FieldKind::Relation { key_kind, .. } => {
            validate_structural_binary_field_by_kind_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Enum { .. }
        | FieldKind::Account
        | FieldKind::Blob { .. }
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int8
        | FieldKind::Int16
        | FieldKind::Int32
        | FieldKind::Int64
        | FieldKind::Int128
        | FieldKind::IntBig { .. }
        | FieldKind::Principal
        | FieldKind::Composite { .. }
        | FieldKind::Subaccount
        | FieldKind::Text { .. }
        | FieldKind::Timestamp
        | FieldKind::Nat8
        | FieldKind::Nat16
        | FieldKind::Nat32
        | FieldKind::Nat64
        | FieldKind::Nat128
        | FieldKind::NatBig { .. }
        | FieldKind::Ulid
        | FieldKind::Unit => Err(FieldDecodeError::new()),
    }
}

// Decode one field through the parallel Structural Binary v1 by-kind lane.
fn decode_structural_binary_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    if let Some(value) = decode_primary_key_component_binary_value_bytes(raw_bytes, kind)? {
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
    if validate_primary_key_component_binary_value_bytes(raw_bytes, kind)? {
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
    if let Some(encoded) = encode_primary_key_component_binary_value_bytes(kind, value, field_name)?
    {
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
                return Err(InternalError::persisted_row_field_encode_internal(
                    field_name,
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
                return Err(InternalError::persisted_row_field_encode_internal(
                    field_name,
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
        FieldKind::Relation { key_kind, .. } => {
            encode_structural_binary_field_by_kind_into(out, *key_kind, value, field_name)?;
        }
        _ => {
            return Err(InternalError::persisted_row_field_encode_internal(
                field_name,
            ));
        }
    }

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
    use crate::{model::field::FieldKind, value::Value};

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
            value: &FieldKind::Nat64,
        };
        let value = Value::Map(vec![
            (Value::Text("alpha".to_string()), Value::Nat64(1)),
            (Value::Text("beta".to_string()), Value::Nat64(2)),
        ]);
        let encoded = encode_composite_field_binary_bytes(kind, &value, "entries")
            .expect("binary composite map should encode");
        let decoded = decode_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite map should decode");
        validate_composite_field_binary_bytes(&encoded, kind)
            .expect("binary composite map should validate");

        assert_eq!(decoded, value);
    }
}
