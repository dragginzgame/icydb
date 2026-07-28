//! Module: data::structural_field::accepted
//! Responsibility: accepted-schema structural field encode, decode, and validation.
//! Does not own: generated model fallback, row layout selection, or schema mutation authority.
//! Boundary: consumes accepted field-kind metadata directly while preserving the structural payload grammar.

use crate::{
    db::{
        data::structural_field::{
            FieldDecodeError,
            binary::{
                push_binary_list_len, push_binary_map_len, walk_binary_list_items,
                walk_binary_map_entries,
            },
            leaf::{decode_leaf_field_by_kind_bytes, encode_leaf_field_binary_bytes},
            primary_key_component::supports_primary_key_component_binary_kind,
            scalar::{
                decode_scalar_fast_path_bytes, encode_scalar_fast_path_binary_bytes,
                validate_scalar_fast_path_binary_bytes,
            },
        },
        schema::AcceptedFieldKind,
    },
    error::InternalError,
    value::Value,
};

// Decode one accepted-schema by-kind field payload.
pub(in crate::db) fn decode_structural_field_by_accepted_kind_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Value, FieldDecodeError> {
    if let Some(value) = decode_scalar_fast_path_bytes(raw_bytes, kind)? {
        return Ok(value);
    }
    if !matches!(
        kind,
        AcceptedFieldKind::Composite { .. } | AcceptedFieldKind::Enum { .. }
    ) && let Some(value) = decode_leaf_field_by_kind_bytes(raw_bytes, kind)?
    {
        return Ok(value);
    }

    match kind {
        AcceptedFieldKind::Composite { .. } | AcceptedFieldKind::Enum { .. } => {
            Err(FieldDecodeError::new())
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            decode_accepted_list_bytes(raw_bytes, inner.as_ref())
        }
        AcceptedFieldKind::Map { key, value } => {
            decode_accepted_map_bytes(raw_bytes, key.as_ref(), value.as_ref())
        }
        AcceptedFieldKind::Relation { key_kind, .. } => {
            decode_structural_field_by_accepted_kind_bytes(raw_bytes, key_kind.as_ref())
        }
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. }
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit => Err(FieldDecodeError::new()),
    }
}

// Encode one accepted-schema by-kind field payload.
pub(in crate::db) fn encode_structural_field_by_accepted_kind_bytes(
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if let Some(encoded) = encode_scalar_fast_path_binary_bytes(kind, value, field_name)? {
        return Ok(encoded);
    }
    if !matches!(
        kind,
        AcceptedFieldKind::Composite { .. } | AcceptedFieldKind::Enum { .. }
    ) && let Some(encoded) = encode_leaf_field_binary_bytes(kind, value, field_name)?
    {
        return Ok(encoded);
    }

    let mut encoded = Vec::new();
    encode_accepted_binary_field_into(&mut encoded, kind, value, field_name)?;

    Ok(encoded)
}

// Validate one accepted-schema by-kind field payload. This mirrors the decode
// entrypoint so accepted row readers have a fail-closed validation seam before
// deciding whether to materialize the final runtime `Value`.
pub(in crate::db) fn validate_structural_field_by_accepted_kind_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<(), FieldDecodeError> {
    if validate_scalar_fast_path_binary_bytes(raw_bytes, kind)? {
        return Ok(());
    }
    if !matches!(
        kind,
        AcceptedFieldKind::Composite { .. } | AcceptedFieldKind::Enum { .. }
    ) && decode_leaf_field_by_kind_bytes(raw_bytes, kind)?.is_some()
    {
        return Ok(());
    }

    match kind {
        AcceptedFieldKind::Composite { .. } | AcceptedFieldKind::Enum { .. } => {
            Err(FieldDecodeError::new())
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            validate_accepted_list_bytes(raw_bytes, inner.as_ref())
        }
        AcceptedFieldKind::Map { key, value } => {
            validate_accepted_map_bytes(raw_bytes, key.as_ref(), value.as_ref())
        }
        AcceptedFieldKind::Relation { key_kind, .. } => {
            validate_structural_field_by_accepted_kind_bytes(raw_bytes, key_kind.as_ref())
        }
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. }
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit => Err(FieldDecodeError::new()),
    }
}

// Return whether one accepted persisted kind uses the storage-key binary lane.
// This mirrors the generated-kind lane so nullable structural-null detection
// can avoid treating storage-key nulls as value-storage null sentinels.
pub(in crate::db) fn accepted_kind_supports_primary_key_component_binary(
    kind: &AcceptedFieldKind,
) -> bool {
    supports_primary_key_component_binary_kind(kind)
}

// Encode one accepted recursive field into Structural Binary v1 bytes.
fn encode_accepted_binary_field_into(
    out: &mut Vec<u8>,
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    if let Some(bytes) = encode_scalar_fast_path_binary_bytes(kind, value, field_name)? {
        out.extend_from_slice(bytes.as_slice());
        return Ok(());
    }
    if !matches!(
        kind,
        AcceptedFieldKind::Composite { .. } | AcceptedFieldKind::Enum { .. }
    ) && let Some(bytes) = encode_leaf_field_binary_bytes(kind, value, field_name)?
    {
        out.extend_from_slice(bytes.as_slice());
        return Ok(());
    }

    match kind {
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            encode_accepted_list_bytes(out, inner.as_ref(), value, field_name)
        }
        AcceptedFieldKind::Map { key, value: item } => {
            encode_accepted_map_bytes(out, key.as_ref(), item.as_ref(), value, field_name)
        }
        AcceptedFieldKind::Relation { key_kind, .. } => {
            encode_accepted_binary_field_into(out, key_kind.as_ref(), value, field_name)
        }
        AcceptedFieldKind::Composite { .. }
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. }
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit => Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        )),
    }
}

// Decode one accepted list or set by recursively decoding each item slice.
fn decode_accepted_list_bytes(
    raw_bytes: &[u8],
    inner: &AcceptedFieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut items = Vec::new();
    walk_binary_list_items(raw_bytes, &mut |item_bytes| {
        let item = decode_structural_field_by_accepted_kind_bytes(item_bytes, inner)?;
        if matches!(inner, AcceptedFieldKind::Relation { .. }) && matches!(item, Value::Null) {
            return Ok(());
        }
        items.push(item);

        Ok(())
    })?;

    Ok(Value::List(items))
}

// Encode one accepted list or set by recursively encoding each item. Accepted
// relation collections omit explicit null items from their canonical storage
// representation.
fn encode_accepted_list_bytes(
    out: &mut Vec<u8>,
    inner: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::List(items) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };
    let skip_null_items = matches!(inner, AcceptedFieldKind::Relation { .. });
    let encoded_len = if skip_null_items {
        items
            .iter()
            .filter(|item| !matches!(item, Value::Null))
            .count()
    } else {
        items.len()
    };

    push_binary_list_len(out, encoded_len);
    for item in items {
        if skip_null_items && matches!(item, Value::Null) {
            continue;
        }
        encode_accepted_binary_field_into(out, inner, item, field_name)?;
    }

    Ok(())
}

// Validate one accepted list or set by recursively validating each item slice.
fn validate_accepted_list_bytes(
    raw_bytes: &[u8],
    inner: &AcceptedFieldKind,
) -> Result<(), FieldDecodeError> {
    walk_binary_list_items(raw_bytes, &mut |item_bytes| {
        validate_structural_field_by_accepted_kind_bytes(item_bytes, inner)
    })
}

// Encode one accepted map by recursively encoding each key/value pair.
fn encode_accepted_map_bytes(
    out: &mut Vec<u8>,
    key_kind: &AcceptedFieldKind,
    value_kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Map(entries) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    push_binary_map_len(out, entries.len());
    for (entry_key, entry_value) in entries {
        encode_accepted_binary_field_into(out, key_kind, entry_key, field_name)?;
        encode_accepted_binary_field_into(out, value_kind, entry_value, field_name)?;
    }

    Ok(())
}

// Decode one accepted map by recursively decoding each key/value slice pair.
fn decode_accepted_map_bytes(
    raw_bytes: &[u8],
    key_kind: &AcceptedFieldKind,
    value_kind: &AcceptedFieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut entries = Vec::new();
    walk_binary_map_entries(raw_bytes, &mut |key_bytes, value_bytes| {
        entries.push((
            decode_structural_field_by_accepted_kind_bytes(key_bytes, key_kind)?,
            decode_structural_field_by_accepted_kind_bytes(value_bytes, value_kind)?,
        ));

        Ok(())
    })?;

    Ok(Value::Map(entries))
}

// Validate one accepted map by recursively validating each key/value slice
// pair.
fn validate_accepted_map_bytes(
    raw_bytes: &[u8],
    key_kind: &AcceptedFieldKind,
    value_kind: &AcceptedFieldKind,
) -> Result<(), FieldDecodeError> {
    walk_binary_map_entries(raw_bytes, &mut |key_bytes, value_bytes| {
        validate_structural_field_by_accepted_kind_bytes(key_bytes, key_kind)?;
        validate_structural_field_by_accepted_kind_bytes(value_bytes, value_kind)
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
