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
            decode_structural_field_by_kind_bytes, encode_structural_field_by_kind_bytes,
            validate_structural_field_by_kind_bytes,
        },
        schema::AcceptedFieldKind,
    },
    error::InternalError,
    model::field::FieldKind,
    value::Value,
};

// Decode one accepted-schema by-kind field payload. Simple non-recursive kinds
// still reuse the existing generated-compatible decoder because their runtime
// shape has no borrowed nested metadata. Recursive kinds stay on accepted
// `AcceptedFieldKind` references throughout the traversal.
pub(in crate::db) fn decode_structural_field_by_accepted_kind_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Value, FieldDecodeError> {
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        return decode_structural_field_by_kind_bytes(raw_bytes, runtime_kind);
    }

    match kind {
        AcceptedFieldKind::Enum { .. } => Err(FieldDecodeError::new()),
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
        | AcceptedFieldKind::Structured { .. }
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
        | AcceptedFieldKind::Unit => unreachable!("simple accepted kinds are decoded above"),
    }
}

// Encode one accepted-schema by-kind field payload. Simple non-recursive kinds
// reuse the generated-compatible structural encoder after the accepted
// `AcceptedFieldKind` has selected the kind. Recursive shapes stay on
// accepted metadata throughout traversal.
pub(in crate::db) fn encode_structural_field_by_accepted_kind_bytes(
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        return encode_structural_field_by_kind_bytes(runtime_kind, value, field_name);
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
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        return validate_structural_field_by_kind_bytes(raw_bytes, runtime_kind);
    }

    match kind {
        AcceptedFieldKind::Enum { .. } => Err(FieldDecodeError::new()),
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
        | AcceptedFieldKind::Structured { .. }
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
        | AcceptedFieldKind::Unit => unreachable!("simple accepted kinds are validated above"),
    }
}

// Return whether one accepted persisted kind uses the storage-key binary lane.
// This mirrors the generated-kind lane so nullable structural-null detection
// can avoid treating storage-key nulls as value-storage null sentinels.
pub(in crate::db) fn accepted_kind_supports_primary_key_component_binary(
    kind: &AcceptedFieldKind,
) -> bool {
    match kind {
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit => true,
        AcceptedFieldKind::Relation { key_kind, .. } => {
            accepted_kind_supports_primary_key_component_binary(key_kind)
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            matches!(inner.as_ref(), AcceptedFieldKind::Relation { .. })
                && accepted_kind_supports_primary_key_component_binary(inner)
        }
        _ => false,
    }
}

// Adapt accepted field kinds that carry no borrowed nested metadata into the
// existing generated-compatible field-codec shape. The accepted
// `AcceptedFieldKind` remains the authority; this is only a leaf-codec reuse
// seam, not Rust-type inference. Recursive collections, relations, and enums
// stay in accepted-kind form throughout traversal.
const fn generated_compatible_simple_kind_from_accepted_kind(
    kind: &AcceptedFieldKind,
) -> Option<FieldKind> {
    match kind {
        AcceptedFieldKind::Account => Some(FieldKind::Account),
        AcceptedFieldKind::Blob { max_len } => Some(FieldKind::Blob { max_len: *max_len }),
        AcceptedFieldKind::Bool => Some(FieldKind::Bool),
        AcceptedFieldKind::Date => Some(FieldKind::Date),
        AcceptedFieldKind::Decimal { scale } => Some(FieldKind::Decimal { scale: *scale }),
        AcceptedFieldKind::Duration => Some(FieldKind::Duration),
        AcceptedFieldKind::Float32 => Some(FieldKind::Float32),
        AcceptedFieldKind::Float64 => Some(FieldKind::Float64),
        AcceptedFieldKind::Int64 => Some(FieldKind::Int64),
        AcceptedFieldKind::Int8 => Some(FieldKind::Int8),
        AcceptedFieldKind::Int16 => Some(FieldKind::Int16),
        AcceptedFieldKind::Int32 => Some(FieldKind::Int32),
        AcceptedFieldKind::Int128 => Some(FieldKind::Int128),
        AcceptedFieldKind::IntBig { max_bytes } => Some(FieldKind::IntBig {
            max_bytes: *max_bytes,
        }),
        AcceptedFieldKind::Principal => Some(FieldKind::Principal),
        AcceptedFieldKind::Structured { queryable } => Some(FieldKind::Structured {
            queryable: *queryable,
        }),
        AcceptedFieldKind::Subaccount => Some(FieldKind::Subaccount),
        AcceptedFieldKind::Text { max_len } => Some(FieldKind::Text { max_len: *max_len }),
        AcceptedFieldKind::Timestamp => Some(FieldKind::Timestamp),
        AcceptedFieldKind::Nat64 => Some(FieldKind::Nat64),
        AcceptedFieldKind::Nat8 => Some(FieldKind::Nat8),
        AcceptedFieldKind::Nat16 => Some(FieldKind::Nat16),
        AcceptedFieldKind::Nat32 => Some(FieldKind::Nat32),
        AcceptedFieldKind::Nat128 => Some(FieldKind::Nat128),
        AcceptedFieldKind::NatBig { max_bytes } => Some(FieldKind::NatBig {
            max_bytes: *max_bytes,
        }),
        AcceptedFieldKind::Ulid => Some(FieldKind::Ulid),
        AcceptedFieldKind::Unit => Some(FieldKind::Unit),
        AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::Set(_) => None,
    }
}

// Encode one accepted recursive field into Structural Binary v1 bytes.
fn encode_accepted_binary_field_into(
    out: &mut Vec<u8>,
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        let bytes = encode_structural_field_by_kind_bytes(runtime_kind, value, field_name)?;
        out.extend_from_slice(bytes.as_slice());
        return Ok(());
    }

    match kind {
        AcceptedFieldKind::Enum { .. } => Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        )),
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            encode_accepted_list_bytes(out, inner.as_ref(), value, field_name)
        }
        AcceptedFieldKind::Map { key, value: item } => {
            encode_accepted_map_bytes(out, key.as_ref(), item.as_ref(), value, field_name)
        }
        AcceptedFieldKind::Relation { key_kind, .. } => {
            encode_accepted_binary_field_into(out, key_kind.as_ref(), value, field_name)
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
        | AcceptedFieldKind::Structured { .. }
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
        | AcceptedFieldKind::Unit => unreachable!("simple accepted kinds are encoded above"),
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
// relation collections preserve generated-compatible relation-list behavior by
// skipping explicit null items.
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
