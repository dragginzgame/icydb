//! Module: data::structural_field::accepted
//! Responsibility: accepted-schema structural field encode, decode, and validation.
//! Does not own: generated model fallback, row layout selection, or schema mutation authority.
//! Boundary: consumes accepted field-kind metadata directly while preserving the structural payload grammar.

use crate::{
    db::{
        data::structural_field::{
            FieldDecodeError,
            binary::{
                TAG_LIST, TAG_MAP, TAG_NULL, parse_binary_head, push_binary_list_len,
                push_binary_map_len, skip_binary_value,
            },
            leaf::{decode_leaf_field_by_kind_bytes, push_leaf_field_binary_bytes},
            scalar::{
                decode_scalar_fast_path_bytes, push_scalar_fast_path_binary_bytes,
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
    let (value, end) = decode_accepted_value_at(raw_bytes, 0, kind)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }
    Ok(value)
}

// Each child consumes its own frame; the root checks complete consumption.
fn decode_accepted_value_at(
    bytes: &[u8],
    offset: usize,
    kind: &AcceptedFieldKind,
) -> Result<(Value, usize), FieldDecodeError> {
    if let AcceptedFieldKind::Relation { key_kind, .. } = kind {
        return decode_accepted_value_at(bytes, offset, key_kind);
    }
    if bytes.get(offset) != Some(&TAG_NULL) {
        match kind {
            AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
                let (len, mut cursor) = accepted_collection_head(bytes, offset, TAG_LIST)?;
                let mut items = Vec::new();
                for _ in 0..len {
                    let (item, end) = decode_accepted_value_at(bytes, cursor, inner)?;
                    cursor = end;
                    if !matches!(inner.as_ref(), AcceptedFieldKind::Relation { .. })
                        || !matches!(item, Value::Null)
                    {
                        items.push(item);
                    }
                }
                return Ok((Value::List(items), cursor));
            }
            AcceptedFieldKind::Map { key, value } => {
                let (len, mut cursor) = accepted_collection_head(bytes, offset, TAG_MAP)?;
                let mut entries = Vec::new();
                for _ in 0..len {
                    let (key, end) = decode_accepted_value_at(bytes, cursor, key)?;
                    let (value, end) = decode_accepted_value_at(bytes, end, value)?;
                    entries.push((key, value));
                    cursor = end;
                }
                return Ok((Value::Map(entries), cursor));
            }
            _ => {}
        }
    }
    let end = skip_binary_value(bytes, offset)?;
    let raw = &bytes[offset..end];
    let value = if let Some(value) = decode_scalar_fast_path_bytes(raw, kind)? {
        value
    } else {
        decode_leaf_field_by_kind_bytes(raw, kind)?.ok_or_else(FieldDecodeError::new)?
    };
    Ok((value, end))
}

fn accepted_collection_head(
    bytes: &[u8],
    offset: usize,
    expected: u8,
) -> Result<(u32, usize), FieldDecodeError> {
    let (tag, len, cursor) = parse_binary_head(bytes, offset)?.ok_or_else(FieldDecodeError::new)?;
    if tag != expected {
        return Err(FieldDecodeError::new());
    }
    Ok((len, cursor))
}

// Encode one accepted-schema by-kind field payload.
pub(in crate::db) fn encode_structural_field_by_accepted_kind_bytes(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_accepted_binary_field_into(&mut encoded, kind, value)?;

    Ok(encoded)
}

// Validate one accepted-schema by-kind field payload. This mirrors the decode
// entrypoint so accepted row readers have a fail-closed validation seam before
// deciding whether to materialize the final runtime `Value`.
pub(in crate::db) fn validate_structural_field_by_accepted_kind_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<(), FieldDecodeError> {
    if validate_accepted_value_at(raw_bytes, 0, kind)? != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }
    Ok(())
}

// Validation keeps the same cursor discipline without allocating runtime lists.
fn validate_accepted_value_at(
    bytes: &[u8],
    offset: usize,
    kind: &AcceptedFieldKind,
) -> Result<usize, FieldDecodeError> {
    if let AcceptedFieldKind::Relation { key_kind, .. } = kind {
        return validate_accepted_value_at(bytes, offset, key_kind);
    }
    if bytes.get(offset) != Some(&TAG_NULL) {
        match kind {
            AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
                let (len, mut cursor) = accepted_collection_head(bytes, offset, TAG_LIST)?;
                for _ in 0..len {
                    cursor = validate_accepted_value_at(bytes, cursor, inner)?;
                }
                return Ok(cursor);
            }
            AcceptedFieldKind::Map { key, value } => {
                let (len, mut cursor) = accepted_collection_head(bytes, offset, TAG_MAP)?;
                for _ in 0..len {
                    cursor = validate_accepted_value_at(bytes, cursor, key)?;
                    cursor = validate_accepted_value_at(bytes, cursor, value)?;
                }
                return Ok(cursor);
            }
            _ => {}
        }
    }
    let end = skip_binary_value(bytes, offset)?;
    let raw = &bytes[offset..end];
    if !validate_scalar_fast_path_binary_bytes(raw, kind)?
        && decode_leaf_field_by_kind_bytes(raw, kind)?.is_none()
    {
        return Err(FieldDecodeError::new());
    }
    Ok(end)
}

// Encode one accepted recursive field into Structural Binary v1 bytes.
fn encode_accepted_binary_field_into(
    out: &mut Vec<u8>,
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    if push_scalar_fast_path_binary_bytes(out, kind, value)? {
        return Ok(());
    }
    if push_leaf_field_binary_bytes(out, kind, value)? {
        return Ok(());
    }

    match kind {
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            encode_accepted_list_bytes(out, inner.as_ref(), value)
        }
        AcceptedFieldKind::Map { key, value: item } => {
            encode_accepted_map_bytes(out, key.as_ref(), item.as_ref(), value)
        }
        AcceptedFieldKind::Relation { key_kind, .. } => {
            encode_accepted_binary_field_into(out, key_kind.as_ref(), value)
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
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => Err(InternalError::persisted_row_encode_internal()),
    }
}

// Encode one accepted list or set by recursively encoding each item. Accepted
// relation collections omit explicit null items from their canonical storage
// representation.
fn encode_accepted_list_bytes(
    out: &mut Vec<u8>,
    inner: &AcceptedFieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    let Value::List(items) = value else {
        return Err(InternalError::persisted_row_encode_internal());
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
        encode_accepted_binary_field_into(out, inner, item)?;
    }

    Ok(())
}

// Encode one accepted map by recursively encoding each key/value pair.
fn encode_accepted_map_bytes(
    out: &mut Vec<u8>,
    key_kind: &AcceptedFieldKind,
    value_kind: &AcceptedFieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    let Value::Map(entries) = value else {
        return Err(InternalError::persisted_row_encode_internal());
    };

    push_binary_map_len(out, entries.len());
    for (entry_key, entry_value) in entries {
        encode_accepted_binary_field_into(out, key_kind, entry_key)?;
        encode_accepted_binary_field_into(out, value_kind, entry_value)?;
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
