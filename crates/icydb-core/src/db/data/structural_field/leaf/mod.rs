//! Module: data::structural_field::leaf
//! Responsibility: typed wrapper and structured leaf decoding that still has fixed payload semantics.
//! Does not own: scalar primitive fast paths, composite recursion, or `Value` storage envelopes.
//! Boundary: sibling modules use this file for leaf contracts like decimal, duration, and date.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        decode_binary_decimal_payload, decode_binary_required_i64, decode_binary_required_u64,
        push_binary_decimal_payload, push_binary_int64, push_binary_nat64,
    },
    primary_key_component::{
        decode_primary_key_component_binary_value_bytes,
        push_primary_key_component_binary_value_bytes,
    },
    typed::{
        decode_date_payload_days, decode_duration_payload_millis, encode_date_payload_days,
        encode_duration_payload_millis,
    },
};
use crate::{db::schema::AcceptedFieldKind, error::InternalError, value::Value};

/// Decode one non-recursive leaf `ByKind` field payload through the canonical
/// Structural Binary v1 leaf lane.
pub(super) fn decode_leaf_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    let value = match kind {
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => {
            let Some(value) = decode_primary_key_component_binary_value_bytes(raw_bytes, kind)?
            else {
                return Err(FieldDecodeError::new());
            };
            value
        }
        AcceptedFieldKind::Date => decode_date_value_bytes(raw_bytes)?,
        AcceptedFieldKind::Decimal { scale } => decode_decimal_value_bytes(raw_bytes, *scale)?,
        AcceptedFieldKind::Duration => decode_duration_value_bytes(raw_bytes)?,
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
        | AcceptedFieldKind::Ulid => {
            return Err(FieldDecodeError::new());
        }
        AcceptedFieldKind::Composite { .. }
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::Set(_) => return Ok(None),
    };

    Ok(Some(value))
}

/// Encode one non-recursive leaf `ByKind` field payload through the canonical
/// Structural Binary v1 leaf lane.
pub(super) fn push_leaf_field_binary_bytes(
    out: &mut Vec<u8>,
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Result<bool, InternalError> {
    match kind {
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => {
            return push_primary_key_component_binary_value_bytes(out, kind, value);
        }
        AcceptedFieldKind::Date => push_date_value_bytes(out, value)?,
        AcceptedFieldKind::Decimal { .. } => push_decimal_value_bytes(out, value)?,
        AcceptedFieldKind::Duration => push_duration_value_bytes(out, value)?,
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
        | AcceptedFieldKind::Composite { .. }
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::Set(_) => return Ok(false),
    }

    Ok(true)
}

// Decode one date payload from its canonical signed day-count form.
fn decode_date_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_date_payload_days(decode_binary_required_i64(raw_bytes)?).map(Value::Date)
}

// Decode one Decimal through its shared fixed payload owner.
fn decode_decimal_value_bytes(raw_bytes: &[u8], scale: u32) -> Result<Value, FieldDecodeError> {
    let value = decode_binary_decimal_payload(raw_bytes)?;
    if value.scale() != scale {
        return Err(FieldDecodeError::new());
    }
    Ok(Value::Decimal(value))
}

// Decode one duration payload from its canonical millis form.
fn decode_duration_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    Ok(Value::Duration(decode_duration_payload_millis(
        decode_binary_required_u64(raw_bytes)?,
    )))
}

// Encode one date payload into canonical signed day-count form.
fn push_date_value_bytes(out: &mut Vec<u8>, value: &Value) -> Result<(), InternalError> {
    let Value::Date(value) = value else {
        return Err(InternalError::persisted_row_encode_internal());
    };

    push_binary_int64(out, encode_date_payload_days(*value));
    Ok(())
}

// Encode one Decimal with its shared fixed payload and generic byte frame.
fn push_decimal_value_bytes(out: &mut Vec<u8>, value: &Value) -> Result<(), InternalError> {
    let Value::Decimal(value) = value else {
        return Err(InternalError::persisted_row_encode_internal());
    };

    push_binary_decimal_payload(out, *value);

    Ok(())
}

// Encode one duration payload into canonical millis.
fn push_duration_value_bytes(out: &mut Vec<u8>, value: &Value) -> Result<(), InternalError> {
    let Value::Duration(value) = value else {
        return Err(InternalError::persisted_row_encode_internal());
    };

    push_binary_nat64(out, encode_duration_payload_millis(*value));
    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
