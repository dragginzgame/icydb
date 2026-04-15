//! Module: data::structural_field::encode
//! Responsibility: owner-local `ByKind` persisted field encoding.
//! Does not own: row layout orchestration, generic serde wire surfaces, or
//! externally tagged `Value` storage envelopes.
//! Boundary: persisted-row writers call into this file when they need the raw
//! CBOR bytes for one semantic field kind without routing through
//! `serde_cbor::Value`.

use crate::db::data::structural_field::value_storage::encode_structural_value_storage_bytes;
use crate::{
    db::data::structural_field::cbor::{
        push_array_len, push_bool, push_byte_string, push_float32, push_float64, push_map_len,
        push_null, push_signed_integer, push_text, push_unsigned_integer,
    },
    error::InternalError,
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    types::{Int, Nat},
    value::{Value, ValueEnum},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Encode one `ByKind` field payload into the raw CBOR shape expected by the
/// structural field decoder.
pub(in crate::db) fn encode_structural_field_by_kind_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_structural_field_by_kind_into(&mut encoded, kind, value, field_name)?;

    Ok(encoded)
}

// Encode one semantic field-kind payload directly into raw CBOR bytes.
fn encode_structural_field_by_kind_into(
    out: &mut Vec<u8>,
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    match (kind, value) {
        (_, Value::Null) | (FieldKind::Unit, Value::Unit) => push_null(out),
        (FieldKind::Blob, Value::Blob(value)) => push_byte_string(out, value.as_slice()),
        (FieldKind::Bool, Value::Bool(value)) => push_bool(out, *value),
        (FieldKind::Text, Value::Text(value)) => push_text(out, value),
        (FieldKind::Int, Value::Int(value)) => push_signed_integer(out, i128::from(*value)),
        (FieldKind::Uint, Value::Uint(value)) => push_unsigned_integer(out, u128::from(*value)),
        (FieldKind::Float32, Value::Float32(value)) => push_float32(out, value.get()),
        (FieldKind::Float64, Value::Float64(value)) => push_float64(out, value.get()),
        (FieldKind::Int128, Value::Int128(value)) => {
            push_byte_string(out, &value.get().to_be_bytes());
        }
        (FieldKind::Uint128, Value::Uint128(value)) => {
            push_byte_string(out, &value.get().to_be_bytes());
        }
        (FieldKind::Ulid, Value::Ulid(value)) => push_text(out, &value.to_string()),
        (FieldKind::Account, Value::Account(value)) => push_account_payload(out, *value),
        (FieldKind::Date, Value::Date(value)) => push_text(out, &value.to_string()),
        (FieldKind::Decimal { .. }, Value::Decimal(value)) => push_decimal_payload(out, *value),
        (FieldKind::Duration, Value::Duration(value)) => {
            push_unsigned_integer(out, u128::from(value.as_millis()));
        }
        (FieldKind::IntBig, Value::IntBig(value)) => push_int_big_payload(out, value),
        (FieldKind::Principal, Value::Principal(value)) => push_byte_string(out, value.as_slice()),
        (FieldKind::Subaccount, Value::Subaccount(value)) => push_subaccount_payload(out, *value),
        (FieldKind::Timestamp, Value::Timestamp(value)) => push_timestamp_payload(out, *value)?,
        (FieldKind::UintBig, Value::UintBig(value)) => push_uint_big_payload(out, value),
        (FieldKind::Relation { key_kind, .. }, value) => {
            encode_structural_field_by_kind_into(out, *key_kind, value, field_name)?;
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            push_array_len(out, items.len());
            for item in items {
                encode_structural_field_by_kind_into(out, *inner, item, field_name)?;
            }
        }
        (FieldKind::Map { key, value }, Value::Map(entries)) => {
            push_map_len(out, entries.len());
            for (entry_key, entry_value) in entries {
                encode_structural_field_by_kind_into(out, *key, entry_key, field_name)?;
                encode_structural_field_by_kind_into(out, *value, entry_value, field_name)?;
            }
        }
        (FieldKind::Enum { path, variants }, Value::Enum(value)) => {
            encode_enum_payload(out, path, variants, value, field_name)?;
        }
        (FieldKind::Structured { .. }, _) => {
            return Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                "structured ByKind field encoding is unsupported",
            ));
        }
        _ => {
            return Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                format!("field kind {kind:?} does not accept runtime value {value:?}"),
            ));
        }
    }

    Ok(())
}

// Encode one account payload using the stable two-field CBOR struct shape.
fn push_account_payload(out: &mut Vec<u8>, value: crate::types::Account) {
    push_map_len(out, 2);

    push_text(out, "owner");
    push_byte_string(out, value.owner().as_slice());

    push_text(out, "subaccount");
    match value.subaccount() {
        Some(subaccount) => push_subaccount_payload(out, subaccount),
        None => push_null(out),
    }
}

// Encode one decimal payload using the persisted binary `(mantissa, scale)`
// tuple shape.
fn push_decimal_payload(out: &mut Vec<u8>, value: crate::types::Decimal) {
    push_array_len(out, 2);
    push_byte_string(out, &value.mantissa().to_be_bytes());
    push_unsigned_integer(out, u128::from(value.scale()));
}

// Encode one arbitrary-precision signed integer as `(sign, limbs)`.
fn push_int_big_payload(out: &mut Vec<u8>, value: &Int) {
    let (negative, digits) = value.sign_and_u32_digits();
    let sign = if digits.is_empty() {
        0
    } else if negative {
        -1
    } else {
        1
    };

    push_array_len(out, 2);
    push_signed_integer(out, sign);
    push_uint_big_digits(out, digits.as_slice());
}

// Encode one arbitrary-precision unsigned integer as its base-2^32 limb array.
fn push_uint_big_payload(out: &mut Vec<u8>, value: &Nat) {
    let digits = value.u32_digits();
    push_uint_big_digits(out, digits.as_slice());
}

// Encode one base-2^32 limb sequence as the persisted CBOR array shape.
fn push_uint_big_digits(out: &mut Vec<u8>, digits: &[u32]) {
    push_array_len(out, digits.len());
    for digit in digits {
        push_unsigned_integer(out, u128::from(*digit));
    }
}

// Encode one subaccount using the stable derived `[u8; 32]` CBOR array shape.
fn push_subaccount_payload(out: &mut Vec<u8>, value: crate::types::Subaccount) {
    push_array_len(out, 32);
    for byte in value.as_slice() {
        push_unsigned_integer(out, u128::from(*byte));
    }
}

// Encode one timestamp payload using the persisted RFC3339 text form.
fn push_timestamp_payload(
    out: &mut Vec<u8>,
    value: crate::types::Timestamp,
) -> Result<(), InternalError> {
    let nanos = i128::from(value.as_millis()).saturating_mul(1_000_000);
    let dt = OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(InternalError::persisted_row_encode_failed)?;
    let rendered = dt
        .format(&Rfc3339)
        .map_err(InternalError::persisted_row_encode_failed)?;
    push_text(out, &rendered);

    Ok(())
}

// Encode one enum field using the same unit-vs-one-entry-map envelope expected
// by structural enum decode.
fn encode_enum_payload(
    out: &mut Vec<u8>,
    path: &'static str,
    variants: &'static [EnumVariantModel],
    value: &ValueEnum,
    field_name: &str,
) -> Result<(), InternalError> {
    if let Some(actual_path) = value.path()
        && actual_path != path
    {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("enum path mismatch: expected '{path}', found '{actual_path}'"),
        ));
    }

    let Some(payload) = value.payload() else {
        push_text(out, value.variant());
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

    push_map_len(out, 1);
    push_text(out, value.variant());
    match variant_model.payload_storage_decode() {
        FieldStorageDecode::ByKind => {
            encode_structural_field_by_kind_into(out, *payload_kind, payload, field_name)?;
        }
        FieldStorageDecode::Value => {
            out.extend_from_slice(&encode_structural_value_storage_bytes(payload)?);
        }
    }

    Ok(())
}
