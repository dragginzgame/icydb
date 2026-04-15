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
        push_account_payload, push_array_len, push_bool, push_byte_string, push_decimal_payload,
        push_float32, push_float64, push_int_big_payload, push_map_len, push_null,
        push_signed_integer, push_subaccount_payload, push_text, push_timestamp_payload,
        push_uint_big_payload, push_unsigned_integer,
    },
    error::InternalError,
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    value::{Value, ValueEnum},
};

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
