//! Canonical accepted-value encoding for `FieldStorageDecode::CatalogValue` payloads.
//!
//! Non-enum values retain the existing Structural Binary v1 representation.
//! Enums use the store-local type/variant ID envelope.

use crate::{
    db::{
        data::structural_field::{
            FieldDecodeError,
            binary::{
                TAG_LIST, TAG_MAP, parse_binary_head, push_binary_bool, push_binary_bytes,
                push_binary_int64, push_binary_list_len, push_binary_map_len, push_binary_nat64,
                push_binary_null, push_binary_text, push_binary_unit,
            },
            value_storage::{
                decode_structural_value_storage_bytes,
                encode::{
                    push_account_payload, push_date_payload, push_decimal_payload,
                    push_duration_payload, push_float32_payload, push_float64_payload,
                    push_int_big_payload, push_int128_payload, push_nat_big_payload,
                    push_nat128_payload, push_principal_payload, push_subaccount_payload,
                    push_timestamp_payload, push_u256_payload, push_ulid_payload,
                },
                skip::skip_value_storage_binary_value,
            },
        },
        schema::{
            MAX_ACCEPTED_RECURSIVE_DEPTH_U16,
            enum_catalog::{
                CanonicalEnumWireError, CanonicalValue, decode_canonical_enum_value,
                push_canonical_enum_value,
            },
        },
    },
    error::InternalError,
    value::Value,
};

const CANONICAL_ENUM_HEADER_BYTES: usize = 14;
const CANONICAL_ENUM_VALUE_TAG: u8 = 0x84;

/// Encode one accepted canonical value into a single recursive destination.
pub(in crate::db) fn encode_canonical_value_storage_bytes(
    value: &CanonicalValue,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    push_canonical_value_storage(&mut encoded, value, 0)?;
    Ok(encoded)
}

// Each nested value appends to its root buffer; depth admission precedes writes.
fn push_canonical_value_storage(
    out: &mut Vec<u8>,
    value: &CanonicalValue,
    depth: u16,
) -> Result<(), InternalError> {
    ensure_depth(depth).map_err(|_| InternalError::persisted_row_encode_internal())?;
    match value {
        CanonicalValue::Account(value) => push_account_payload(out, *value)?,
        CanonicalValue::Blob(value) => push_binary_bytes(out, value),
        CanonicalValue::Bool(value) => push_binary_bool(out, *value),
        CanonicalValue::Date(value) => push_date_payload(out, *value),
        CanonicalValue::Decimal(value) => push_decimal_payload(out, *value),
        CanonicalValue::Duration(value) => push_duration_payload(out, *value),
        CanonicalValue::Enum(value) => {
            push_canonical_enum_value(out, value.canonical(), |payload, encoded| {
                push_canonical_value_storage(encoded, payload, depth.saturating_add(1))
                    .map_err(|_| CanonicalEnumWireError::PayloadCodec)
            })
            .map_err(|_| InternalError::persisted_row_encode_internal())?;
        }
        CanonicalValue::Float32(value) => push_float32_payload(out, *value),
        CanonicalValue::Float64(value) => push_float64_payload(out, *value),
        CanonicalValue::Int64(value) => push_binary_int64(out, *value),
        CanonicalValue::Int128(value) => push_int128_payload(out, *value),
        CanonicalValue::IntBig(value) => push_int_big_payload(out, value),
        CanonicalValue::List(items) => {
            push_binary_list_len(out, items.len());
            for item in items {
                push_canonical_value_storage(out, item, depth.saturating_add(1))?;
            }
        }
        CanonicalValue::Map(entries) => {
            push_binary_map_len(out, entries.len());
            for (key, value) in entries {
                push_canonical_value_storage(out, key, depth.saturating_add(1))?;
                push_canonical_value_storage(out, value, depth.saturating_add(1))?;
            }
        }
        CanonicalValue::Null => push_binary_null(out),
        CanonicalValue::Principal(value) => push_principal_payload(out, *value)?,
        CanonicalValue::Subaccount(value) => push_subaccount_payload(out, *value),
        CanonicalValue::Text(value) => push_binary_text(out, value),
        CanonicalValue::Timestamp(value) => push_timestamp_payload(out, *value),
        CanonicalValue::Nat64(value) => push_binary_nat64(out, *value),
        CanonicalValue::Nat128(value) => push_nat128_payload(out, *value),
        CanonicalValue::NatBig(value) => push_nat_big_payload(out, value),
        CanonicalValue::Ulid(value) => push_ulid_payload(out, *value),
        CanonicalValue::Unit => push_binary_unit(out),
        CanonicalValue::U256(value) => push_u256_payload(out, *value),
    }
    Ok(())
}

/// Decode one current-format accepted canonical value fail-closed.
pub(in crate::db) fn decode_canonical_value_storage_bytes(
    encoded: &[u8],
) -> Result<CanonicalValue, FieldDecodeError> {
    let end = skip_canonical_value(encoded, 0, 0)?;
    if end != encoded.len() {
        return Err(FieldDecodeError::new());
    }
    decode_canonical_value_storage(encoded, 0)
}

fn decode_canonical_value_storage(
    encoded: &[u8],
    depth: u16,
) -> Result<CanonicalValue, FieldDecodeError> {
    ensure_depth(depth)?;
    let Some(tag) = encoded.first().copied() else {
        return Err(FieldDecodeError::new());
    };

    if tag == CANONICAL_ENUM_VALUE_TAG {
        return decode_canonical_enum_value(encoded, |payload| {
            decode_canonical_value_storage(payload, depth.saturating_add(1))
                .map_err(|_| CanonicalEnumWireError::PayloadCodec)
        })
        .map(|value| CanonicalValue::Enum(crate::value::ValueEnum::from_canonical(value)))
        .map_err(|_| FieldDecodeError::new());
    }
    if tag == TAG_LIST {
        return decode_canonical_list(encoded, depth).map(CanonicalValue::List);
    }
    if tag == TAG_MAP {
        return decode_canonical_map(encoded, depth).map(CanonicalValue::Map);
    }

    runtime_scalar_to_canonical(decode_structural_value_storage_bytes(encoded)?)
}

fn decode_canonical_list(
    encoded: &[u8],
    depth: u16,
) -> Result<Vec<CanonicalValue>, FieldDecodeError> {
    let (_, len, mut cursor) = canonical_collection_head(encoded, TAG_LIST)?;
    let mut items = Vec::new();
    items
        .try_reserve(usize::try_from(len).map_err(|_| FieldDecodeError::new())?)
        .map_err(|_| FieldDecodeError::new())?;
    for _ in 0..len {
        let start = cursor;
        cursor = skip_canonical_value(encoded, cursor, depth.saturating_add(1))?;
        items.push(decode_canonical_value_storage(
            &encoded[start..cursor],
            depth.saturating_add(1),
        )?);
    }
    if cursor != encoded.len() {
        return Err(FieldDecodeError::new());
    }
    Ok(items)
}

fn decode_canonical_map(
    encoded: &[u8],
    depth: u16,
) -> Result<Vec<(CanonicalValue, CanonicalValue)>, FieldDecodeError> {
    let (_, len, mut cursor) = canonical_collection_head(encoded, TAG_MAP)?;
    let mut entries = Vec::new();
    entries
        .try_reserve(usize::try_from(len).map_err(|_| FieldDecodeError::new())?)
        .map_err(|_| FieldDecodeError::new())?;
    for _ in 0..len {
        let key_start = cursor;
        cursor = skip_canonical_value(encoded, cursor, depth.saturating_add(1))?;
        let key =
            decode_canonical_value_storage(&encoded[key_start..cursor], depth.saturating_add(1))?;
        let value_start = cursor;
        cursor = skip_canonical_value(encoded, cursor, depth.saturating_add(1))?;
        let value =
            decode_canonical_value_storage(&encoded[value_start..cursor], depth.saturating_add(1))?;
        entries.push((key, value));
    }
    if cursor != encoded.len() {
        return Err(FieldDecodeError::new());
    }
    Ok(entries)
}

fn skip_canonical_value(
    encoded: &[u8],
    offset: usize,
    depth: u16,
) -> Result<usize, FieldDecodeError> {
    ensure_depth(depth)?;
    let tag = encoded
        .get(offset)
        .copied()
        .ok_or_else(FieldDecodeError::new)?;
    if tag == CANONICAL_ENUM_VALUE_TAG {
        return skip_canonical_enum(encoded, offset, depth);
    }
    if tag == TAG_LIST || tag == TAG_MAP {
        let (_, len, mut cursor) = canonical_collection_head_at(encoded, offset, tag)?;
        let item_count = if tag == TAG_MAP {
            len.checked_mul(2).ok_or_else(FieldDecodeError::new)?
        } else {
            len
        };
        for _ in 0..item_count {
            cursor = skip_canonical_value(encoded, cursor, depth.saturating_add(1))?;
        }
        return Ok(cursor);
    }

    skip_value_storage_binary_value(encoded, offset)
}

fn skip_canonical_enum(
    encoded: &[u8],
    offset: usize,
    depth: u16,
) -> Result<usize, FieldDecodeError> {
    let header_end = offset
        .checked_add(CANONICAL_ENUM_HEADER_BYTES)
        .ok_or_else(FieldDecodeError::new)?;
    let header = encoded
        .get(offset..header_end)
        .ok_or_else(FieldDecodeError::new)?;
    let payload_len = u32::from_be_bytes(
        header[10..14]
            .try_into()
            .map_err(|_| FieldDecodeError::new())?,
    );
    let end = header_end
        .checked_add(usize::try_from(payload_len).map_err(|_| FieldDecodeError::new())?)
        .ok_or_else(FieldDecodeError::new)?;
    let value = encoded.get(offset..end).ok_or_else(FieldDecodeError::new)?;
    decode_canonical_enum_value(value, |payload| {
        let payload_end = skip_canonical_value(payload, 0, depth.saturating_add(1))
            .map_err(|_| CanonicalEnumWireError::PayloadCodec)?;
        if payload_end != payload.len() {
            return Err(CanonicalEnumWireError::PayloadCodec);
        }
        Ok(())
    })
    .map_err(|_| FieldDecodeError::new())?;
    Ok(end)
}

fn canonical_collection_head(
    encoded: &[u8],
    expected_tag: u8,
) -> Result<(u8, u32, usize), FieldDecodeError> {
    canonical_collection_head_at(encoded, 0, expected_tag)
}

fn canonical_collection_head_at(
    encoded: &[u8],
    offset: usize,
    expected_tag: u8,
) -> Result<(u8, u32, usize), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(encoded, offset)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != expected_tag {
        return Err(FieldDecodeError::new());
    }
    Ok((tag, len, payload_start))
}

fn runtime_scalar_to_canonical(value: Value) -> Result<CanonicalValue, FieldDecodeError> {
    match value {
        Value::Account(value) => Ok(CanonicalValue::Account(value)),
        Value::Blob(value) => Ok(CanonicalValue::Blob(value)),
        Value::Bool(value) => Ok(CanonicalValue::Bool(value)),
        Value::Date(value) => Ok(CanonicalValue::Date(value)),
        Value::Decimal(value) => Ok(CanonicalValue::Decimal(value)),
        Value::Duration(value) => Ok(CanonicalValue::Duration(value)),
        Value::Float32(value) => Ok(CanonicalValue::Float32(value)),
        Value::Float64(value) => Ok(CanonicalValue::Float64(value)),
        Value::Int64(value) => Ok(CanonicalValue::Int64(value)),
        Value::Int128(value) => Ok(CanonicalValue::Int128(value)),
        Value::IntBig(value) => Ok(CanonicalValue::IntBig(value)),
        Value::Null => Ok(CanonicalValue::Null),
        Value::Principal(value) => Ok(CanonicalValue::Principal(value)),
        Value::Subaccount(value) => Ok(CanonicalValue::Subaccount(value)),
        Value::Text(value) => Ok(CanonicalValue::Text(value)),
        Value::Timestamp(value) => Ok(CanonicalValue::Timestamp(value)),
        Value::Nat64(value) => Ok(CanonicalValue::Nat64(value)),
        Value::Nat128(value) => Ok(CanonicalValue::Nat128(value)),
        Value::NatBig(value) => Ok(CanonicalValue::NatBig(value)),
        Value::Ulid(value) => Ok(CanonicalValue::Ulid(value)),
        Value::Unit => Ok(CanonicalValue::Unit),
        Value::U256(value) => Ok(CanonicalValue::U256(value)),
        Value::Enum(_) | Value::List(_) | Value::Map(_) => Err(FieldDecodeError::new()),
    }
}

const fn ensure_depth(depth: u16) -> Result<(), FieldDecodeError> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH_U16 {
        return Err(FieldDecodeError::new());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    mod fixed_width;

    use super::*;
    use crate::db::schema::MAX_ACCEPTED_RECURSIVE_DEPTH;
    use crate::types::{Decimal, IntBig, NatBig};
    use crate::value::{
        CanonicalEnumBody, CanonicalEnumValue, EnumTypeId, EnumVariantId, ValueEnum,
    };

    fn canonical_enum(payload: Option<CanonicalValue>) -> CanonicalValue {
        let type_id = EnumTypeId::new(7).expect("test type ID should be non-zero");
        let variant_id = EnumVariantId::new(11).expect("test variant ID should be non-zero");
        let body = payload.map_or(CanonicalEnumBody::Unit, |payload| {
            CanonicalEnumBody::Payload(Box::new(payload))
        });
        CanonicalValue::Enum(ValueEnum::from_canonical(CanonicalEnumValue::new(
            type_id, variant_id, body,
        )))
    }

    fn nested_list_value(depth: usize) -> CanonicalValue {
        let mut value = CanonicalValue::Unit;
        for _ in 0..depth {
            value = CanonicalValue::List(vec![value]);
        }
        value
    }

    #[test]
    fn canonical_value_encoding_uses_the_shared_recursive_depth_boundary() {
        let allowed = nested_list_value(MAX_ACCEPTED_RECURSIVE_DEPTH - 1);
        let excessive = nested_list_value(MAX_ACCEPTED_RECURSIVE_DEPTH);

        encode_canonical_value_storage_bytes(&allowed)
            .expect("the maximum accepted recursive depth should encode");
        encode_canonical_value_storage_bytes(&excessive)
            .expect_err("a value deeper than the accepted contract must reject");
    }

    #[test]
    fn canonical_value_storage_round_trips_nested_id_backed_enums() {
        let value = CanonicalValue::Map(vec![(
            CanonicalValue::Text("state".to_string()),
            CanonicalValue::List(vec![
                canonical_enum(None),
                canonical_enum(Some(CanonicalValue::List(vec![
                    canonical_enum(Some(CanonicalValue::Nat64(9))),
                    CanonicalValue::Text("after nested enum".to_string()),
                ]))),
                CanonicalValue::Bool(true),
            ]),
        )]);

        let encoded = encode_canonical_value_storage_bytes(&value)
            .expect("canonical value storage should encode");
        assert_eq!(
            decode_canonical_value_storage_bytes(&encoded)
                .expect("canonical value storage should decode"),
            value,
        );
    }

    #[test]
    fn canonical_value_storage_round_trips_u256_as_fixed_width_bytes() {
        for value in [crate::types::U256::ZERO, crate::types::U256::MAX] {
            let canonical = CanonicalValue::U256(value);
            let encoded = encode_canonical_value_storage_bytes(&canonical)
                .expect("canonical U256 should encode");

            assert_eq!(encoded.len(), 33);
            assert_eq!(
                decode_canonical_value_storage_bytes(&encoded)
                    .expect("canonical U256 should decode"),
                canonical,
            );
        }
    }

    #[test]
    fn canonical_value_storage_round_trips_shared_structural_binary_payloads() {
        let values = [
            CanonicalValue::Decimal(Decimal::from_i128_with_scale(12_345, 2)),
            CanonicalValue::IntBig(IntBig::from(-123_456_789_i64)),
            CanonicalValue::NatBig(NatBig::from(987_654_321_u64)),
        ];

        for value in values {
            let encoded = encode_canonical_value_storage_bytes(&value)
                .expect("canonical value storage should encode");

            assert_eq!(
                decode_canonical_value_storage_bytes(&encoded)
                    .expect("canonical value storage should decode"),
                value,
            );
        }
    }
}
