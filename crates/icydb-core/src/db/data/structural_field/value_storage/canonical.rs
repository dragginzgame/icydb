//! Canonical accepted-value encoding for `FieldStorageDecode::Value` payloads.
//!
//! Non-enum values retain the existing Structural Binary v1 representation.
//! Enums use the store-local type/variant ID envelope.

use crate::{
    db::{
        data::structural_field::{
            FieldDecodeError,
            binary::{TAG_LIST, TAG_MAP, parse_binary_head},
            value_storage::{
                decode_structural_value_storage_bytes, encode_account, encode_decimal, encode_int,
                encode_int128, encode_nat, encode_nat128,
                encode_structural_value_storage_blob_bytes,
                encode_structural_value_storage_bool_bytes,
                encode_structural_value_storage_date_bytes,
                encode_structural_value_storage_duration_bytes,
                encode_structural_value_storage_float32_bytes,
                encode_structural_value_storage_float64_bytes,
                encode_structural_value_storage_i64_bytes,
                encode_structural_value_storage_null_bytes,
                encode_structural_value_storage_principal_bytes,
                encode_structural_value_storage_subaccount_bytes,
                encode_structural_value_storage_timestamp_bytes,
                encode_structural_value_storage_u64_bytes,
                encode_structural_value_storage_ulid_bytes,
                encode_structural_value_storage_unit_bytes, encode_value_storage_owned_list_items,
                encode_value_storage_owned_map_entries, encode_value_storage_text,
                skip::skip_value_storage_binary_value,
            },
        },
        schema::enum_catalog::{
            CanonicalEnumWireError, CanonicalValue, decode_canonical_enum_value,
            encode_canonical_enum_value,
        },
    },
    error::InternalError,
    value::Value,
};

const MAX_CANONICAL_VALUE_STORAGE_DEPTH: u16 = 64;
const CANONICAL_ENUM_HEADER_BYTES: usize = 14;
const CANONICAL_ENUM_VALUE_TAG: u8 = 0x84;

type EncodedCanonicalMapEntry = (Vec<u8>, Vec<u8>);
type EncodedCanonicalMapEntries = Vec<EncodedCanonicalMapEntry>;

/// Encode one accepted canonical value without constructing runtime `Value`.
pub(in crate::db) fn encode_canonical_value_storage_bytes(
    value: &CanonicalValue,
) -> Result<Vec<u8>, InternalError> {
    encode_canonical_value_storage(value, 0)
}

fn encode_canonical_value_storage(
    value: &CanonicalValue,
    depth: u16,
) -> Result<Vec<u8>, InternalError> {
    ensure_depth(depth).map_err(|_| InternalError::persisted_row_encode_internal())?;

    match value {
        CanonicalValue::Account(value) => encode_account(*value),
        CanonicalValue::Blob(value) => {
            Ok(encode_structural_value_storage_blob_bytes(value.as_slice()))
        }
        CanonicalValue::Bool(value) => Ok(encode_structural_value_storage_bool_bytes(*value)),
        CanonicalValue::Date(value) => Ok(encode_structural_value_storage_date_bytes(*value)),
        CanonicalValue::Decimal(value) => Ok(encode_decimal(*value)),
        CanonicalValue::Duration(value) => {
            Ok(encode_structural_value_storage_duration_bytes(*value))
        }
        CanonicalValue::Enum(value) => {
            encode_canonical_enum_value(value.canonical(), |payload, encoded| {
                let payload = encode_canonical_value_storage(payload, depth.saturating_add(1))
                    .map_err(|_| CanonicalEnumWireError::PayloadCodec)?;
                encoded.extend_from_slice(payload.as_slice());
                Ok(())
            })
            .map_err(|_| InternalError::persisted_row_encode_internal())
        }
        CanonicalValue::Float32(value) => Ok(encode_structural_value_storage_float32_bytes(*value)),
        CanonicalValue::Float64(value) => Ok(encode_structural_value_storage_float64_bytes(*value)),
        CanonicalValue::Int64(value) => Ok(encode_structural_value_storage_i64_bytes(*value)),
        CanonicalValue::Int128(value) => Ok(encode_int128(*value)),
        CanonicalValue::IntBig(value) => Ok(encode_int(value)),
        CanonicalValue::List(items) => {
            let encoded = encode_canonical_list(items, depth)?;
            Ok(encode_value_storage_owned_list_items(encoded.as_slice()))
        }
        CanonicalValue::Map(entries) => {
            let encoded = encode_canonical_map(entries, depth)?;
            Ok(encode_value_storage_owned_map_entries(encoded.as_slice()))
        }
        CanonicalValue::Null => Ok(encode_structural_value_storage_null_bytes()),
        CanonicalValue::Principal(value) => encode_structural_value_storage_principal_bytes(*value),
        CanonicalValue::Subaccount(value) => {
            Ok(encode_structural_value_storage_subaccount_bytes(*value))
        }
        CanonicalValue::Text(value) => Ok(encode_value_storage_text(value)),
        CanonicalValue::Timestamp(value) => {
            Ok(encode_structural_value_storage_timestamp_bytes(*value))
        }
        CanonicalValue::Nat64(value) => Ok(encode_structural_value_storage_u64_bytes(*value)),
        CanonicalValue::Nat128(value) => Ok(encode_nat128(*value)),
        CanonicalValue::NatBig(value) => Ok(encode_nat(value)),
        CanonicalValue::Ulid(value) => Ok(encode_structural_value_storage_ulid_bytes(*value)),
        CanonicalValue::Unit => Ok(encode_structural_value_storage_unit_bytes()),
    }
}

fn encode_canonical_list(
    items: &[CanonicalValue],
    depth: u16,
) -> Result<Vec<Vec<u8>>, InternalError> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve(items.len())
        .map_err(|_| InternalError::persisted_row_encode_internal())?;
    for item in items {
        encoded.push(encode_canonical_value_storage(
            item,
            depth.saturating_add(1),
        )?);
    }
    Ok(encoded)
}

fn encode_canonical_map(
    entries: &[(CanonicalValue, CanonicalValue)],
    depth: u16,
) -> Result<EncodedCanonicalMapEntries, InternalError> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve(entries.len())
        .map_err(|_| InternalError::persisted_row_encode_internal())?;
    for (key, value) in entries {
        encoded.push((
            encode_canonical_value_storage(key, depth.saturating_add(1))?,
            encode_canonical_value_storage(value, depth.saturating_add(1))?,
        ));
    }
    Ok(encoded)
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
        Value::Enum(_) | Value::List(_) | Value::Map(_) => Err(FieldDecodeError::new()),
    }
}

const fn ensure_depth(depth: u16) -> Result<(), FieldDecodeError> {
    if depth >= MAX_CANONICAL_VALUE_STORAGE_DEPTH {
        return Err(FieldDecodeError::new());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn canonical_value_storage_round_trips_nested_id_backed_enums() {
        let value = CanonicalValue::Map(vec![(
            CanonicalValue::Text("state".to_string()),
            CanonicalValue::List(vec![
                canonical_enum(None),
                canonical_enum(Some(CanonicalValue::Nat64(9))),
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
}
