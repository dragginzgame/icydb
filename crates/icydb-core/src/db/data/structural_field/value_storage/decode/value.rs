//! Module: data::structural_field::value_storage::decode::value
//! Responsibility: runtime `Value` materialization from structural value-storage bytes.
//! Does not own: encoding, borrowed view APIs, or field-kind routing.
//! Boundary: decodes already-bounded value-storage payloads into owned runtime values.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NAT64, TAG_NULL, TAG_TEXT,
            TAG_TRUE, TAG_UNIT, parse_binary_head, skip_binary_value,
        },
        typed::{
            decode_account_payload_bytes, decode_date_payload_days,
            decode_decimal_payload_mantissa_and_scale, decode_duration_payload_millis,
            decode_float32_payload_bytes, decode_float64_payload_bytes,
            decode_int128_payload_bytes, decode_nat128_payload_bytes,
            decode_principal_payload_bytes, decode_subaccount_payload_bytes,
            decode_timestamp_payload_millis, decode_ulid_payload_bytes,
        },
        value_storage::{
            decode::{ValueStorageSlice, cursor::decode_value_storage_binary_value_at},
            next_value_storage_decode_depth,
            primitives::{
                decode_binary_required_bytes, decode_binary_required_i64,
                decode_binary_required_text, decode_binary_required_u64,
                decode_value_storage_binary_payload, split_binary_tuple_2,
            },
            reserve_one_value_storage_item,
            skip::skip_value_storage_binary_value,
            tags::{
                VALUE_BINARY_TAG_ACCOUNT, VALUE_BINARY_TAG_DATE, VALUE_BINARY_TAG_DECIMAL,
                VALUE_BINARY_TAG_DURATION, VALUE_BINARY_TAG_FLOAT32, VALUE_BINARY_TAG_FLOAT64,
                VALUE_BINARY_TAG_INT_BIG, VALUE_BINARY_TAG_INT128, VALUE_BINARY_TAG_NAT_BIG,
                VALUE_BINARY_TAG_NAT128, VALUE_BINARY_TAG_PRINCIPAL, VALUE_BINARY_TAG_SUBACCOUNT,
                VALUE_BINARY_TAG_TIMESTAMP, VALUE_BINARY_TAG_ULID,
            },
            walk::{
                decode_value_storage_binary_list_items_single_pass,
                decode_value_storage_binary_map_entries_single_pass,
            },
        },
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, Ulid,
    },
    value::Value,
};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};

// Borrowed map-entry payload slices returned by the direct structural
// value-storage split helpers.
type ValueStorageMapEntrySlices<'a> = Vec<(&'a [u8], &'a [u8])>;

/// Decode one `FieldStorageDecode::Value` payload directly from the externally
/// tagged `Value` wire shape without routing through serde's recursive enum
/// visitor graph.
pub(in crate::db) fn decode_structural_value_storage_bytes(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let slice = ValueStorageSlice::from_raw(raw_bytes)?;

    decode_value_storage_slice(slice)
}

/// Validate one `FieldStorageDecode::Value` payload through the canonical
/// Structural Binary v1 owner.
pub(in crate::db) fn validate_structural_value_storage_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    ValueStorageSlice::from_raw(raw_bytes).map(|_| ())
}

/// Return `true` when one structural value-storage payload is the canonical
/// encoded `NULL` form and reject malformed bytes fail-closed.
pub(in crate::db) fn value_storage_bytes_are_null(
    raw_bytes: &[u8],
) -> Result<bool, FieldDecodeError> {
    let tag = validated_value_storage_root_tag(raw_bytes)?;

    Ok(tag == TAG_NULL)
}

// Validate one complete value-storage root and return the root tag. This helper
// deliberately uses value-storage skip traversal instead of generic Structural
// Binary parsing so local tags such as `Ulid` stay valid non-null payloads.
fn validated_value_storage_root_tag(raw_bytes: &[u8]) -> Result<u8, FieldDecodeError> {
    let Some(&tag) = raw_bytes.first() else {
        return Err(FieldDecodeError::new());
    };
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(tag)
}

/// Decode one canonical structural value-storage `unit` payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_unit_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    let tag = validated_value_storage_root_tag(raw_bytes)?;
    if tag != TAG_UNIT {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

/// Decode one canonical structural value-storage boolean payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_bool_bytes(
    raw_bytes: &[u8],
) -> Result<bool, FieldDecodeError> {
    let tag = validated_value_storage_root_tag(raw_bytes)?;

    match tag {
        TAG_FALSE => Ok(false),
        TAG_TRUE => Ok(true),
        _ => Err(FieldDecodeError::new()),
    }
}

/// Decode one canonical structural value-storage unsigned integer payload
/// without materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_u64_bytes(
    raw_bytes: &[u8],
) -> Result<u64, FieldDecodeError> {
    decode_binary_required_u64(raw_bytes)
}

/// Decode one canonical structural value-storage signed integer payload
/// without materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_i64_bytes(
    raw_bytes: &[u8],
) -> Result<i64, FieldDecodeError> {
    decode_binary_required_i64(raw_bytes)
}

/// Decode one canonical structural value-storage text payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_value_storage_text(
    raw_bytes: &[u8],
) -> Result<String, FieldDecodeError> {
    decode_binary_required_text(raw_bytes).map(str::to_owned)
}

/// Decode one canonical structural value-storage account payload.
pub(in crate::db) fn decode_account(raw_bytes: &[u8]) -> Result<Account, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ACCOUNT)?;
    let bytes = decode_binary_required_bytes(payload)?;

    decode_account_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage decimal payload.
pub(in crate::db) fn decode_decimal(raw_bytes: &[u8]) -> Result<Decimal, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DECIMAL)?;
    let [mantissa, scale] = split_binary_tuple_2(payload)?;
    let mantissa_bytes = decode_binary_required_bytes(mantissa)?;
    let scale = decode_binary_required_u64(scale)?;
    let mantissa_buf: [u8; 16] = mantissa_bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new())?;
    let scale = u32::try_from(scale).map_err(|_| FieldDecodeError::new())?;

    decode_decimal_payload_mantissa_and_scale(i128::from_be_bytes(mantissa_buf), scale)
}

/// Decode one canonical structural value-storage int128 payload.
pub(in crate::db) fn decode_int128(raw_bytes: &[u8]) -> Result<i128, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_INT128)?;
    let bytes: [u8; 16] = decode_binary_required_bytes(payload)?
        .try_into()
        .map_err(|_| FieldDecodeError::new())?;

    decode_int128_payload_bytes(bytes.as_slice())
}

/// Decode one canonical structural value-storage nat128 payload.
pub(in crate::db) fn decode_nat128(raw_bytes: &[u8]) -> Result<u128, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_NAT128)?;
    let bytes: [u8; 16] = decode_binary_required_bytes(payload)?
        .try_into()
        .map_err(|_| FieldDecodeError::new())?;

    decode_nat128_payload_bytes(bytes.as_slice())
}

/// Decode one canonical structural value-storage `Value::IntBig` payload.
pub(in crate::db) fn decode_int(raw_bytes: &[u8]) -> Result<IntBig, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_INT_BIG)?;
    let [sign, magnitude] = split_binary_tuple_2(payload)?;
    let sign = decode_binary_required_i64(sign)?;
    let magnitude = decode_binary_big_integer_magnitude_digits(magnitude)?;
    let sign = decode_binary_int_big_sign(sign)?;

    Ok(IntBig::from_bigint(BigInt::from_biguint(sign, magnitude)))
}

/// Decode one canonical structural value-storage `Value::NatBig` payload.
pub(in crate::db) fn decode_nat(raw_bytes: &[u8]) -> Result<NatBig, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_NAT_BIG)?;
    let digits = decode_binary_big_integer_magnitude_digits(payload)?;

    Ok(NatBig::from_biguint(digits))
}

/// Decode one canonical structural value-storage bytes payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_blob_bytes(
    raw_bytes: &[u8],
) -> Result<Vec<u8>, FieldDecodeError> {
    decode_binary_required_bytes(raw_bytes).map(<[u8]>::to_vec)
}

/// Decode one canonical structural value-storage float32 payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_float32_bytes(
    raw_bytes: &[u8],
) -> Result<Float32, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_FLOAT32)?;
    let bytes = decode_binary_required_bytes(payload)?;

    decode_float32_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage float64 payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_float64_bytes(
    raw_bytes: &[u8],
) -> Result<Float64, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_FLOAT64)?;
    let bytes = decode_binary_required_bytes(payload)?;

    decode_float64_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage date payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_date_bytes(
    raw_bytes: &[u8],
) -> Result<Date, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DATE)?;
    let days = decode_binary_required_i64(payload)?;

    decode_date_payload_days(days)
}

/// Decode one canonical structural value-storage duration payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_duration_bytes(
    raw_bytes: &[u8],
) -> Result<Duration, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DURATION)?;

    Ok(decode_duration_payload_millis(decode_binary_required_u64(
        payload,
    )?))
}

/// Decode one canonical structural value-storage principal payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_principal_bytes(
    raw_bytes: &[u8],
) -> Result<Principal, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_PRINCIPAL)?;
    let bytes = decode_binary_required_bytes(payload)?;

    decode_principal_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage subaccount payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_subaccount_bytes(
    raw_bytes: &[u8],
) -> Result<Subaccount, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_SUBACCOUNT)?;
    decode_subaccount_payload_bytes(decode_binary_required_bytes(payload)?)
}

/// Decode one canonical structural value-storage timestamp payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_timestamp_bytes(
    raw_bytes: &[u8],
) -> Result<Timestamp, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_TIMESTAMP)?;

    Ok(decode_timestamp_payload_millis(decode_binary_required_i64(
        payload,
    )?))
}

/// Decode one canonical structural value-storage ULID payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_ulid_bytes(
    raw_bytes: &[u8],
) -> Result<Ulid, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ULID)?;
    decode_ulid_payload_bytes(decode_binary_required_bytes(payload)?)
}

/// Split one structural value-storage list payload into borrowed nested item
/// payload slices without materializing runtime `Value` items.
pub(in crate::db) fn decode_value_storage_list_item_slices(
    raw_bytes: &[u8],
) -> Result<Vec<&[u8]>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    let mut items = Vec::new();
    for _ in 0..len {
        reserve_one_value_storage_item(&mut items)?;
        let item_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        items.push(&raw_bytes[item_start..cursor]);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(items)
}

/// Split one structural value-storage map payload into borrowed nested key and
/// value payload slices without materializing runtime `Value` entries.
pub(in crate::db) fn decode_value_storage_map_entry_slices(
    raw_bytes: &[u8],
) -> Result<ValueStorageMapEntrySlices<'_>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    let mut entries = Vec::new();
    for _ in 0..len {
        reserve_one_value_storage_item(&mut entries)?;
        let key_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        entries.push((
            &raw_bytes[key_start..value_start],
            &raw_bytes[value_start..cursor],
        ));
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(entries)
}

/// Decode one `FieldStorageDecode::Value` payload from the parallel
/// Structural Binary v1 `Value` envelope.
pub(super) fn decode_value_storage_slice(
    slice: ValueStorageSlice<'_>,
) -> Result<Value, FieldDecodeError> {
    decode_value_storage_slice_at_depth(slice, 0)
}

pub(super) fn decode_value_storage_slice_at_depth(
    slice: ValueStorageSlice<'_>,
    depth: usize,
) -> Result<Value, FieldDecodeError> {
    let depth = next_value_storage_decode_depth(depth)?;
    let raw_bytes = slice.as_bytes();
    let Some(&tag) = raw_bytes.first() else {
        return Err(FieldDecodeError::new());
    };

    // Phase 1: decode the unambiguous generic root tags directly.
    let generic = match tag {
        TAG_NULL => Some(Value::Null),
        TAG_UNIT => Some(Value::Unit),
        TAG_FALSE => Some(Value::Bool(false)),
        TAG_TRUE => Some(Value::Bool(true)),
        TAG_INT64 => Some(Value::Int64(decode_structural_value_storage_i64_bytes(
            raw_bytes,
        )?)),
        TAG_NAT64 => Some(Value::Nat64(decode_structural_value_storage_u64_bytes(
            raw_bytes,
        )?)),
        TAG_TEXT => Some(Value::Text(decode_value_storage_text(raw_bytes)?)),
        TAG_BYTES => Some(Value::Blob(decode_structural_value_storage_blob_bytes(
            raw_bytes,
        )?)),
        TAG_LIST => Some(decode_value_storage_binary_list_bytes(raw_bytes, depth)?),
        TAG_MAP => Some(decode_value_storage_binary_map_bytes(raw_bytes, depth)?),
        _ => None,
    };
    if let Some(value) = generic {
        return Ok(value);
    }

    // Phase 2: decode the local value-envelope tags without widening authority
    // beyond this owner's semantic surface.
    match tag {
        VALUE_BINARY_TAG_ACCOUNT => decode_account(raw_bytes).map(Value::Account),
        VALUE_BINARY_TAG_DATE => {
            decode_structural_value_storage_date_bytes(raw_bytes).map(Value::Date)
        }
        VALUE_BINARY_TAG_DECIMAL => decode_decimal(raw_bytes).map(Value::Decimal),
        VALUE_BINARY_TAG_DURATION => {
            decode_structural_value_storage_duration_bytes(raw_bytes).map(Value::Duration)
        }
        VALUE_BINARY_TAG_FLOAT32 => {
            decode_structural_value_storage_float32_bytes(raw_bytes).map(Value::Float32)
        }
        VALUE_BINARY_TAG_FLOAT64 => {
            decode_structural_value_storage_float64_bytes(raw_bytes).map(Value::Float64)
        }
        VALUE_BINARY_TAG_INT128 => decode_int128(raw_bytes).map(Value::Int128),
        VALUE_BINARY_TAG_INT_BIG => decode_int(raw_bytes).map(Value::IntBig),
        VALUE_BINARY_TAG_PRINCIPAL => {
            decode_structural_value_storage_principal_bytes(raw_bytes).map(Value::Principal)
        }
        VALUE_BINARY_TAG_SUBACCOUNT => {
            decode_structural_value_storage_subaccount_bytes(raw_bytes).map(Value::Subaccount)
        }
        VALUE_BINARY_TAG_TIMESTAMP => {
            decode_structural_value_storage_timestamp_bytes(raw_bytes).map(Value::Timestamp)
        }
        VALUE_BINARY_TAG_NAT128 => decode_nat128(raw_bytes).map(Value::Nat128),
        VALUE_BINARY_TAG_NAT_BIG => decode_nat(raw_bytes).map(Value::NatBig),
        VALUE_BINARY_TAG_ULID => {
            decode_structural_value_storage_ulid_bytes(raw_bytes).map(Value::Ulid)
        }
        _ => Err(FieldDecodeError::new()),
    }
}

// Decode one binary list payload recursively from raw item bytes.
fn decode_value_storage_binary_list_bytes(
    raw_bytes: &[u8],
    depth: usize,
) -> Result<Value, FieldDecodeError> {
    // TODO(value-storage zero-copy): recursive decode must allocate the final
    // runtime Vec<Value>, but a future projection-only path can use the walker
    // directly and avoid materializing each item.
    let (items, _) = decode_value_storage_binary_list_items_single_pass(
        raw_bytes,
        0,
        true,
        depth,
        decode_value_storage_binary_value_at,
    )?;

    Ok(Value::List(items))
}

// Decode one binary map payload recursively while preserving runtime map
// invariants.
fn decode_value_storage_binary_map_bytes(
    raw_bytes: &[u8],
    depth: usize,
) -> Result<Value, FieldDecodeError> {
    // TODO(value-storage zero-copy): recursive map decode allocates the final
    // runtime Vec<(Value, Value)>. Projection and validation paths should use a
    // streaming visitor before constructing owned runtime pairs.
    let (entries, _) = decode_value_storage_binary_map_entries_single_pass(
        raw_bytes,
        0,
        true,
        depth,
        decode_value_storage_binary_value_at,
    )?;

    Value::from_map(entries).map_err(|_| FieldDecodeError::new())
}

// Decode one u32-limb magnitude sequence into a `BigUint`.
fn decode_binary_big_integer_magnitude_digits(
    raw_bytes: &[u8],
) -> Result<BigUint, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    let mut digits = Vec::new();
    for _ in 0..len {
        reserve_one_value_storage_item(&mut digits)?;
        let start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        let digit = decode_binary_required_u64(&raw_bytes[start..cursor])?;
        digits.push(u32::try_from(digit).map_err(|_| FieldDecodeError::new())?);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(BigUint::new(digits))
}

// Decode one `Value::IntBig` sign marker while preserving the fail-closed
// signed limb contract shared by direct and runtime `Value` decode paths.
const fn decode_binary_int_big_sign(sign: i64) -> Result<BigIntSign, FieldDecodeError> {
    match sign {
        -1 => Ok(BigIntSign::Minus),
        0 => Ok(BigIntSign::NoSign),
        1 => Ok(BigIntSign::Plus),
        _ => Err(FieldDecodeError::new()),
    }
}
