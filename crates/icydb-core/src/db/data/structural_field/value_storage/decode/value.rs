//! Decode-side materialization for the structural value-storage owner.
//!
//! Two traversal models intentionally coexist here. Skip-based traversal is the
//! authoritative boundary detector for borrowed-slice helpers and local tagged
//! payload extraction: it validates the structural shape, finds the exact byte
//! boundary, and only then lets callers inspect the bounded slice. Decode-based
//! traversal is used when this module materializes runtime `Value` trees; those
//! paths advance a cursor while decoding and may assume any slice handed to a
//! nested decoder is already bounded by the owning traversal step.
//!
//! The distinction is important for maintenance: skip owns structural
//! validation and boundary discovery, while decode owns `Value` construction.
//! New callers should pick the model that matches their ownership needs rather
//! than mixing borrowed boundary detection with runtime materialization.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NULL, TAG_TEXT, TAG_TRUE,
            TAG_UINT64, TAG_UNIT, parse_binary_head, skip_binary_value,
        },
        typed::{
            decode_account_payload_bytes, decode_date_payload_days, decode_decimal_payload_parts,
            decode_duration_payload_millis, decode_float32_payload_bytes,
            decode_float64_payload_bytes, decode_int128_payload_bytes, decode_nat128_payload_bytes,
            decode_principal_payload_bytes, decode_subaccount_payload_bytes,
            decode_timestamp_payload_millis, decode_ulid_payload_bytes,
        },
        value_storage::{
            decode::{ValueStorageSlice, cursor::decode_value_storage_binary_value_at},
            primitives::{
                decode_binary_required_bytes, decode_binary_required_i64,
                decode_binary_required_text, decode_binary_required_u64,
                decode_value_storage_binary_payload, split_binary_tuple_2,
                split_value_storage_tuple_3,
            },
            skip::skip_value_storage_binary_value,
            tags::{
                VALUE_BINARY_TAG_ACCOUNT, VALUE_BINARY_TAG_DATE, VALUE_BINARY_TAG_DECIMAL,
                VALUE_BINARY_TAG_DURATION, VALUE_BINARY_TAG_ENUM, VALUE_BINARY_TAG_FLOAT32,
                VALUE_BINARY_TAG_FLOAT64, VALUE_BINARY_TAG_INT_BIG, VALUE_BINARY_TAG_INT128,
                VALUE_BINARY_TAG_PRINCIPAL, VALUE_BINARY_TAG_SUBACCOUNT,
                VALUE_BINARY_TAG_TIMESTAMP, VALUE_BINARY_TAG_UINT_BIG, VALUE_BINARY_TAG_UINT128,
                VALUE_BINARY_TAG_ULID,
            },
            walk::{
                decode_value_storage_binary_list_items_single_pass,
                decode_value_storage_binary_map_entries_single_pass,
                visit_value_storage_list_items, visit_value_storage_map_entries,
            },
        },
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Nat, Principal, Subaccount,
        Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};

// Borrowed map-entry payload slices returned by the direct structural
// value-storage split helpers.
type ValueStorageMapEntrySlices<'a> = Vec<(&'a [u8], &'a [u8])>;
type EnumBinaryDecodedPayload<'a> = (String, Option<String>, Option<&'a [u8]>);

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
    validate_value_storage_bytes(raw_bytes)
}

/// Return `true` when one structural value-storage payload is the canonical
/// encoded `NULL` form and reject malformed bytes fail-closed.
pub(in crate::db) fn value_storage_bytes_are_null(
    raw_bytes: &[u8],
) -> Result<bool, FieldDecodeError> {
    let tag = validated_value_storage_root_tag(raw_bytes, "null payload")?;

    Ok(tag == TAG_NULL)
}

// Validate one complete value-storage root and return the root tag. This helper
// deliberately uses value-storage skip traversal instead of generic Structural
// Binary parsing so local tags such as `Ulid` stay valid non-null payloads.
fn validated_value_storage_root_tag(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<u8, FieldDecodeError> {
    let Some(&tag) = raw_bytes.first() else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label}"
        )));
    }

    Ok(tag)
}

/// Decode one canonical structural value-storage `unit` payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_unit_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    let tag = validated_value_storage_root_tag(raw_bytes, "unit payload")?;
    if tag != TAG_UNIT {
        return Err(FieldDecodeError::new(
            "structural binary: expected unit payload",
        ));
    }

    Ok(())
}

/// Decode one canonical structural value-storage boolean payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_bool_bytes(
    raw_bytes: &[u8],
) -> Result<bool, FieldDecodeError> {
    let tag = validated_value_storage_root_tag(raw_bytes, "bool payload")?;

    match tag {
        TAG_FALSE => Ok(false),
        TAG_TRUE => Ok(true),
        _ => Err(FieldDecodeError::new(
            "structural binary: expected bool payload",
        )),
    }
}

/// Decode one canonical structural value-storage unsigned integer payload
/// without materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_u64_bytes(
    raw_bytes: &[u8],
) -> Result<u64, FieldDecodeError> {
    decode_binary_required_u64(raw_bytes, "u64 integer")
}

/// Decode one canonical structural value-storage signed integer payload
/// without materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_i64_bytes(
    raw_bytes: &[u8],
) -> Result<i64, FieldDecodeError> {
    decode_binary_required_i64(raw_bytes, "i64 integer")
}

/// Decode one canonical structural value-storage text payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_value_storage_text(
    raw_bytes: &[u8],
) -> Result<String, FieldDecodeError> {
    decode_binary_required_text(raw_bytes, "text payload").map(str::to_owned)
}

/// Decode one canonical structural value-storage account payload.
pub(in crate::db) fn decode_account(raw_bytes: &[u8]) -> Result<Account, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ACCOUNT, "account")?;
    let bytes = decode_binary_required_bytes(payload, "account bytes")?;

    decode_account_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage decimal payload.
pub(in crate::db) fn decode_decimal(raw_bytes: &[u8]) -> Result<Decimal, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DECIMAL, "decimal")?;
    let [mantissa, scale] = split_binary_tuple_2(payload, "decimal tuple")?;
    let mantissa_bytes = decode_binary_required_bytes(mantissa, "decimal mantissa")?;
    let scale = decode_binary_required_u64(scale, "decimal scale")?;
    let mantissa_buf: [u8; 16] = mantissa_bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid decimal mantissa length"))?;
    let scale = u32::try_from(scale)
        .map_err(|_| FieldDecodeError::new("structural binary: decimal scale out of u32 range"))?;

    decode_decimal_payload_parts(i128::from_be_bytes(mantissa_buf), scale)
}

/// Decode one canonical structural value-storage int128 payload.
pub(in crate::db) fn decode_int128(
    raw_bytes: &[u8],
) -> Result<crate::types::Int128, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_INT128, "int128")?;
    let bytes: [u8; 16] = decode_binary_required_bytes(payload, "int128 bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid int128 length"))?;

    decode_int128_payload_bytes(bytes.as_slice())
}

/// Decode one canonical structural value-storage uint128 payload.
pub(in crate::db) fn decode_nat128(
    raw_bytes: &[u8],
) -> Result<crate::types::Nat128, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_UINT128, "uint128")?;
    let bytes: [u8; 16] = decode_binary_required_bytes(payload, "uint128 bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid uint128 length"))?;

    decode_nat128_payload_bytes(bytes.as_slice())
}

/// Decode one canonical structural value-storage bigint payload.
pub(in crate::db) fn decode_int(raw_bytes: &[u8]) -> Result<Int, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_INT_BIG, "bigint")?;
    let [sign, magnitude] = split_binary_tuple_2(payload, "bigint tuple")?;
    let sign = decode_binary_required_i64(sign, "bigint sign")?;
    let magnitude = decode_binary_biguint_digits(magnitude)?;
    let sign = decode_binary_bigint_sign(sign)?;

    Ok(Int::from(WrappedInt::from(BigInt::from_biguint(
        sign, magnitude,
    ))))
}

/// Decode one canonical structural value-storage biguint payload.
pub(in crate::db) fn decode_nat(raw_bytes: &[u8]) -> Result<Nat, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_UINT_BIG, "biguint")?;
    let digits = decode_binary_biguint_digits(payload)?;

    Ok(Nat::from(WrappedNat::from(digits)))
}

/// Decode one canonical structural value-storage bytes payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_blob_bytes(
    raw_bytes: &[u8],
) -> Result<Vec<u8>, FieldDecodeError> {
    decode_binary_required_bytes(raw_bytes, "byte payload").map(<[u8]>::to_vec)
}

/// Decode one canonical structural value-storage float32 payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_float32_bytes(
    raw_bytes: &[u8],
) -> Result<Float32, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_FLOAT32, "float32")?;
    let bytes = decode_binary_required_bytes(payload, "float32 bytes")?;

    decode_float32_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage float64 payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_float64_bytes(
    raw_bytes: &[u8],
) -> Result<Float64, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_FLOAT64, "float64")?;
    let bytes = decode_binary_required_bytes(payload, "float64 bytes")?;

    decode_float64_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage date payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_date_bytes(
    raw_bytes: &[u8],
) -> Result<Date, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DATE, "date")?;
    let days = decode_binary_required_i64(payload, "date days")?;

    decode_date_payload_days(days)
}

/// Decode one canonical structural value-storage duration payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_duration_bytes(
    raw_bytes: &[u8],
) -> Result<Duration, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DURATION, "duration")?;

    Ok(decode_duration_payload_millis(decode_binary_required_u64(
        payload,
        "duration millis",
    )?))
}

/// Decode one canonical structural value-storage principal payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_principal_bytes(
    raw_bytes: &[u8],
) -> Result<Principal, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_PRINCIPAL, "principal")?;
    let bytes = decode_binary_required_bytes(payload, "principal bytes")?;

    decode_principal_payload_bytes(bytes)
}

/// Decode one canonical structural value-storage subaccount payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_subaccount_bytes(
    raw_bytes: &[u8],
) -> Result<Subaccount, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_SUBACCOUNT, "subaccount")?;
    decode_subaccount_payload_bytes(decode_binary_required_bytes(payload, "subaccount bytes")?)
}

/// Decode one canonical structural value-storage timestamp payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_timestamp_bytes(
    raw_bytes: &[u8],
) -> Result<Timestamp, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_TIMESTAMP, "timestamp")?;

    Ok(decode_timestamp_payload_millis(decode_binary_required_i64(
        payload,
        "timestamp millis",
    )?))
}

/// Decode one canonical structural value-storage ULID payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_ulid_bytes(
    raw_bytes: &[u8],
) -> Result<Ulid, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ULID, "ulid")?;
    decode_ulid_payload_bytes(decode_binary_required_bytes(payload, "ulid bytes")?)
}

/// Decode one canonical enum payload into its variant, optional strict path,
/// and borrowed nested payload bytes without constructing `Value`.
pub(in crate::db) fn decode_enum(
    raw_bytes: &[u8],
) -> Result<EnumBinaryDecodedPayload<'_>, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ENUM, "enum")?;
    let [variant, path, nested] = split_value_storage_tuple_3(payload, "enum tuple")?;
    let variant = decode_binary_required_text(variant, "enum variant")?.to_owned();
    let path = decode_binary_optional_text(path, "enum path")?.map(str::to_owned);
    let payload = if value_storage_bytes_are_null(nested)? {
        None
    } else {
        Some(nested)
    };

    Ok((variant, path, payload))
}

/// Split one structural value-storage list payload into borrowed nested item
/// payload slices without materializing runtime `Value` items.
pub(in crate::db) fn decode_value_storage_list_item_slices(
    raw_bytes: &[u8],
) -> Result<Vec<&[u8]>, FieldDecodeError> {
    // TODO(value-storage zero-copy): generated-code list splitting still
    // stages borrowed slices in a Vec. A streaming visitor would let callers
    // consume items without this allocation.
    visit_value_storage_list_items(
        raw_bytes,
        "structural binary: expected value list payload",
        "structural binary: trailing bytes after value list payload",
        Vec::with_capacity,
        |items, item| {
            items.push(item);

            Ok(())
        },
    )
}

/// Split one structural value-storage map payload into borrowed nested key and
/// value payload slices without materializing runtime `Value` entries.
pub(in crate::db) fn decode_value_storage_map_entry_slices(
    raw_bytes: &[u8],
) -> Result<ValueStorageMapEntrySlices<'_>, FieldDecodeError> {
    // TODO(value-storage zero-copy): map splitting allocates one borrowed
    // slice pair per entry. A streaming entry visitor would avoid staging for
    // generated decode paths that can consume entries immediately.
    visit_value_storage_map_entries(
        raw_bytes,
        "structural binary: expected value map payload",
        "structural binary: trailing bytes after value map payload",
        Vec::with_capacity,
        |entries, key, value| {
            entries.push((key, value));

            Ok(())
        },
    )
}

/// Decode one `FieldStorageDecode::Value` payload from the parallel
/// Structural Binary v1 `Value` envelope.
pub(super) fn decode_value_storage_slice(
    slice: ValueStorageSlice<'_>,
) -> Result<Value, FieldDecodeError> {
    let raw_bytes = slice.as_bytes();
    let Some(&tag) = raw_bytes.first() else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value payload",
        ));
    };

    // Phase 1: decode the unambiguous generic root tags directly.
    let generic = match tag {
        TAG_NULL => Some(Value::Null),
        TAG_UNIT => Some(Value::Unit),
        TAG_FALSE => Some(Value::Bool(false)),
        TAG_TRUE => Some(Value::Bool(true)),
        TAG_INT64 => Some(Value::Int(decode_structural_value_storage_i64_bytes(
            raw_bytes,
        )?)),
        TAG_UINT64 => Some(Value::Uint(decode_structural_value_storage_u64_bytes(
            raw_bytes,
        )?)),
        TAG_TEXT => Some(Value::Text(decode_value_storage_text(raw_bytes)?)),
        TAG_BYTES => Some(Value::Blob(decode_structural_value_storage_blob_bytes(
            raw_bytes,
        )?)),
        TAG_LIST => Some(decode_value_storage_binary_list_bytes(raw_bytes)?),
        TAG_MAP => Some(decode_value_storage_binary_map_bytes(raw_bytes)?),
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
        VALUE_BINARY_TAG_ENUM => decode_binary_enum_value(raw_bytes),
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
        VALUE_BINARY_TAG_UINT128 => decode_nat128(raw_bytes).map(Value::Uint128),
        VALUE_BINARY_TAG_UINT_BIG => decode_nat(raw_bytes).map(Value::UintBig),
        VALUE_BINARY_TAG_ULID => {
            decode_structural_value_storage_ulid_bytes(raw_bytes).map(Value::Ulid)
        }
        other => Err(FieldDecodeError::new(format!(
            "structural binary: unsupported value tag 0x{other:02X}"
        ))),
    }
}

/// Validate one `FieldStorageDecode::Value` payload from the parallel
/// Structural Binary v1 `Value` envelope without rebuilding it eagerly.
fn validate_value_storage_bytes(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    ValueStorageSlice::from_raw(raw_bytes).map(|_| ())
}

// Decode one local enum payload from the fixed positional tuple
// `(variant, path, payload)`.
fn decode_binary_enum_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let (variant, path, nested) = decode_enum(raw_bytes)?;

    let mut value = ValueEnum::new(&variant, path.as_deref());
    if let Some(nested) = nested {
        let payload = decode_bounded_value_storage_slice(nested)?;
        value = value.with_payload(payload);
    }

    Ok(Value::Enum(value))
}

// Decode one binary list payload recursively from raw item bytes.
fn decode_value_storage_binary_list_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    // TODO(value-storage zero-copy): recursive decode must allocate the final
    // runtime Vec<Value>, but a future projection-only path can use the walker
    // directly and avoid materializing each item.
    let (items, _) = decode_value_storage_binary_list_items_single_pass(
        raw_bytes,
        0,
        "expected structural binary list for value list payload",
        Some("structural binary: trailing bytes after value list payload"),
        decode_value_storage_binary_value_at,
    )?;

    Ok(Value::List(items))
}

// Decode one binary map payload recursively while preserving runtime map
// invariants.
fn decode_value_storage_binary_map_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    // TODO(value-storage zero-copy): recursive map decode allocates the final
    // runtime Vec<(Value, Value)>. Projection and validation paths should use a
    // streaming visitor before constructing owned runtime pairs.
    let (entries, _) = decode_value_storage_binary_map_entries_single_pass(
        raw_bytes,
        0,
        "expected structural binary map for value map payload",
        Some("structural binary: trailing bytes after value map payload"),
        decode_value_storage_binary_value_at,
    )?;

    Value::from_map(entries)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

// Decode one u32-limb sequence into a `BigUint`.
fn decode_binary_biguint_digits(raw_bytes: &[u8]) -> Result<BigUint, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated biguint digits",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(
            "structural binary: expected biguint digit list",
        ));
    }

    let mut cursor = payload_start;
    let mut digits = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        let digit = decode_binary_required_u64(&raw_bytes[start..cursor], "biguint digit")?;
        digits.push(u32::try_from(digit).map_err(|_| {
            FieldDecodeError::new("structural binary: biguint digit out of u32 range")
        })?);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after biguint digits",
        ));
    }

    Ok(BigUint::new(digits))
}

// Decode one bigint sign marker while preserving the fail-closed signed limb
// contract shared by direct and runtime `Value` decode paths.
fn decode_binary_bigint_sign(sign: i64) -> Result<BigIntSign, FieldDecodeError> {
    match sign {
        -1 => Ok(BigIntSign::Minus),
        0 => Ok(BigIntSign::NoSign),
        1 => Ok(BigIntSign::Plus),
        other => Err(FieldDecodeError::new(format!(
            "structural binary: invalid bigint sign {other}"
        ))),
    }
}

// Decode one optional binary text field from the fixed enum tuple.
fn decode_binary_optional_text<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<Option<&'a str>, FieldDecodeError> {
    let Some(&tag) = raw_bytes.first() else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    if tag == TAG_NULL {
        let end = skip_binary_value(raw_bytes, 0)?;
        if end != raw_bytes.len() {
            return Err(FieldDecodeError::new(format!(
                "structural binary: trailing bytes after {label}"
            )));
        }

        return Ok(None);
    }

    decode_binary_required_text(raw_bytes, label).map(Some)
}

// Materialize a nested value-storage payload whose exact byte range was already
// bounded by tuple/list/map traversal.
fn decode_bounded_value_storage_slice(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let slice = ValueStorageSlice::from_skip_bounded_unchecked(raw_bytes);

    decode_value_storage_slice(slice)
}
