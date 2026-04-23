//! Module: data::structural_field::value_storage
//! Responsibility: owner-local binary `Value` envelope encode and decode.
//! Does not own: top-level `ByKind` dispatch, typed wrapper payload definitions, or storage-key policy.
//! Boundary: `FieldStorageDecode::Value` routes through this module without widening authority over sibling structural lanes.

use crate::db::data::structural_field::FieldDecodeError;
use crate::db::data::structural_field::binary::{
    TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NULL, TAG_TEXT, TAG_TRUE, TAG_UINT64,
    TAG_UNIT, decode_text_scalar_bytes as decode_binary_text_scalar_bytes, parse_binary_head,
    payload_bytes as binary_payload_bytes, push_binary_bool, push_binary_bytes, push_binary_int64,
    push_binary_list_len, push_binary_map_len, push_binary_null, push_binary_tag, push_binary_text,
    push_binary_uint64, push_binary_unit, skip_binary_value,
};
use crate::{
    error::InternalError,
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Nat, Principal, Subaccount,
        Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};

// Carry the output buffer for recursively decoded `Value::List` items.
type ValueArrayDecodeState = Vec<Value>;

// Alias the callback shape for binary value-map walkers.
type ValueBinaryMapEntryFn = unsafe fn(&[u8], &[u8], *mut ()) -> Result<(), FieldDecodeError>;

// Borrowed map-entry payload slices returned by the direct structural
// value-storage split helpers.
type ValueBinarySliceMapEntries<'a> = Vec<(&'a [u8], &'a [u8])>;
type EnumBinaryDecodedPayload<'a> = (String, Option<String>, Option<&'a [u8]>);

const VALUE_BINARY_TAG_ACCOUNT: u8 = 0x80;
const VALUE_BINARY_TAG_DATE: u8 = 0x81;
const VALUE_BINARY_TAG_DECIMAL: u8 = 0x82;
const VALUE_BINARY_TAG_DURATION: u8 = 0x83;
const VALUE_BINARY_TAG_ENUM: u8 = 0x84;
const VALUE_BINARY_TAG_FLOAT32: u8 = 0x85;
const VALUE_BINARY_TAG_FLOAT64: u8 = 0x86;
const VALUE_BINARY_TAG_INT128: u8 = 0x87;
const VALUE_BINARY_TAG_INT_BIG: u8 = 0x88;
const VALUE_BINARY_TAG_PRINCIPAL: u8 = 0x89;
const VALUE_BINARY_TAG_SUBACCOUNT: u8 = 0x8A;
const VALUE_BINARY_TAG_TIMESTAMP: u8 = 0x8B;
const VALUE_BINARY_TAG_UINT128: u8 = 0x8C;
const VALUE_BINARY_TAG_UINT_BIG: u8 = 0x8D;
const VALUE_BINARY_TAG_ULID: u8 = 0x8E;

/// Encode one persisted `FieldStorageDecode::Value` payload through the
/// owner-local structural value-storage contract.
pub(in crate::db) fn encode_structural_value_storage_bytes(
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_value_storage_binary_bytes(value)
}

/// Encode one canonical structural value-storage `NULL` payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_null_bytes() -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_null(&mut encoded);

    encoded
}

/// Encode one canonical structural value-storage `unit` payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_unit_bytes() -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_unit(&mut encoded);

    encoded
}

/// Encode one canonical structural value-storage boolean payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_bool_bytes(value: bool) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_bool(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage unsigned integer payload
/// without constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_u64_bytes(value: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_uint64(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage signed integer payload
/// without constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_i64_bytes(value: i64) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_int64(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage text payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_text(value: &str) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_text(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage bytes payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_blob_bytes(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_bytes(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage account payload.
pub(in crate::db) fn encode_account(value: Account) -> Result<Vec<u8>, InternalError> {
    let bytes = value
        .to_bytes()
        .map_err(InternalError::persisted_row_encode_failed)?;
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_ACCOUNT, |out| {
        push_binary_bytes(out, bytes.as_slice());
        Ok(())
    })
    .expect("account payload encode should be infallible");

    Ok(encoded)
}

/// Encode one canonical structural value-storage decimal payload.
pub(in crate::db) fn encode_decimal(value: Decimal) -> Vec<u8> {
    let parts = value.parts();
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_DECIMAL, |out| {
        push_binary_list_len(out, 2);
        push_binary_bytes(out, &parts.mantissa().to_be_bytes());
        push_binary_uint64(out, u64::from(parts.scale()));
        Ok(())
    })
    .expect("decimal payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage int128 payload.
pub(in crate::db) fn encode_int128(value: crate::types::Int128) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_INT128, |out| {
        push_binary_bytes(out, &value.get().to_be_bytes());
        Ok(())
    })
    .expect("int128 payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage uint128 payload.
pub(in crate::db) fn encode_nat128(value: crate::types::Nat128) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_UINT128, |out| {
        push_binary_bytes(out, &value.get().to_be_bytes());
        Ok(())
    })
    .expect("uint128 payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage bigint payload.
pub(in crate::db) fn encode_int(value: &Int) -> Vec<u8> {
    let (is_negative, digits) = value.sign_and_u32_digits();
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_INT_BIG, |out| {
        push_binary_list_len(out, 2);
        push_binary_int64(
            out,
            if digits.is_empty() {
                0
            } else if is_negative {
                -1
            } else {
                1
            },
        );
        push_binary_u32_digit_list(out, digits.as_slice());
        Ok(())
    })
    .expect("bigint payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage biguint payload.
pub(in crate::db) fn encode_nat(value: &Nat) -> Vec<u8> {
    let digits = value.u32_digits();
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_UINT_BIG, |out| {
        push_binary_u32_digit_list(out, digits.as_slice());
        Ok(())
    })
    .expect("biguint payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage float32 payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_float32_bytes(value: Float32) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_FLOAT32, |out| {
        push_binary_bytes(out, &value.to_be_bytes());
        Ok(())
    })
    .expect("float32 payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage float64 payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_float64_bytes(value: Float64) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_FLOAT64, |out| {
        push_binary_bytes(out, &value.to_be_bytes());
        Ok(())
    })
    .expect("float64 payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage date payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_date_bytes(value: Date) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_DATE, |out| {
        push_binary_int64(out, i64::from(value.as_days_since_epoch()));
        Ok(())
    })
    .expect("date payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage duration payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_duration_bytes(value: Duration) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_DURATION, |out| {
        push_binary_uint64(out, value.as_millis());
        Ok(())
    })
    .expect("duration payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage principal payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_principal_bytes(
    value: Principal,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_PRINCIPAL, |out| {
        push_binary_bytes(
            out,
            value
                .stored_bytes()
                .map_err(InternalError::persisted_row_encode_failed)?,
        );
        Ok(())
    })?;

    Ok(encoded)
}

/// Encode one canonical structural value-storage subaccount payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_subaccount_bytes(
    value: Subaccount,
) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_SUBACCOUNT, |out| {
        push_binary_bytes(out, value.as_slice());
        Ok(())
    })
    .expect("subaccount payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage timestamp payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_timestamp_bytes(value: Timestamp) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_TIMESTAMP, |out| {
        push_binary_int64(out, value.as_millis());
        Ok(())
    })
    .expect("timestamp payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage ULID payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_ulid_bytes(value: Ulid) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_ULID, |out| {
        push_binary_bytes(out, &value.to_bytes());
        Ok(())
    })
    .expect("ulid payload encode should be infallible");

    encoded
}

/// Encode one canonical structural value-storage list payload from already
/// encoded nested value payload slices.
pub(in crate::db) fn encode_list_item(items: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, items.len());
    for item in items {
        encoded.extend_from_slice(item);
    }

    encoded
}

/// Encode one canonical structural value-storage map payload from already
/// encoded nested key/value payload slices.
pub(in crate::db) fn encode_map_entry(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_map_len(&mut encoded, entries.len());
    for (key_bytes, value_bytes) in entries {
        encoded.extend_from_slice(key_bytes);
        encoded.extend_from_slice(value_bytes);
    }

    encoded
}

/// Encode one persisted `FieldStorageDecode::Value` payload through the
/// parallel Structural Binary v1 `Value` envelope.
pub(super) fn encode_structural_value_storage_binary_bytes(
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_value_storage_binary_into(&mut encoded, value)?;

    Ok(encoded)
}

/// Decode one `FieldStorageDecode::Value` payload from the parallel
/// Structural Binary v1 `Value` envelope.
pub(super) fn decode_structural_value_storage_binary_bytes(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let Some(&tag) = raw_bytes.first() else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value payload",
        ));
    };

    // Phase 1: decode the unambiguous generic root tags directly.
    let generic = match tag {
        TAG_NULL => Some(Value::Null),
        crate::db::data::structural_field::binary::TAG_UNIT => Some(Value::Unit),
        TAG_FALSE => Some(Value::Bool(false)),
        TAG_TRUE => Some(Value::Bool(true)),
        TAG_INT64 => Some(decode_binary_i64_value(raw_bytes)?),
        TAG_UINT64 => Some(decode_binary_u64_value(raw_bytes)?),
        TAG_TEXT => Some(decode_binary_text_value(raw_bytes)?),
        TAG_BYTES => Some(decode_binary_blob_value(raw_bytes)?),
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
        VALUE_BINARY_TAG_ACCOUNT => decode_binary_account_value(raw_bytes),
        VALUE_BINARY_TAG_DATE => decode_binary_date_value(raw_bytes),
        VALUE_BINARY_TAG_DECIMAL => decode_binary_decimal_value(raw_bytes),
        VALUE_BINARY_TAG_DURATION => decode_binary_duration_value(raw_bytes),
        VALUE_BINARY_TAG_ENUM => decode_binary_enum_value(raw_bytes),
        VALUE_BINARY_TAG_FLOAT32 => decode_binary_float32_value(raw_bytes),
        VALUE_BINARY_TAG_FLOAT64 => decode_binary_float64_value(raw_bytes),
        VALUE_BINARY_TAG_INT128 => decode_binary_int128_value(raw_bytes),
        VALUE_BINARY_TAG_INT_BIG => decode_binary_int_big_value(raw_bytes),
        VALUE_BINARY_TAG_PRINCIPAL => decode_binary_principal_value(raw_bytes),
        VALUE_BINARY_TAG_SUBACCOUNT => decode_binary_subaccount_value(raw_bytes),
        VALUE_BINARY_TAG_TIMESTAMP => decode_binary_timestamp_value(raw_bytes),
        VALUE_BINARY_TAG_UINT128 => decode_binary_uint128_value(raw_bytes),
        VALUE_BINARY_TAG_UINT_BIG => decode_binary_uint_big_value(raw_bytes),
        VALUE_BINARY_TAG_ULID => decode_binary_ulid_value(raw_bytes),
        other => Err(FieldDecodeError::new(format!(
            "structural binary: unsupported value tag 0x{other:02X}"
        ))),
    }
}

/// Validate one `FieldStorageDecode::Value` payload from the parallel
/// Structural Binary v1 `Value` envelope without rebuilding it eagerly.
pub(super) fn validate_structural_value_storage_binary_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    decode_structural_value_storage_binary_bytes(raw_bytes).map(|_| ())
}

// Encode one runtime `Value` into the parallel Structural Binary v1 envelope.
fn encode_value_storage_binary_into(out: &mut Vec<u8>, value: &Value) -> Result<(), InternalError> {
    match value {
        Value::Null => push_binary_null(out),
        Value::Unit => push_binary_tag(out, crate::db::data::structural_field::binary::TAG_UNIT),
        Value::Blob(value) => push_binary_bytes(out, value.as_slice()),
        Value::Bool(value) => push_binary_bool(out, *value),
        Value::Int(value) => push_binary_int64(out, *value),
        Value::Uint(value) => push_binary_uint64(out, *value),
        Value::Text(value) => push_binary_text(out, value),
        Value::List(items) => push_value_binary_list_payload(out, items.as_slice())?,
        Value::Map(entries) => push_value_binary_map_payload(out, entries.as_slice())?,
        Value::Account(value) => push_binary_account_value(out, *value)?,
        Value::Date(value) => push_value_binary_payload_tag(out, VALUE_BINARY_TAG_DATE, |out| {
            push_binary_int64(out, i64::from(value.as_days_since_epoch()));
            Ok(())
        })?,
        Value::Decimal(value) => push_binary_decimal_value(out, *value)?,
        Value::Duration(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_DURATION, |out| {
                push_binary_uint64(out, value.as_millis());
                Ok(())
            })?;
        }
        Value::Enum(value) => push_binary_enum_value(out, value)?,
        Value::Float32(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_FLOAT32, |out| {
                push_binary_bytes(out, &value.to_be_bytes());
                Ok(())
            })?;
        }
        Value::Float64(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_FLOAT64, |out| {
                push_binary_bytes(out, &value.to_be_bytes());
                Ok(())
            })?;
        }
        Value::Int128(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_INT128, |out| {
                push_binary_bytes(out, &value.get().to_be_bytes());
                Ok(())
            })?;
        }
        Value::IntBig(value) => push_binary_int_big_value(out, value)?,
        Value::Principal(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_PRINCIPAL, |out| {
                push_binary_bytes(
                    out,
                    value
                        .stored_bytes()
                        .map_err(InternalError::persisted_row_encode_failed)?,
                );
                Ok(())
            })?;
        }
        Value::Subaccount(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_SUBACCOUNT, |out| {
                push_binary_bytes(out, value.as_slice());
                Ok(())
            })?;
        }
        Value::Timestamp(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_TIMESTAMP, |out| {
                push_binary_int64(out, value.as_millis());
                Ok(())
            })?;
        }
        Value::Uint128(value) => {
            push_value_binary_payload_tag(out, VALUE_BINARY_TAG_UINT128, |out| {
                push_binary_bytes(out, &value.get().to_be_bytes());
                Ok(())
            })?;
        }
        Value::UintBig(value) => push_binary_uint_big_value(out, value)?,
        Value::Ulid(value) => push_value_binary_payload_tag(out, VALUE_BINARY_TAG_ULID, |out| {
            push_binary_bytes(out, &value.to_bytes());
            Ok(())
        })?,
    }

    Ok(())
}

// Encode one binary `Value::List` payload as a list of recursively encoded
// nested `Value` items.
fn push_value_binary_list_payload(out: &mut Vec<u8>, items: &[Value]) -> Result<(), InternalError> {
    push_binary_list_len(out, items.len());
    for item in items {
        encode_value_storage_binary_into(out, item)?;
    }

    Ok(())
}

// Encode one binary `Value::Map` payload as a canonical map of recursively
// encoded key/value pairs.
fn push_value_binary_map_payload(
    out: &mut Vec<u8>,
    entries: &[(Value, Value)],
) -> Result<(), InternalError> {
    push_binary_map_len(out, entries.len());
    for (key, value) in entries {
        encode_value_storage_binary_into(out, key)?;
        encode_value_storage_binary_into(out, value)?;
    }

    Ok(())
}

// Encode one locally tagged `Value` payload that carries exactly one nested
// Structural Binary v1 payload.
fn push_value_binary_payload_tag<F>(
    out: &mut Vec<u8>,
    tag: u8,
    push_payload: F,
) -> Result<(), InternalError>
where
    F: FnOnce(&mut Vec<u8>) -> Result<(), InternalError>,
{
    push_binary_tag(out, tag);
    push_payload(out)
}

// Encode one binary `Value::Account` payload through Account's fixed-size byte
// contract instead of routing through the general `Value` lane.
fn push_binary_account_value(out: &mut Vec<u8>, value: Account) -> Result<(), InternalError> {
    push_value_binary_payload_tag(out, VALUE_BINARY_TAG_ACCOUNT, |out| {
        let bytes = value
            .to_stored_bytes()
            .map_err(InternalError::persisted_row_encode_failed)?;
        push_binary_bytes(out, &bytes);

        Ok(())
    })
}

// Encode one binary decimal payload as `(mantissa_bytes, scale)` without
// embedding a generic field-name object model in bytes.
fn push_binary_decimal_value(out: &mut Vec<u8>, value: Decimal) -> Result<(), InternalError> {
    push_value_binary_payload_tag(out, VALUE_BINARY_TAG_DECIMAL, |out| {
        let parts = value.parts();
        push_binary_list_len(out, 2);
        push_binary_bytes(out, &parts.mantissa().to_be_bytes());
        push_binary_uint64(out, u64::from(parts.scale()));

        Ok(())
    })
}

// Encode one binary `Value::Enum` payload using a fixed positional tuple:
// `(variant, path, payload)`.
fn push_binary_enum_value(out: &mut Vec<u8>, value: &ValueEnum) -> Result<(), InternalError> {
    push_value_binary_payload_tag(out, VALUE_BINARY_TAG_ENUM, |out| {
        push_binary_list_len(out, 3);
        push_binary_text(out, value.variant());
        match value.path() {
            Some(path) => push_binary_text(out, path),
            None => push_binary_null(out),
        }
        match value.payload() {
            Some(payload) => encode_value_storage_binary_into(out, payload)?,
            None => push_binary_null(out),
        }

        Ok(())
    })
}

/// Encode one canonical enum payload from its variant, optional strict path,
/// and already encoded nested payload bytes without constructing `Value`.
pub(in crate::db) fn encode_enum(
    variant: &str,
    path: Option<&str>,
    payload: Option<&[u8]>,
) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_value_binary_payload_tag(&mut encoded, VALUE_BINARY_TAG_ENUM, |out| {
        push_binary_list_len(out, 3);
        push_binary_text(out, variant);
        match path {
            Some(path) => push_binary_text(out, path),
            None => push_binary_null(out),
        }
        match payload {
            Some(payload) => out.extend_from_slice(payload),
            None => push_binary_null(out),
        }

        Ok(())
    })
    .expect("enum payload encode should be infallible");

    encoded
}

// Encode one binary `Value::IntBig` payload as `(sign, limbs)`.
fn push_binary_int_big_value(out: &mut Vec<u8>, value: &Int) -> Result<(), InternalError> {
    let (is_negative, digits) = value.sign_and_u32_digits();
    push_value_binary_payload_tag(out, VALUE_BINARY_TAG_INT_BIG, |out| {
        push_binary_list_len(out, 2);
        push_binary_int64(
            out,
            if digits.is_empty() {
                0
            } else if is_negative {
                -1
            } else {
                1
            },
        );
        push_binary_u32_digit_list(out, digits.as_slice());

        Ok(())
    })
}

// Encode one binary `Value::UintBig` payload as a limb sequence.
fn push_binary_uint_big_value(out: &mut Vec<u8>, value: &Nat) -> Result<(), InternalError> {
    let digits = value.u32_digits();
    push_value_binary_payload_tag(out, VALUE_BINARY_TAG_UINT_BIG, |out| {
        push_binary_u32_digit_list(out, digits.as_slice());

        Ok(())
    })
}

// Encode one canonical big-integer limb sequence.
fn push_binary_u32_digit_list(out: &mut Vec<u8>, digits: &[u32]) {
    push_binary_list_len(out, digits.len());
    for digit in digits {
        push_binary_uint64(out, u64::from(*digit));
    }
}

// Skip one binary `Value` envelope without delegating nested `Value` items
// back to the generic Structural Binary walker.
fn skip_value_storage_binary_value(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    let Some(&tag) = raw_bytes.get(offset) else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value payload",
        ));
    };

    match tag {
        TAG_NULL
        | crate::db::data::structural_field::binary::TAG_UNIT
        | TAG_FALSE
        | TAG_TRUE
        | TAG_INT64
        | TAG_UINT64
        | TAG_TEXT
        | TAG_BYTES => skip_binary_value(raw_bytes, offset),
        TAG_LIST => skip_value_storage_binary_list(raw_bytes, offset),
        TAG_MAP => skip_value_storage_binary_map(raw_bytes, offset),
        VALUE_BINARY_TAG_ACCOUNT
        | VALUE_BINARY_TAG_DATE
        | VALUE_BINARY_TAG_DECIMAL
        | VALUE_BINARY_TAG_DURATION
        | VALUE_BINARY_TAG_ENUM
        | VALUE_BINARY_TAG_FLOAT32
        | VALUE_BINARY_TAG_FLOAT64
        | VALUE_BINARY_TAG_INT128
        | VALUE_BINARY_TAG_INT_BIG
        | VALUE_BINARY_TAG_PRINCIPAL
        | VALUE_BINARY_TAG_SUBACCOUNT
        | VALUE_BINARY_TAG_TIMESTAMP
        | VALUE_BINARY_TAG_UINT128
        | VALUE_BINARY_TAG_UINT_BIG
        | VALUE_BINARY_TAG_ULID => skip_value_storage_binary_value(raw_bytes, offset + 1),
        other => Err(FieldDecodeError::new(format!(
            "structural binary: unsupported value tag 0x{other:02X}"
        ))),
    }
}

// Skip one binary value list by recursing through nested `Value` items.
fn skip_value_storage_binary_list(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value list payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(
            "structural binary: expected value list payload",
        ));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
    }

    Ok(cursor)
}

// Skip one binary value map by recursing through nested `Value` keys and
// values.
fn skip_value_storage_binary_map(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value map payload",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(
            "structural binary: expected value map payload",
        ));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
    }

    Ok(cursor)
}

// Walk one binary value list and yield each nested `Value` item slice.
fn walk_value_storage_binary_list_items(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_item: unsafe fn(&[u8], *mut ()) -> Result<(), FieldDecodeError>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value list payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        let item_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        unsafe { on_item(&raw_bytes[item_start..cursor], context)? };
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Walk one binary value map and yield each nested key/value slice pair.
fn walk_value_storage_binary_map_entries(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_entry: ValueBinaryMapEntryFn,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value map payload",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(shape_label));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        let key_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        unsafe {
            on_entry(
                &raw_bytes[key_start..value_start],
                &raw_bytes[value_start..cursor],
                context,
            )?;
        };
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Decode one top-level i64 generic binary value.
fn decode_binary_i64_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    if tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected i64 integer payload",
        ));
    }
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after integer payload",
        ));
    }

    let bytes: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid i64 payload"))?;

    Ok(Value::Int(i64::from_be_bytes(bytes)))
}

// Decode one top-level u64 generic binary value.
fn decode_binary_u64_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    if tag != TAG_UINT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected u64 integer payload",
        ));
    }
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after integer payload",
        ));
    }

    let bytes: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid u64 payload"))?;

    Ok(Value::Uint(u64::from_be_bytes(bytes)))
}

// Decode one top-level text generic binary value.
fn decode_binary_text_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated text payload",
        ));
    };
    if tag != TAG_TEXT {
        return Err(FieldDecodeError::new(
            "structural binary: expected text payload",
        ));
    }
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after text payload",
        ));
    }

    Ok(Value::Text(
        decode_binary_text_scalar_bytes(raw_bytes, len, payload_start)?.to_string(),
    ))
}

// Decode one top-level bytes generic binary value.
fn decode_binary_blob_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated byte payload",
        ));
    };
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected byte payload",
        ));
    }
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after byte payload",
        ));
    }

    Ok(Value::Blob(
        binary_payload_bytes(raw_bytes, len, payload_start, "byte payload")?.to_vec(),
    ))
}

// Extract the single nested payload carried by one local `Value` binary tag.
fn decode_value_storage_binary_payload<'a>(
    raw_bytes: &'a [u8],
    expected_tag: u8,
    label: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let Some((&tag, _)) = raw_bytes.split_first() else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label} payload"
        )));
    };
    if tag != expected_tag {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label} payload"
        )));
    }

    let payload_end = skip_value_storage_binary_value(raw_bytes, 1)?;
    if payload_end != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label} payload"
        )));
    }

    raw_bytes.get(1..payload_end).ok_or_else(|| {
        FieldDecodeError::new(format!("structural binary: truncated {label} payload"))
    })
}

// Split a fixed-length binary tuple into generic item slices without
// allocating a generic intermediate tree.
fn split_binary_tuple_items<'a>(
    raw_bytes: &'a [u8],
    expected_len: u32,
    label: &'static str,
) -> Result<Vec<&'a [u8]>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    if tag != TAG_LIST || len != expected_len {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    let mut cursor = payload_start;
    let mut items = Vec::with_capacity(expected_len as usize);
    for _ in 0..expected_len {
        let item_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        items.push(&raw_bytes[item_start..cursor]);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label}"
        )));
    }

    Ok(items)
}

// Split a fixed-length binary tuple whose items are nested `Value` envelopes.
fn split_binary_value_storage_tuple_items<'a>(
    raw_bytes: &'a [u8],
    expected_len: u32,
    label: &'static str,
) -> Result<Vec<&'a [u8]>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    if tag != TAG_LIST || len != expected_len {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    let mut cursor = payload_start;
    let mut items = Vec::with_capacity(expected_len as usize);
    for _ in 0..expected_len {
        let item_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        items.push(&raw_bytes[item_start..cursor]);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label}"
        )));
    }

    Ok(items)
}

// Decode one local account payload from its fixed byte representation.
fn decode_binary_account_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ACCOUNT, "account")?;
    let Some((tag, len, payload_start)) = parse_binary_head(payload, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated account bytes",
        ));
    };
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected account bytes",
        ));
    }

    let account = Account::try_from_bytes(binary_payload_bytes(
        payload,
        len,
        payload_start,
        "account bytes",
    )?)
    .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))?;

    Ok(Value::Account(account))
}

// Decode one local date payload from canonical signed day-count form.
fn decode_binary_date_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DATE, "date")?;
    Date::try_from_i64(decode_binary_required_i64(payload, "date days")?)
        .map(Value::Date)
        .ok_or_else(|| FieldDecodeError::new("structural binary: date day count out of range"))
}

// Decode one local decimal payload from `(mantissa_bytes, scale)`.
fn decode_binary_decimal_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DECIMAL, "decimal")?;
    let parts = split_binary_tuple_items(payload, 2, "decimal tuple")?;
    let mantissa_bytes = decode_binary_required_bytes(parts[0], "decimal mantissa")?;
    let scale = decode_binary_required_u64(parts[1], "decimal scale")?;
    let mantissa_buf: [u8; 16] = mantissa_bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid decimal mantissa length"))?;
    let scale = u32::try_from(scale)
        .map_err(|_| FieldDecodeError::new("structural binary: decimal scale out of u32 range"))?;

    Ok(Value::Decimal(decode_binary_decimal_mantissa_scale(
        i128::from_be_bytes(mantissa_buf),
        scale,
    )?))
}

// Decode one local duration payload from canonical millis.
fn decode_binary_duration_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DURATION, "duration")?;
    Ok(Value::Duration(Duration::from_millis(
        decode_binary_required_u64(payload, "duration millis")?,
    )))
}

// Decode one local enum payload from the fixed positional tuple
// `(variant, path, payload)`.
fn decode_binary_enum_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ENUM, "enum")?;
    let fields = split_binary_value_storage_tuple_items(payload, 3, "enum tuple")?;
    let variant = decode_binary_required_text(fields[0], "enum variant")?;
    let path = decode_binary_optional_text(fields[1], "enum path")?;
    let nested = decode_binary_optional_nested_value(fields[2], "enum payload")?;

    let mut value = ValueEnum::new(variant, path);
    if let Some(payload) = nested {
        value = value.with_payload(payload);
    }

    Ok(Value::Enum(value))
}

/// Decode one canonical enum payload into its variant, optional strict path,
/// and borrowed nested payload bytes without constructing `Value`.
pub(in crate::db) fn decode_enum(
    raw_bytes: &[u8],
) -> Result<EnumBinaryDecodedPayload<'_>, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ENUM, "enum")?;
    let fields = split_binary_value_storage_tuple_items(payload, 3, "enum tuple")?;
    let variant = decode_binary_required_text(fields[0], "enum variant")?.to_owned();
    let path = decode_binary_optional_text(fields[1], "enum path")?.map(str::to_owned);
    let payload = if structural_value_storage_bytes_are_null(fields[2])? {
        None
    } else {
        Some(fields[2])
    };

    Ok((variant, path, payload))
}

// Decode one local float32 payload from its canonical finite-byte form.
fn decode_binary_float32_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_FLOAT32, "float32")?;
    let bytes: [u8; 4] = decode_binary_required_bytes(payload, "float32 bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid float32 length"))?;
    let value = Float32::try_from_bytes(&bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))?;

    Ok(Value::Float32(value))
}

// Decode one local float64 payload from its canonical finite-byte form.
fn decode_binary_float64_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_FLOAT64, "float64")?;
    let bytes: [u8; 8] = decode_binary_required_bytes(payload, "float64 bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid float64 length"))?;
    let value = Float64::try_from_bytes(&bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))?;

    Ok(Value::Float64(value))
}

// Decode one local int128 payload from canonical big-endian bytes.
fn decode_binary_int128_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_INT128, "int128")?;
    let bytes: [u8; 16] = decode_binary_required_bytes(payload, "int128 bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid int128 length"))?;

    Ok(Value::Int128(crate::types::Int128::from(
        i128::from_be_bytes(bytes),
    )))
}

// Decode one local arbitrary-precision signed integer payload from
// `(sign, limbs)`.
fn decode_binary_int_big_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_INT_BIG, "bigint")?;
    let parts = split_binary_tuple_items(payload, 2, "bigint tuple")?;
    let sign = decode_binary_required_i64(parts[0], "bigint sign")?;
    let magnitude = decode_binary_biguint_digits(parts[1])?;
    let sign = match sign {
        -1 => BigIntSign::Minus,
        0 => BigIntSign::NoSign,
        1 => BigIntSign::Plus,
        other => {
            return Err(FieldDecodeError::new(format!(
                "structural binary: invalid bigint sign {other}"
            )));
        }
    };
    let wrapped = WrappedInt::from(BigInt::from_biguint(sign, magnitude));

    Ok(Value::IntBig(Int::from(wrapped)))
}

// Decode one local principal payload from canonical raw bytes.
fn decode_binary_principal_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_PRINCIPAL, "principal")?;
    let bytes = decode_binary_required_bytes(payload, "principal bytes")?;
    let principal = Principal::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))?;

    Ok(Value::Principal(principal))
}

// Decode one local subaccount payload from canonical raw bytes.
fn decode_binary_subaccount_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_SUBACCOUNT, "subaccount")?;
    let bytes: [u8; 32] = decode_binary_required_bytes(payload, "subaccount bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid subaccount length"))?;

    Ok(Value::Subaccount(Subaccount::from_array(bytes)))
}

// Decode one local timestamp payload from canonical unix millis.
fn decode_binary_timestamp_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_TIMESTAMP, "timestamp")?;
    Ok(Value::Timestamp(Timestamp::from_millis(
        decode_binary_required_i64(payload, "timestamp millis")?,
    )))
}

// Decode one local uint128 payload from canonical big-endian bytes.
fn decode_binary_uint128_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_UINT128, "uint128")?;
    let bytes: [u8; 16] = decode_binary_required_bytes(payload, "uint128 bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid uint128 length"))?;

    Ok(Value::Uint128(crate::types::Nat128::from(
        u128::from_be_bytes(bytes),
    )))
}

// Decode one local arbitrary-precision unsigned integer payload from a limb
// sequence.
fn decode_binary_uint_big_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_UINT_BIG, "biguint")?;
    let magnitude = decode_binary_biguint_digits(payload)?;
    let wrapped = WrappedNat::from(magnitude);

    Ok(Value::UintBig(Nat::from(wrapped)))
}

// Decode one local ULID payload from canonical fixed-width bytes.
fn decode_binary_ulid_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ULID, "ulid")?;
    let bytes: [u8; 16] = decode_binary_required_bytes(payload, "ulid bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid ulid length"))?;

    Ok(Value::Ulid(Ulid::from_bytes(bytes)))
}

// Decode one binary list payload recursively from raw item bytes.
fn decode_value_storage_binary_list_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let mut items = Vec::new();
    walk_value_storage_binary_list_items(
        raw_bytes,
        "expected structural binary list for value list payload",
        "structural binary: trailing bytes after value list payload",
        (&raw mut items).cast(),
        push_value_binary_array_item,
    )?;

    Ok(Value::List(items))
}

// Decode one binary map payload recursively while preserving runtime map
// invariants.
fn decode_value_storage_binary_map_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let mut entries = Vec::new();
    walk_value_storage_binary_map_entries(
        raw_bytes,
        "expected structural binary map for value map payload",
        "structural binary: trailing bytes after value map payload",
        (&raw mut entries).cast(),
        push_value_binary_map_entry,
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

// Decode one required binary bytes payload.
fn decode_binary_required_bytes<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_BYTES {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    binary_payload_bytes(raw_bytes, len, payload_start, label)
}

// Decode one required binary text payload.
fn decode_binary_required_text<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a str, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_TEXT {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    decode_binary_text_scalar_bytes(raw_bytes, len, payload_start)
}

// Decode one required binary i64 payload.
fn decode_binary_required_i64(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<i64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    let bytes: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, label)?
        .try_into()
        .map_err(|_| FieldDecodeError::new(format!("structural binary: invalid {label}")))?;

    Ok(i64::from_be_bytes(bytes))
}

// Decode one required binary u64 payload.
fn decode_binary_required_u64(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<u64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label}"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_UINT64 || len != 8 {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label}"
        )));
    }

    let bytes: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, label)?
        .try_into()
        .map_err(|_| FieldDecodeError::new(format!("structural binary: invalid {label}")))?;

    Ok(u64::from_be_bytes(bytes))
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

// Decode one optional nested binary `Value` field from the fixed enum tuple.
fn decode_binary_optional_nested_value(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<Option<Value>, FieldDecodeError> {
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

    decode_structural_value_storage_binary_bytes(raw_bytes).map(Some)
}

// Apply Decimal's mantissa/scale validation locally so the binary value
// envelope does not silently normalize invalid payloads to zero.
fn decode_binary_decimal_mantissa_scale(
    mantissa: i128,
    scale: u32,
) -> Result<Decimal, FieldDecodeError> {
    if scale <= Decimal::max_supported_scale() {
        return Ok(Decimal::from_i128_with_scale(mantissa, scale));
    }

    let mut value = mantissa;
    let mut normalized_scale = scale;
    while normalized_scale > Decimal::max_supported_scale() {
        if value == 0 {
            return Ok(Decimal::from_i128_with_scale(
                0,
                Decimal::max_supported_scale(),
            ));
        }
        if value % 10 != 0 {
            return Err(FieldDecodeError::new(
                "structural binary: invalid decimal payload",
            ));
        }
        value /= 10;
        normalized_scale -= 1;
    }

    Ok(Decimal::from_i128_with_scale(value, normalized_scale))
}

// Push one recursively tagged `Value` list item into the decoded buffer.
//
// Safety:
// `context` must be a valid `ValueArrayDecodeState`.
fn push_value_binary_array_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let items = unsafe { &mut *context.cast::<ValueArrayDecodeState>() };
    items.push(decode_structural_value_storage_binary_bytes(item_bytes)?);

    Ok(())
}

// Push one decoded binary `Value::Map` entry into the runtime entry buffer.
//
// Safety:
// `context` must be a valid `Vec<(Value, Value)>`.
fn push_value_binary_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let entries = unsafe { &mut *context.cast::<Vec<(Value, Value)>>() };
    entries.push((
        decode_structural_value_storage_binary_bytes(key_bytes)?,
        decode_structural_value_storage_binary_bytes(value_bytes)?,
    ));

    Ok(())
}

// Decode one `FieldStorageDecode::Value` payload directly from the externally
// tagged `Value` wire shape without routing through serde's recursive enum
// visitor graph.
pub(in crate::db) fn decode_structural_value_storage_bytes(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    decode_structural_value_storage_binary_bytes(raw_bytes)
}

/// Validate one `FieldStorageDecode::Value` payload through the canonical
/// Structural Binary v1 owner.
pub(in crate::db) fn validate_structural_value_storage_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    validate_structural_value_storage_binary_bytes(raw_bytes)
}

/// Return `true` when one structural value-storage payload is the canonical
/// encoded `NULL` form and reject malformed bytes fail-closed.
pub(in crate::db) fn structural_value_storage_bytes_are_null(
    raw_bytes: &[u8],
) -> Result<bool, FieldDecodeError> {
    let Some((tag, _, _)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated null payload",
        ));
    };
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after null payload",
        ));
    }

    Ok(tag == TAG_NULL)
}

/// Decode one canonical structural value-storage `unit` payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_unit_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    let Some((tag, _, _)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated unit payload",
        ));
    };
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_UNIT {
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
    let Some((tag, _, _)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated bool payload",
        ));
    };
    let end = skip_value_storage_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after bool payload",
        ));
    }

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
pub(in crate::db) fn decode_text(raw_bytes: &[u8]) -> Result<String, FieldDecodeError> {
    decode_binary_required_text(raw_bytes, "text payload").map(str::to_owned)
}

/// Decode one canonical structural value-storage account payload.
pub(in crate::db) fn decode_account(raw_bytes: &[u8]) -> Result<Account, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_ACCOUNT, "account")?;
    let bytes = decode_binary_required_bytes(payload, "account bytes")?;

    Account::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

/// Decode one canonical structural value-storage decimal payload.
pub(in crate::db) fn decode_decimal(raw_bytes: &[u8]) -> Result<Decimal, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DECIMAL, "decimal")?;
    let parts = split_binary_tuple_items(payload, 2, "decimal tuple")?;
    let mantissa_bytes = decode_binary_required_bytes(parts[0], "decimal mantissa")?;
    let scale = decode_binary_required_u64(parts[1], "decimal scale")?;
    let mantissa_buf: [u8; 16] = mantissa_bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid decimal mantissa length"))?;
    let scale = u32::try_from(scale)
        .map_err(|_| FieldDecodeError::new("structural binary: decimal scale out of u32 range"))?;

    decode_binary_decimal_mantissa_scale(i128::from_be_bytes(mantissa_buf), scale)
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

    Ok(crate::types::Int128::from(i128::from_be_bytes(bytes)))
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

    Ok(crate::types::Nat128::from(u128::from_be_bytes(bytes)))
}

/// Decode one canonical structural value-storage bigint payload.
pub(in crate::db) fn decode_int(raw_bytes: &[u8]) -> Result<Int, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_INT_BIG, "bigint")?;
    let parts = split_binary_tuple_items(payload, 2, "bigint tuple")?;
    let sign = decode_binary_required_i64(parts[0], "bigint sign")?;
    let magnitude = decode_binary_biguint_digits(parts[1])?;
    let sign = match sign {
        -1 => BigIntSign::Minus,
        0 => BigIntSign::NoSign,
        1 => BigIntSign::Plus,
        other => {
            return Err(FieldDecodeError::new(format!(
                "structural binary: invalid bigint sign {other}"
            )));
        }
    };

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

    Float32::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

/// Decode one canonical structural value-storage float64 payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_float64_bytes(
    raw_bytes: &[u8],
) -> Result<Float64, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_FLOAT64, "float64")?;
    let bytes = decode_binary_required_bytes(payload, "float64 bytes")?;

    Float64::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

/// Decode one canonical structural value-storage date payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_date_bytes(
    raw_bytes: &[u8],
) -> Result<Date, FieldDecodeError> {
    let payload = decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DATE, "date")?;
    let days = decode_binary_required_i64(payload, "date days")?;

    Date::try_from_i64(days)
        .ok_or_else(|| FieldDecodeError::new("structural binary: date day count out of range"))
}

/// Decode one canonical structural value-storage duration payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_duration_bytes(
    raw_bytes: &[u8],
) -> Result<Duration, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_DURATION, "duration")?;

    Ok(Duration::from_millis(decode_binary_required_u64(
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

    Principal::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

/// Decode one canonical structural value-storage subaccount payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_subaccount_bytes(
    raw_bytes: &[u8],
) -> Result<Subaccount, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_SUBACCOUNT, "subaccount")?;
    let bytes: [u8; 32] = decode_binary_required_bytes(payload, "subaccount bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid subaccount length"))?;

    Ok(Subaccount::from_array(bytes))
}

/// Decode one canonical structural value-storage timestamp payload without
/// materializing a runtime `Value`.
pub(in crate::db) fn decode_structural_value_storage_timestamp_bytes(
    raw_bytes: &[u8],
) -> Result<Timestamp, FieldDecodeError> {
    let payload =
        decode_value_storage_binary_payload(raw_bytes, VALUE_BINARY_TAG_TIMESTAMP, "timestamp")?;

    Ok(Timestamp::from_millis(decode_binary_required_i64(
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
    let bytes: [u8; 16] = decode_binary_required_bytes(payload, "ulid bytes")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid ulid length"))?;

    Ok(Ulid::from_bytes(bytes))
}

/// Split one structural value-storage list payload into borrowed nested item
/// payload slices without materializing runtime `Value` items.
pub(in crate::db) fn decode_list_item(raw_bytes: &[u8]) -> Result<Vec<&[u8]>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value list payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(
            "structural binary: expected value list payload",
        ));
    }

    let mut cursor = payload_start;
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let item_start = cursor;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        items.push(&raw_bytes[item_start..cursor]);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after value list payload",
        ));
    }

    Ok(items)
}

/// Split one structural value-storage map payload into borrowed nested key and
/// value payload slices without materializing runtime `Value` entries.
pub(in crate::db) fn decode_map_entry(
    raw_bytes: &[u8],
) -> Result<ValueBinarySliceMapEntries<'_>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value map payload",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(
            "structural binary: expected value map payload",
        ));
    }

    let mut cursor = payload_start;
    let mut entries = Vec::with_capacity(len as usize);
    for _ in 0..len {
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
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after value map payload",
        ));
    }

    Ok(entries)
}

// Normalize decoded map entries in place when they satisfy the runtime map
// invariants, but preserve the original decoded order when validation rejects
// the shape. This keeps current semantics without cloning the whole entry list.
pub(super) fn normalize_map_entries_or_preserve(mut entries: Vec<(Value, Value)>) -> Value {
    if Value::validate_map_entries(&entries).is_err() {
        return Value::Map(entries);
    }

    Value::sort_map_entries_in_place(entries.as_mut_slice());

    for i in 1..entries.len() {
        let (left_key, _) = &entries[i - 1];
        let (right_key, _) = &entries[i];
        if Value::canonical_cmp_key(left_key, right_key) == std::cmp::Ordering::Equal {
            return Value::Map(entries);
        }
    }

    Value::Map(entries)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        VALUE_BINARY_TAG_ENUM, VALUE_BINARY_TAG_ULID, decode_structural_value_storage_binary_bytes,
        encode_structural_value_storage_binary_bytes,
        validate_structural_value_storage_binary_bytes,
    };
    use crate::{
        db::data::structural_field::binary::TAG_LIST,
        types::{Account, Decimal, Float32, Float64, Principal, Subaccount, Timestamp, Ulid},
        value::{Value, ValueEnum},
    };

    #[test]
    fn binary_value_storage_roundtrips_nested_variants() {
        let value = Value::Map(vec![
            (
                Value::Text("account".to_string()),
                Value::Account(Account::new(
                    Principal::from_slice(&[1, 2, 3]),
                    Some([7u8; 32]),
                )),
            ),
            (
                Value::Text("enum".to_string()),
                Value::Enum(
                    ValueEnum::new("Spell", Some("Demo/Spell")).with_payload(Value::List(vec![
                        Value::Decimal(Decimal::from_i128_with_scale(12345, 2)),
                        Value::Timestamp(Timestamp::from_millis(1_710_013_530_123)),
                        Value::Ulid(Ulid::from_u128(77)),
                    ])),
                ),
            ),
            (
                Value::Text("floats".to_string()),
                Value::List(vec![
                    Value::Float32(Float32::try_new(3.5).expect("finite f32")),
                    Value::Float64(Float64::try_new(9.25).expect("finite f64")),
                    Value::Subaccount(Subaccount::from_array([9u8; 32])),
                ]),
            ),
        ]);

        let encoded = encode_structural_value_storage_binary_bytes(&value)
            .expect("binary value bytes should encode");
        let decoded = decode_structural_value_storage_binary_bytes(&encoded)
            .expect("binary value bytes should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn binary_value_storage_uses_local_tags_for_ambiguous_variants() {
        let ulid = Value::Ulid(Ulid::from_u128(99));
        let enum_value = Value::Enum(ValueEnum::loose("Loose"));

        let ulid_bytes = encode_structural_value_storage_binary_bytes(&ulid)
            .expect("ulid value bytes should encode");
        let enum_bytes = encode_structural_value_storage_binary_bytes(&enum_value)
            .expect("enum value bytes should encode");

        assert_eq!(ulid_bytes[0], VALUE_BINARY_TAG_ULID);
        assert_eq!(enum_bytes[0], VALUE_BINARY_TAG_ENUM);
        assert_eq!(enum_bytes[1], TAG_LIST);
    }

    #[test]
    fn binary_value_storage_rejects_trailing_bytes() {
        let mut encoded =
            encode_structural_value_storage_binary_bytes(&Value::Text("alpha".to_string()))
                .expect("binary value bytes should encode");
        encoded.push(0xFF);

        let err = decode_structural_value_storage_binary_bytes(&encoded)
            .expect_err("trailing bytes must be rejected");
        assert!(
            err.to_string().contains("trailing bytes"),
            "expected trailing-byte error, got: {err}",
        );
    }

    #[test]
    fn binary_value_storage_validate_matches_decode() {
        let value = Value::Enum(
            ValueEnum::new("Arc", Some("Spell/Arc")).with_payload(Value::Ulid(Ulid::from_u128(5))),
        );
        let encoded = encode_structural_value_storage_binary_bytes(&value)
            .expect("binary value bytes should encode");

        validate_structural_value_storage_binary_bytes(&encoded)
            .expect("binary value bytes should validate");
    }
}
