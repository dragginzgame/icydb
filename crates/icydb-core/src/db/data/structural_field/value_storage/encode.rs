//! Module: data::structural_field::value_storage::encode
//! Responsibility: structural value-storage byte encoding for runtime `Value` payloads.
//! Does not own: field-kind routing, row encoding, or borrowed decode traversal.
//! Boundary: writes the owner-local value-storage envelope used by `FieldStorageDecode::Value`.

use crate::{
    db::data::structural_field::{
        binary::{
            push_binary_bool, push_binary_bytes, push_binary_int64, push_binary_list_len,
            push_binary_map_len, push_binary_nat64, push_binary_null, push_binary_tag,
            push_binary_text, push_binary_unit,
        },
        typed::{
            encode_account_payload_bytes, encode_date_payload_days, encode_decimal_payload_parts,
            encode_duration_payload_millis, encode_float32_payload_bytes,
            encode_float64_payload_bytes, encode_int128_payload_bytes, encode_nat128_payload_bytes,
            encode_principal_payload_bytes, encode_subaccount_payload_bytes,
            encode_timestamp_payload_millis, encode_ulid_payload_bytes,
        },
        value_storage::tags::{
            VALUE_BINARY_TAG_ACCOUNT, VALUE_BINARY_TAG_DATE, VALUE_BINARY_TAG_DECIMAL,
            VALUE_BINARY_TAG_DURATION, VALUE_BINARY_TAG_ENUM, VALUE_BINARY_TAG_FLOAT32,
            VALUE_BINARY_TAG_FLOAT64, VALUE_BINARY_TAG_INT_BIG, VALUE_BINARY_TAG_INT128,
            VALUE_BINARY_TAG_NAT_BIG, VALUE_BINARY_TAG_NAT128, VALUE_BINARY_TAG_PRINCIPAL,
            VALUE_BINARY_TAG_SUBACCOUNT, VALUE_BINARY_TAG_TIMESTAMP, VALUE_BINARY_TAG_ULID,
        },
    },
    error::InternalError,
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Nat, Principal, Subaccount,
        Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};

/// Encode one persisted `FieldStorageDecode::Value` payload through the
/// owner-local structural value-storage contract.
pub(in crate::db) fn encode_structural_value_storage_bytes(
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_value_storage_binary_into(&mut encoded, value)?;

    Ok(encoded)
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
    push_binary_nat64(&mut encoded, value);

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
pub(in crate::db) fn encode_value_storage_text(value: &str) -> Vec<u8> {
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
    let mut encoded = Vec::new();
    push_account_payload(&mut encoded, value)?;

    Ok(encoded)
}

/// Encode one canonical structural value-storage decimal payload.
pub(in crate::db) fn encode_decimal(value: Decimal) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_decimal_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage int128 payload.
pub(in crate::db) fn encode_int128(value: crate::types::Int128) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_int128_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage nat128 payload.
pub(in crate::db) fn encode_nat128(value: crate::types::Nat128) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_nat128_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage `Value::IntBig` payload.
pub(in crate::db) fn encode_int(value: &Int) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_int_big_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage `Value::NatBig` payload.
pub(in crate::db) fn encode_nat(value: &Nat) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_nat_big_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage float32 payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_float32_bytes(value: Float32) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_float32_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage float64 payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_float64_bytes(value: Float64) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_float64_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage date payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_date_bytes(value: Date) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_date_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage duration payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_duration_bytes(value: Duration) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_duration_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage principal payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_principal_bytes(
    value: Principal,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    push_principal_payload(&mut encoded, value)?;

    Ok(encoded)
}

/// Encode one canonical structural value-storage subaccount payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_subaccount_bytes(
    value: Subaccount,
) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_subaccount_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage timestamp payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_timestamp_bytes(value: Timestamp) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_timestamp_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage ULID payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_ulid_bytes(value: Ulid) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_ulid_payload(&mut encoded, value);

    encoded
}

/// Encode one canonical structural value-storage list payload from already
/// encoded nested value payload slices.
pub(in crate::db) fn encode_value_storage_list_item_slices(items: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, items.len());
    for item in items {
        encoded.extend_from_slice(item);
    }

    encoded
}

/// Encode one canonical structural value-storage list payload from owned nested
/// value payload buffers without staging a second borrowed-slice vector.
pub(in crate::db) fn encode_value_storage_owned_list_items(items: &[Vec<u8>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, items.len());
    for item in items {
        encoded.extend_from_slice(item);
    }

    encoded
}

/// Encode one canonical structural value-storage map payload from already
/// encoded nested key/value payload slices.
pub(in crate::db) fn encode_value_storage_map_entry_slices(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_map_len(&mut encoded, entries.len());
    for (key_bytes, value_bytes) in entries {
        encoded.extend_from_slice(key_bytes);
        encoded.extend_from_slice(value_bytes);
    }

    encoded
}

/// Encode one canonical structural value-storage map payload from owned nested
/// key/value payload buffers without staging a second borrowed-slice vector.
pub(in crate::db) fn encode_value_storage_owned_map_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_map_len(&mut encoded, entries.len());
    for (key_bytes, value_bytes) in entries {
        encoded.extend_from_slice(key_bytes);
        encoded.extend_from_slice(value_bytes);
    }

    encoded
}

/// Encode one canonical enum payload from its variant, optional strict path,
/// and already encoded nested payload bytes without constructing `Value`.
pub(in crate::db) fn encode_enum(
    variant: &str,
    path: Option<&str>,
    payload: Option<&[u8]>,
) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_tag(&mut encoded, VALUE_BINARY_TAG_ENUM);
    push_binary_list_len(&mut encoded, 3);
    push_binary_text(&mut encoded, variant);
    match path {
        Some(path) => push_binary_text(&mut encoded, path),
        None => push_binary_null(&mut encoded),
    }
    match payload {
        Some(payload) => encoded.extend_from_slice(payload),
        None => push_binary_null(&mut encoded),
    }

    encoded
}

// Encode one runtime `Value` into the parallel Structural Binary v1 envelope.
fn encode_value_storage_binary_into(out: &mut Vec<u8>, value: &Value) -> Result<(), InternalError> {
    match value {
        Value::Null => push_binary_null(out),
        Value::Unit => push_binary_unit(out),
        Value::Blob(value) => push_binary_bytes(out, value.as_slice()),
        Value::Bool(value) => push_binary_bool(out, *value),
        Value::Int(value) => push_binary_int64(out, *value),
        Value::Nat(value) => push_binary_nat64(out, *value),
        Value::Text(value) => push_binary_text(out, value),
        Value::List(items) => push_value_binary_list_payload(out, items.as_slice())?,
        Value::Map(entries) => push_value_binary_map_payload(out, entries.as_slice())?,
        Value::Account(value) => push_account_payload(out, *value)?,
        Value::Date(value) => push_date_payload(out, *value),
        Value::Decimal(value) => push_decimal_payload(out, *value),
        Value::Duration(value) => push_duration_payload(out, *value),
        Value::Enum(value) => push_binary_enum_value(out, value)?,
        Value::Float32(value) => push_float32_payload(out, *value),
        Value::Float64(value) => push_float64_payload(out, *value),
        Value::Int128(value) => push_int128_payload(out, *value),
        Value::IntBig(value) => push_int_big_payload(out, value),
        Value::Principal(value) => push_principal_payload(out, *value)?,
        Value::Subaccount(value) => push_subaccount_payload(out, *value),
        Value::Timestamp(value) => push_timestamp_payload(out, *value),
        Value::Nat128(value) => push_nat128_payload(out, *value),
        Value::NatBig(value) => push_nat_big_payload(out, value),
        Value::Ulid(value) => push_ulid_payload(out, *value),
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

// Encode one binary `Value::Account` payload through Account's fixed-size byte
// contract instead of routing through the general `Value` lane.
fn push_account_payload(out: &mut Vec<u8>, value: Account) -> Result<(), InternalError> {
    let bytes = encode_account_payload_bytes(value)?;

    push_binary_tag(out, VALUE_BINARY_TAG_ACCOUNT);
    push_binary_bytes(out, &bytes);

    Ok(())
}

// Encode one binary decimal payload as `(mantissa_bytes, scale)` without
// embedding a generic field-name object model in bytes.
fn push_decimal_payload(out: &mut Vec<u8>, value: Decimal) {
    let (mantissa, scale) = encode_decimal_payload_parts(value);

    push_binary_tag(out, VALUE_BINARY_TAG_DECIMAL);
    push_binary_list_len(out, 2);
    push_binary_bytes(out, &mantissa.to_be_bytes());
    push_binary_nat64(out, u64::from(scale));
}

fn push_date_payload(out: &mut Vec<u8>, value: Date) {
    push_tagged_i64_payload(out, VALUE_BINARY_TAG_DATE, encode_date_payload_days(value));
}

fn push_duration_payload(out: &mut Vec<u8>, value: Duration) {
    push_tagged_u64_payload(
        out,
        VALUE_BINARY_TAG_DURATION,
        encode_duration_payload_millis(value),
    );
}

fn push_float32_payload(out: &mut Vec<u8>, value: Float32) {
    push_tagged_binary_payload(
        out,
        VALUE_BINARY_TAG_FLOAT32,
        &encode_float32_payload_bytes(value),
    );
}

fn push_float64_payload(out: &mut Vec<u8>, value: Float64) {
    push_tagged_binary_payload(
        out,
        VALUE_BINARY_TAG_FLOAT64,
        &encode_float64_payload_bytes(value),
    );
}

fn push_int128_payload(out: &mut Vec<u8>, value: crate::types::Int128) {
    push_tagged_binary_payload(
        out,
        VALUE_BINARY_TAG_INT128,
        &encode_int128_payload_bytes(value),
    );
}

fn push_nat128_payload(out: &mut Vec<u8>, value: crate::types::Nat128) {
    push_tagged_binary_payload(
        out,
        VALUE_BINARY_TAG_NAT128,
        &encode_nat128_payload_bytes(value),
    );
}

fn push_principal_payload(out: &mut Vec<u8>, value: Principal) -> Result<(), InternalError> {
    let bytes = encode_principal_payload_bytes(value)?;
    push_tagged_binary_payload(out, VALUE_BINARY_TAG_PRINCIPAL, bytes.as_slice());

    Ok(())
}

fn push_subaccount_payload(out: &mut Vec<u8>, value: Subaccount) {
    push_tagged_binary_payload(
        out,
        VALUE_BINARY_TAG_SUBACCOUNT,
        &encode_subaccount_payload_bytes(value),
    );
}

fn push_timestamp_payload(out: &mut Vec<u8>, value: Timestamp) {
    push_tagged_i64_payload(
        out,
        VALUE_BINARY_TAG_TIMESTAMP,
        encode_timestamp_payload_millis(value),
    );
}

fn push_ulid_payload(out: &mut Vec<u8>, value: Ulid) {
    push_tagged_binary_payload(
        out,
        VALUE_BINARY_TAG_ULID,
        &encode_ulid_payload_bytes(value),
    );
}

fn push_tagged_binary_payload(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    push_binary_tag(out, tag);
    push_binary_bytes(out, payload);
}

fn push_tagged_i64_payload(out: &mut Vec<u8>, tag: u8, value: i64) {
    push_binary_tag(out, tag);
    push_binary_int64(out, value);
}

fn push_tagged_u64_payload(out: &mut Vec<u8>, tag: u8, value: u64) {
    push_binary_tag(out, tag);
    push_binary_nat64(out, value);
}

// Encode one binary `Value::Enum` payload using a fixed positional tuple:
// `(variant, path, payload)`.
fn push_binary_enum_value(out: &mut Vec<u8>, value: &ValueEnum) -> Result<(), InternalError> {
    push_binary_tag(out, VALUE_BINARY_TAG_ENUM);
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
}

// Encode one binary `Value::IntBig` payload as `(sign, limbs)`.
fn push_int_big_payload(out: &mut Vec<u8>, value: &Int) {
    let (is_negative, digits) = value.sign_and_u32_digits();

    push_binary_tag(out, VALUE_BINARY_TAG_INT_BIG);
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
}

// Encode one binary `Value::NatBig` payload as a limb sequence.
fn push_nat_big_payload(out: &mut Vec<u8>, value: &Nat) {
    let digits = value.u32_digits();

    push_binary_tag(out, VALUE_BINARY_TAG_NAT_BIG);
    push_binary_u32_digit_list(out, digits.as_slice());
}

// Encode one canonical big-integer limb sequence.
fn push_binary_u32_digit_list(out: &mut Vec<u8>, digits: &[u32]) {
    push_binary_list_len(out, digits.len());
    for digit in digits {
        push_binary_nat64(out, u64::from(*digit));
    }
}
