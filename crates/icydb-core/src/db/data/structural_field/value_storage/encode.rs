//! Module: data::structural_field::value_storage::encode
//! Responsibility: scalar payload appends for canonical structural value storage.
//! Does not own: field-kind routing, row encoding, or borrowed decode traversal.
//! Boundary: writes the owner-local value-storage envelope used by `FieldStorageDecode::CatalogValue`.

use crate::{
    db::data::structural_field::{
        binary::{
            push_binary_bytes, push_binary_int_big_payload, push_binary_nat_big_payload,
            push_binary_null, push_binary_tag,
        },
        typed::{
            encode_account_payload_bytes, encode_decimal_payload_bytes,
            encode_duration_payload_millis, encode_float32_payload_bytes,
            encode_float64_payload_bytes, encode_int128_payload_bytes, encode_nat128_payload_bytes,
            encode_principal_payload_bytes, encode_subaccount_payload_bytes,
            encode_timestamp_payload_millis, encode_ulid_payload_bytes,
        },
        value_storage::tags::{
            VALUE_BINARY_TAG_ACCOUNT, VALUE_BINARY_TAG_DATE, VALUE_BINARY_TAG_DECIMAL,
            VALUE_BINARY_TAG_DURATION, VALUE_BINARY_TAG_FLOAT32, VALUE_BINARY_TAG_FLOAT64,
            VALUE_BINARY_TAG_INT_BIG, VALUE_BINARY_TAG_INT128, VALUE_BINARY_TAG_NAT_BIG,
            VALUE_BINARY_TAG_NAT128, VALUE_BINARY_TAG_PRINCIPAL, VALUE_BINARY_TAG_SUBACCOUNT,
            VALUE_BINARY_TAG_TIMESTAMP, VALUE_BINARY_TAG_U256, VALUE_BINARY_TAG_ULID,
        },
    },
    error::InternalError,
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, U256, Ulid,
    },
};

/// Encode one canonical structural value-storage `NULL` payload without
/// constructing a runtime `Value` at the call site.
pub(in crate::db) fn encode_structural_value_storage_null_bytes() -> Vec<u8> {
    let mut encoded = Vec::new();
    push_binary_null(&mut encoded);

    encoded
}

// Encode one binary `Value::Account` payload through Account's fixed-size byte
// contract instead of routing through the general `Value` lane.
pub(super) fn push_account_payload(out: &mut Vec<u8>, value: Account) -> Result<(), InternalError> {
    let bytes = encode_account_payload_bytes(value)?;

    push_fixed_payload(out, VALUE_BINARY_TAG_ACCOUNT, &bytes);

    Ok(())
}

// Decimal shares its raw mantissa/scale payload with ordinary structural fields.
pub(super) fn push_decimal_payload(out: &mut Vec<u8>, value: Decimal) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_DECIMAL,
        &encode_decimal_payload_bytes(value),
    );
}

pub(super) fn push_date_payload(out: &mut Vec<u8>, value: Date) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_DATE,
        &value.as_days_since_epoch().to_be_bytes(),
    );
}

pub(super) fn push_duration_payload(out: &mut Vec<u8>, value: Duration) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_DURATION,
        &encode_duration_payload_millis(value).to_be_bytes(),
    );
}

pub(super) fn push_float32_payload(out: &mut Vec<u8>, value: Float32) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_FLOAT32,
        &encode_float32_payload_bytes(value),
    );
}

pub(super) fn push_float64_payload(out: &mut Vec<u8>, value: Float64) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_FLOAT64,
        &encode_float64_payload_bytes(value),
    );
}

pub(super) fn push_int128_payload(out: &mut Vec<u8>, value: i128) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_INT128,
        &encode_int128_payload_bytes(value),
    );
}

pub(super) fn push_nat128_payload(out: &mut Vec<u8>, value: u128) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_NAT128,
        &encode_nat128_payload_bytes(value),
    );
}

pub(super) fn push_principal_payload(
    out: &mut Vec<u8>,
    value: Principal,
) -> Result<(), InternalError> {
    let bytes = encode_principal_payload_bytes(&value)?;
    push_binary_tag(out, VALUE_BINARY_TAG_PRINCIPAL);
    push_binary_bytes(out, bytes);

    Ok(())
}

pub(super) fn push_subaccount_payload(out: &mut Vec<u8>, value: Subaccount) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_SUBACCOUNT,
        &encode_subaccount_payload_bytes(value),
    );
}

pub(super) fn push_timestamp_payload(out: &mut Vec<u8>, value: Timestamp) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_TIMESTAMP,
        &encode_timestamp_payload_millis(value).to_be_bytes(),
    );
}

pub(super) fn push_ulid_payload(out: &mut Vec<u8>, value: Ulid) {
    push_fixed_payload(
        out,
        VALUE_BINARY_TAG_ULID,
        &encode_ulid_payload_bytes(value),
    );
}

fn push_fixed_payload(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    push_binary_tag(out, tag);
    out.extend_from_slice(payload);
}

// Encode one binary `Value::IntBig` sign and minimal magnitude.
pub(super) fn push_int_big_payload(out: &mut Vec<u8>, value: &IntBig) {
    let (is_negative, digits) = value.sign_and_u32_digits();

    push_binary_tag(out, VALUE_BINARY_TAG_INT_BIG);
    push_binary_int_big_payload(out, is_negative, digits);
}

// Encode one binary `Value::NatBig` minimal magnitude.
pub(super) fn push_nat_big_payload(out: &mut Vec<u8>, value: &NatBig) {
    let digits = value.u32_digits();

    push_binary_tag(out, VALUE_BINARY_TAG_NAT_BIG);
    push_binary_nat_big_payload(out, digits);
}

pub(super) fn push_u256_payload(out: &mut Vec<u8>, value: U256) {
    push_fixed_payload(out, VALUE_BINARY_TAG_U256, &value.to_be_bytes());
}
