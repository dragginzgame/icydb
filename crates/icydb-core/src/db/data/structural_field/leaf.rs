//! Module: data::structural_field::leaf
//! Responsibility: typed wrapper and structured leaf decoding that still has fixed payload semantics.
//! Does not own: scalar primitive fast paths, composite recursion, or `Value` storage envelopes.
//! Boundary: sibling modules use this file for leaf contracts like decimal, duration, bigint, and date.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        TAG_BYTES, TAG_INT64, TAG_LIST, TAG_NULL, TAG_UINT64, parse_binary_head,
        payload_bytes as binary_payload_bytes, push_binary_bytes, push_binary_int64,
        push_binary_list_len, push_binary_null, push_binary_uint64, skip_binary_value,
    },
    primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
    storage_key::{decode_storage_key_binary_value_bytes, encode_storage_key_binary_value_bytes},
    typed::{
        decode_date_payload_days, decode_decimal_payload_parts, decode_duration_payload_millis,
        encode_date_payload_days, encode_decimal_payload_parts, encode_duration_payload_millis,
    },
};
use crate::{
    error::InternalError,
    model::field::FieldKind,
    types::{Date, Decimal, Duration, Int, Nat},
    value::Value,
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};

/// Decode one non-recursive leaf `ByKind` field payload through the canonical
/// Structural Binary v1 leaf lane.
pub(super) fn decode_leaf_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    let value = match kind {
        FieldKind::Account
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Timestamp
        | FieldKind::Unit => decode_storage_key_binary_value_bytes(raw_bytes, kind)?
            .expect("storage-key-owned leaf kinds must return a value"),
        FieldKind::Date => decode_date_value_bytes(raw_bytes)?,
        FieldKind::Decimal { .. } => decode_decimal_value_bytes(raw_bytes)?,
        FieldKind::Duration => decode_duration_value_bytes(raw_bytes)?,
        FieldKind::IntBig => decode_int_big_value_bytes(raw_bytes)?,
        FieldKind::Structured { .. } => decode_structured_leaf_null_value_bytes(raw_bytes)?,
        FieldKind::UintBig => decode_uint_big_value_bytes(raw_bytes)?,
        FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::Text { .. }
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::Ulid => {
            return Err(FieldDecodeError::new(
                "scalar field unexpectedly bypassed byte-level fast path",
            ));
        }
        FieldKind::Enum { .. }
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Relation { .. }
        | FieldKind::Set(_) => return Ok(None),
    };

    Ok(Some(value))
}

/// Encode one non-recursive leaf `ByKind` field payload through the canonical
/// Structural Binary v1 leaf lane.
pub(super) fn encode_leaf_field_binary_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<u8>>, InternalError> {
    let encoded = match kind {
        FieldKind::Account
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Timestamp
        | FieldKind::Unit => encode_storage_key_binary_value_bytes(kind, value, field_name)?,
        FieldKind::Date => Some(encode_date_value_bytes(value, field_name)?),
        FieldKind::Decimal { .. } => Some(encode_decimal_value_bytes(value, field_name)?),
        FieldKind::Duration => Some(encode_duration_value_bytes(value, field_name)?),
        FieldKind::IntBig => Some(encode_int_big_value_bytes(value, field_name)?),
        FieldKind::Structured { .. } => Some(encode_structured_leaf_null_bytes(value, field_name)?),
        FieldKind::UintBig => Some(encode_uint_big_value_bytes(value, field_name)?),
        FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::Text { .. }
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::Ulid
        | FieldKind::Enum { .. }
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Relation { .. }
        | FieldKind::Set(_) => None,
    };

    Ok(encoded)
}

// Decode the only supported structured leaf `ByKind` case: explicit null.
fn decode_structured_leaf_null_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_required_null_payload(raw_bytes, "structured")?;

    Ok(Value::Null)
}

// Encode the only supported structured leaf `ByKind` case: explicit null.
fn encode_structured_leaf_null_bytes(
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let Value::Null = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            "structured ByKind field encoding is unsupported",
        ));
    };

    let mut encoded = Vec::new();
    push_binary_null(&mut encoded);

    Ok(encoded)
}

// Decode one date payload from its canonical signed day-count form.
fn decode_date_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_date_payload_days(decode_required_i64_payload(raw_bytes, "date days")?).map(Value::Date)
}

// Decode one decimal payload from the canonical `(mantissa_bytes, scale)`
// tuple.
fn decode_decimal_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let items = split_binary_tuple_items(raw_bytes, 2, "decimal")?;
    let mantissa_bytes: [u8; 16] = decode_required_bytes_payload(items[0], "decimal mantissa")?
        .try_into()
        .map_err(|_| {
            FieldDecodeError::new(
                "structural binary: invalid decimal mantissa length: 16 bytes expected",
            )
        })?;
    let scale = decode_required_u32_payload(items[1], "decimal scale")?;

    Ok(Value::Decimal(decode_decimal_payload_parts(
        i128::from_be_bytes(mantissa_bytes),
        scale,
    )?))
}

// Decode one duration payload from its canonical millis form.
fn decode_duration_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    Ok(Value::Duration(decode_duration_payload_millis(
        decode_required_u64_payload(raw_bytes, "duration millis")?,
    )))
}

// Decode one arbitrary-precision signed integer payload from the canonical
// `(sign, limbs)` tuple.
fn decode_int_big_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let items = split_binary_tuple_items(raw_bytes, 2, "bigint")?;
    let sign = decode_bigint_sign_payload(items[0])?;
    let magnitude = decode_biguint_payload(items[1])?;
    let wrapped = WrappedInt::from(BigInt::from_biguint(sign, magnitude));

    Ok(Value::IntBig(Int::from(wrapped)))
}

// Decode one arbitrary-precision unsigned integer payload from the canonical
// limb sequence.
fn decode_uint_big_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let wrapped = WrappedNat::from(decode_biguint_payload(raw_bytes)?);

    Ok(Value::UintBig(Nat::from(wrapped)))
}

// Encode one date payload into canonical signed day-count form.
fn encode_date_value_bytes(value: &Value, field_name: &str) -> Result<Vec<u8>, InternalError> {
    let Value::Date(value) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind Date does not accept runtime value {value:?}"),
        ));
    };

    let mut encoded = Vec::new();
    push_binary_int64(&mut encoded, encode_date_payload_days(*value));
    Ok(encoded)
}

// Encode one decimal payload into the canonical `(mantissa_bytes, scale)`
// tuple.
fn encode_decimal_value_bytes(value: &Value, field_name: &str) -> Result<Vec<u8>, InternalError> {
    let Value::Decimal(value) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind Decimal does not accept runtime value {value:?}"),
        ));
    };

    let (mantissa, scale) = encode_decimal_payload_parts(*value);
    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, 2);
    push_binary_bytes(&mut encoded, &mantissa.to_be_bytes());
    push_binary_uint64(&mut encoded, u64::from(scale));

    Ok(encoded)
}

// Encode one duration payload into canonical millis.
fn encode_duration_value_bytes(value: &Value, field_name: &str) -> Result<Vec<u8>, InternalError> {
    let Value::Duration(value) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind Duration does not accept runtime value {value:?}"),
        ));
    };

    let mut encoded = Vec::new();
    push_binary_uint64(&mut encoded, encode_duration_payload_millis(*value));
    Ok(encoded)
}

// Encode one arbitrary-precision signed integer payload as `(sign, limbs)`.
fn encode_int_big_value_bytes(value: &Value, field_name: &str) -> Result<Vec<u8>, InternalError> {
    let Value::IntBig(value) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind IntBig does not accept runtime value {value:?}"),
        ));
    };

    let (is_negative, digits) = value.sign_and_u32_digits();
    let sign = if digits.is_empty() {
        0
    } else if is_negative {
        -1
    } else {
        1
    };

    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, 2);
    push_binary_int64(&mut encoded, sign);
    push_binary_u32_digit_list(&mut encoded, digits.as_slice());

    Ok(encoded)
}

// Encode one arbitrary-precision unsigned integer payload as a canonical limb
// sequence.
fn encode_uint_big_value_bytes(value: &Value, field_name: &str) -> Result<Vec<u8>, InternalError> {
    let Value::UintBig(value) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind UintBig does not accept runtime value {value:?}"),
        ));
    };

    let mut encoded = Vec::new();
    push_binary_u32_digit_list(&mut encoded, value.u32_digits().as_slice());

    Ok(encoded)
}

// Emit one canonical biguint limb sequence.
fn push_binary_u32_digit_list(out: &mut Vec<u8>, digits: &[u32]) {
    push_binary_list_len(out, digits.len());
    for digit in digits {
        push_binary_uint64(out, u64::from(*digit));
    }
}

// Decode one bigint sign payload serialized as -1, 0, or 1.
fn decode_bigint_sign_payload(raw_bytes: &[u8]) -> Result<BigIntSign, FieldDecodeError> {
    match decode_required_i64_payload(raw_bytes, "bigint sign")? {
        -1 => Ok(BigIntSign::Minus),
        0 => Ok(BigIntSign::NoSign),
        1 => Ok(BigIntSign::Plus),
        other => Err(FieldDecodeError::new(format!(
            "structural binary: invalid bigint sign {other}"
        ))),
    }
}

// Decode one biguint payload serialized as a canonical sequence of base-2^32
// limbs.
fn decode_biguint_payload(raw_bytes: &[u8]) -> Result<BigUint, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated biguint payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(
            "structural binary: expected biguint limb sequence",
        ));
    }

    let mut cursor = payload_start;
    let mut limbs = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let limb_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        limbs.push(decode_required_u32_payload(
            &raw_bytes[limb_start..cursor],
            "biguint limb",
        )?);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after biguint payload",
        ));
    }

    Ok(BigUint::new(limbs))
}

// Decode one required top-level `null` payload and enforce full-byte
// consumption.
fn decode_required_null_payload(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<(), FieldDecodeError> {
    let Some((tag, _, _)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label} payload"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_NULL {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected null for {label}"
        )));
    }

    Ok(())
}

// Decode one required top-level byte-string payload and enforce full-byte
// consumption.
fn decode_required_bytes_payload<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label} payload"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_BYTES {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected bytes for {label}"
        )));
    }

    binary_payload_bytes(raw_bytes, len, payload_start, label)
}

// Decode one required top-level `u32` payload and enforce full-byte
// consumption.
fn decode_required_u32_payload(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<u32, FieldDecodeError> {
    u32::try_from(decode_required_u64_payload(raw_bytes, label)?)
        .map_err(|_| FieldDecodeError::new(format!("structural binary: {label} out of u32 range")))
}

// Decode one required top-level `u64` payload and enforce full-byte
// consumption.
fn decode_required_u64_payload(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<u64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label} payload"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_UINT64 || len != 8 {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected u64 for {label}"
        )));
    }

    decode_u64_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, label)?,
        label,
    )
}

// Decode one required top-level `i64` payload and enforce full-byte
// consumption.
fn decode_required_i64_payload(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<i64, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label} payload"
        )));
    };
    let end = skip_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() || tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected i64 for {label}"
        )));
    }

    decode_i64_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, label)?,
        label,
    )
}

// Split one fixed-length binary tuple into self-contained item slices.
fn split_binary_tuple_items<'a>(
    raw_bytes: &'a [u8],
    expected_len: u32,
    label: &'static str,
) -> Result<Vec<&'a [u8]>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "structural binary: truncated {label} payload"
        )));
    };
    if tag != TAG_LIST || len != expected_len {
        return Err(FieldDecodeError::new(format!(
            "structural binary: expected {label} tuple of length {expected_len}"
        )));
    }

    let mut items = Vec::with_capacity(expected_len as usize);
    let mut cursor = payload_start;
    for _ in 0..expected_len {
        let item_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        items.push(&raw_bytes[item_start..cursor]);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "structural binary: trailing bytes after {label} payload"
        )));
    }

    Ok(items)
}

/// Encode one direct date leaf through the canonical structural leaf lane.
pub(super) fn encode_date_field_by_kind_bytes(
    value: Date,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Date) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept date"),
        ));
    }

    encode_date_value_bytes(&Value::Date(value), field_name)
}

/// Decode one direct date leaf through the canonical structural leaf lane.
pub(super) fn decode_date_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Date>, FieldDecodeError> {
    if !matches!(kind, FieldKind::Date) {
        return Err(FieldDecodeError::new(
            "field kind is not owned by the structural date leaf lane",
        ));
    }

    match decode_date_value_bytes(raw_bytes)? {
        Value::Date(value) => Ok(Some(value)),
        _ => Err(FieldDecodeError::new(
            "structural date leaf unexpectedly decoded as non-date value",
        )),
    }
}

/// Encode one direct decimal leaf through the canonical structural leaf lane.
pub(super) fn encode_decimal_field_by_kind_bytes(
    value: Decimal,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Decimal { .. }) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept decimal"),
        ));
    }

    encode_decimal_value_bytes(&Value::Decimal(value), field_name)
}

/// Decode one direct decimal leaf through the canonical structural leaf lane.
pub(super) fn decode_decimal_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Decimal>, FieldDecodeError> {
    if !matches!(kind, FieldKind::Decimal { .. }) {
        return Err(FieldDecodeError::new(
            "field kind is not owned by the structural decimal leaf lane",
        ));
    }

    match decode_decimal_value_bytes(raw_bytes)? {
        Value::Decimal(value) => Ok(Some(value)),
        _ => Err(FieldDecodeError::new(
            "structural decimal leaf unexpectedly decoded as non-decimal value",
        )),
    }
}

/// Encode one direct duration leaf through the canonical structural leaf lane.
pub(super) fn encode_duration_field_by_kind_bytes(
    value: Duration,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Duration) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept duration"),
        ));
    }

    encode_duration_value_bytes(&Value::Duration(value), field_name)
}

/// Decode one direct duration leaf through the canonical structural leaf lane.
pub(super) fn decode_duration_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Duration>, FieldDecodeError> {
    if !matches!(kind, FieldKind::Duration) {
        return Err(FieldDecodeError::new(
            "field kind is not owned by the structural duration leaf lane",
        ));
    }

    match decode_duration_value_bytes(raw_bytes)? {
        Value::Duration(value) => Ok(Some(value)),
        _ => Err(FieldDecodeError::new(
            "structural duration leaf unexpectedly decoded as non-duration value",
        )),
    }
}

/// Encode one direct signed-bigint leaf through the canonical structural leaf
/// lane.
pub(super) fn encode_int_big_field_by_kind_bytes(
    value: &Int,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::IntBig) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept bigint"),
        ));
    }

    encode_int_big_value_bytes(&Value::IntBig(value.clone()), field_name)
}

/// Decode one direct signed-bigint leaf through the canonical structural leaf
/// lane.
pub(super) fn decode_int_big_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Int>, FieldDecodeError> {
    if !matches!(kind, FieldKind::IntBig) {
        return Err(FieldDecodeError::new(
            "field kind is not owned by the structural bigint leaf lane",
        ));
    }

    match decode_int_big_value_bytes(raw_bytes)? {
        Value::IntBig(value) => Ok(Some(value)),
        _ => Err(FieldDecodeError::new(
            "structural bigint leaf unexpectedly decoded as non-bigint value",
        )),
    }
}

/// Encode one direct unsigned-bigint leaf through the canonical structural
/// leaf lane.
pub(super) fn encode_uint_big_field_by_kind_bytes(
    value: &Nat,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::UintBig) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept biguint"),
        ));
    }

    encode_uint_big_value_bytes(&Value::UintBig(value.clone()), field_name)
}

/// Decode one direct unsigned-bigint leaf through the canonical structural
/// leaf lane.
pub(super) fn decode_uint_big_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Nat>, FieldDecodeError> {
    if !matches!(kind, FieldKind::UintBig) {
        return Err(FieldDecodeError::new(
            "field kind is not owned by the structural biguint leaf lane",
        ));
    }

    match decode_uint_big_value_bytes(raw_bytes)? {
        Value::UintBig(value) => Ok(Some(value)),
        _ => Err(FieldDecodeError::new(
            "structural biguint leaf unexpectedly decoded as non-biguint value",
        )),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        TAG_NULL, decode_leaf_field_by_kind_bytes, encode_leaf_field_binary_bytes,
        push_binary_bytes, push_binary_int64, push_binary_list_len, push_binary_null,
        push_binary_uint64,
    };
    use crate::{
        db::data::structural_field::{
            binary::push_binary_text, validate_structural_field_by_kind_bytes,
        },
        model::field::FieldKind,
        types::{Date, Decimal, Duration, Int, Nat},
        value::Value,
    };
    use candid::{Int as WrappedInt, Nat as WrappedNat};

    #[test]
    fn leaf_field_binary_roundtrips_supported_leaf_wrappers() {
        let cases = vec![
            (
                FieldKind::Date,
                Value::Date(Date::new_checked(2025, 10, 19).expect("valid date")),
            ),
            (
                FieldKind::Decimal { scale: 2 },
                Value::Decimal(Decimal::from_i128_with_scale(12_345, 2)),
            ),
            (FieldKind::Duration, Value::Duration(Duration::from_secs(5))),
            (
                FieldKind::IntBig,
                Value::IntBig(Int::from(WrappedInt::from(123_456_789_i64))),
            ),
            (
                FieldKind::UintBig,
                Value::UintBig(Nat::from(WrappedNat::from(987_654_321_u64))),
            ),
            (FieldKind::Structured { queryable: false }, Value::Null),
        ];

        for (kind, value) in cases {
            let encoded = encode_leaf_field_binary_bytes(kind, &value, "field")
                .expect("leaf payload should encode")
                .expect("leaf kind should be owned by the leaf lane");
            let decoded = decode_leaf_field_by_kind_bytes(encoded.as_slice(), kind)
                .expect("leaf payload should decode")
                .expect("leaf kind should decode through the leaf lane");

            validate_structural_field_by_kind_bytes(encoded.as_slice(), kind)
                .expect("leaf payload should validate");

            assert_eq!(decoded, value, "leaf roundtrip mismatch for {kind:?}");
        }
    }

    #[test]
    fn leaf_field_binary_rejects_malformed_decimal_payload() {
        let mut bytes = Vec::new();
        push_binary_list_len(&mut bytes, 2);
        push_binary_bytes(&mut bytes, &1_i128.to_be_bytes());
        push_binary_uint64(&mut bytes, u64::from(Decimal::max_supported_scale() + 1));

        let kind = FieldKind::Decimal { scale: 2 };

        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), kind);
        let validate = validate_structural_field_by_kind_bytes(bytes.as_slice(), kind);

        assert!(
            decode.is_err(),
            "malformed decimal payload must fail decode"
        );
        assert!(
            validate.is_err(),
            "malformed decimal payload must fail validate"
        );
    }

    #[test]
    fn leaf_field_binary_rejects_invalid_bigint_sign() {
        let mut bytes = Vec::new();
        push_binary_list_len(&mut bytes, 2);
        push_binary_int64(&mut bytes, 2);
        push_binary_list_len(&mut bytes, 0);

        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), FieldKind::IntBig);
        let validate = validate_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::IntBig);

        assert!(decode.is_err(), "invalid bigint sign must fail decode");
        assert!(validate.is_err(), "invalid bigint sign must fail validate");
    }

    #[test]
    fn leaf_field_binary_rejects_non_list_biguint_payload() {
        let mut bytes = Vec::new();
        push_binary_text(&mut bytes, "not-a-limb-list");

        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), FieldKind::UintBig);
        let validate =
            validate_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::UintBig);

        assert!(decode.is_err(), "non-list biguint payload must fail decode");
        assert!(
            validate.is_err(),
            "non-list biguint payload must fail validate"
        );
    }

    #[test]
    fn leaf_field_binary_rejects_structured_non_null_payload() {
        let mut bytes = Vec::new();
        push_binary_null(&mut bytes);
        bytes.push(TAG_NULL);

        let kind = FieldKind::Structured { queryable: false };
        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), kind);
        let validate = validate_structural_field_by_kind_bytes(bytes.as_slice(), kind);

        assert!(
            decode.is_err(),
            "structured leaf trailing bytes must fail decode"
        );
        assert!(
            validate.is_err(),
            "structured leaf trailing bytes must fail validate"
        );
    }
}
