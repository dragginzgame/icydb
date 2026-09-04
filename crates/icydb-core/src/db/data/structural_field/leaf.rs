//! Module: data::structural_field::leaf
//! Responsibility: typed wrapper and structured leaf decoding that still has fixed payload semantics.
//! Does not own: scalar primitive fast paths, composite recursion, or `Value` storage envelopes.
//! Boundary: sibling modules use this file for leaf contracts like decimal, duration, `int_big`, `nat_big`, and date.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        TAG_BYTES, TAG_INT64, TAG_LIST, TAG_NAT64, TAG_NULL, parse_binary_head,
        parse_complete_binary_value, payload_bytes as binary_payload_bytes, push_binary_bytes,
        push_binary_int64, push_binary_list_len, push_binary_nat64, push_binary_null,
        skip_binary_value,
    },
    primary_key_component::{
        decode_primary_key_component_binary_value_bytes,
        encode_primary_key_component_binary_value_bytes,
    },
    primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
    typed::{
        decimal_payload_mantissa_and_scale, decode_date_payload_days,
        decode_decimal_payload_mantissa_and_scale, decode_duration_payload_millis,
        encode_date_payload_days, encode_duration_payload_millis,
    },
};
use crate::{
    db::schema::AcceptedFieldKind,
    error::InternalError,
    types::{IntBig, NatBig},
    value::Value,
};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};

/// Decode one non-recursive leaf `ByKind` field payload through the canonical
/// Structural Binary v1 leaf lane.
pub(super) fn decode_leaf_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    let value = match kind {
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => {
            let Some(value) = decode_primary_key_component_binary_value_bytes(raw_bytes, kind)?
            else {
                return Err(FieldDecodeError::new());
            };
            value
        }
        AcceptedFieldKind::Date => decode_date_value_bytes(raw_bytes)?,
        AcceptedFieldKind::Decimal { .. } => decode_decimal_value_bytes(raw_bytes)?,
        AcceptedFieldKind::Duration => decode_duration_value_bytes(raw_bytes)?,
        AcceptedFieldKind::IntBig { max_bytes } => {
            decode_int_big_value_bytes(raw_bytes, *max_bytes)?
        }
        AcceptedFieldKind::Composite { .. } => decode_structured_leaf_null_value_bytes(raw_bytes)?,
        AcceptedFieldKind::NatBig { max_bytes } => {
            decode_nat_big_value_bytes(raw_bytes, *max_bytes)?
        }
        AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::Ulid => {
            return Err(FieldDecodeError::new());
        }
        AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::Set(_) => return Ok(None),
    };

    Ok(Some(value))
}

/// Encode one non-recursive leaf `ByKind` field payload through the canonical
/// Structural Binary v1 leaf lane.
pub(super) fn encode_leaf_field_binary_bytes(
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<u8>>, InternalError> {
    let encoded = match kind {
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => {
            encode_primary_key_component_binary_value_bytes(kind, value, field_name)?
        }
        AcceptedFieldKind::Date => Some(encode_date_value_bytes(value, field_name)?),
        AcceptedFieldKind::Decimal { .. } => Some(encode_decimal_value_bytes(value, field_name)?),
        AcceptedFieldKind::Duration => Some(encode_duration_value_bytes(value, field_name)?),
        AcceptedFieldKind::IntBig { max_bytes } => {
            Some(encode_int_big_value_bytes(value, *max_bytes, field_name)?)
        }
        AcceptedFieldKind::Composite { .. } => {
            Some(encode_structured_leaf_null_bytes(value, field_name)?)
        }
        AcceptedFieldKind::NatBig { max_bytes } => {
            Some(encode_nat_big_value_bytes(value, *max_bytes, field_name)?)
        }
        AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::Set(_) => None,
    };

    Ok(encoded)
}

// Decode the only supported structured leaf `ByKind` case: explicit null.
fn decode_structured_leaf_null_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_required_null_payload(raw_bytes)?;

    Ok(Value::Null)
}

// Encode the only supported structured leaf `ByKind` case: explicit null.
fn encode_structured_leaf_null_bytes(
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let Value::Null = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    let mut encoded = Vec::new();
    push_binary_null(&mut encoded);

    Ok(encoded)
}

// Decode one date payload from its canonical signed day-count form.
fn decode_date_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_date_payload_days(decode_required_i64_payload(raw_bytes)?).map(Value::Date)
}

// Decode one decimal payload from the canonical `(mantissa_bytes, scale)`
// tuple.
fn decode_decimal_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let items = split_binary_tuple_items(raw_bytes, 2)?;
    let mantissa_bytes: [u8; 16] = decode_required_bytes_payload(items[0])?
        .try_into()
        .map_err(|_| FieldDecodeError::new())?;
    let scale = decode_required_u32_payload(items[1])?;

    Ok(Value::Decimal(decode_decimal_payload_mantissa_and_scale(
        i128::from_be_bytes(mantissa_bytes),
        scale,
    )?))
}

// Decode one duration payload from its canonical millis form.
fn decode_duration_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    Ok(Value::Duration(decode_duration_payload_millis(
        decode_required_u64_payload(raw_bytes)?,
    )))
}

// Decode one bounded signed big-integer payload from the canonical `(sign,
// limbs)` tuple used by `int_big`.
fn decode_int_big_value_bytes(raw_bytes: &[u8], max_bytes: u32) -> Result<Value, FieldDecodeError> {
    let items = split_binary_tuple_items(raw_bytes, 2)?;
    let sign = decode_big_integer_sign_payload(items[0])?;
    let magnitude = decode_big_integer_magnitude_payload(items[1])?;
    let value = IntBig::from_bigint(BigInt::from_biguint(sign, magnitude));
    ensure_int_big_max_bytes(&value, max_bytes)?;

    Ok(Value::IntBig(value))
}

// Decode one bounded unsigned big-integer payload from the canonical limb
// sequence used by `nat_big`.
fn decode_nat_big_value_bytes(raw_bytes: &[u8], max_bytes: u32) -> Result<Value, FieldDecodeError> {
    let value = NatBig::from_biguint(decode_big_integer_magnitude_payload(raw_bytes)?);
    ensure_nat_big_max_bytes(&value, max_bytes)?;

    Ok(Value::NatBig(value))
}

// Encode one date payload into canonical signed day-count form.
fn encode_date_value_bytes(value: &Value, field_name: &str) -> Result<Vec<u8>, InternalError> {
    let Value::Date(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
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
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    let (mantissa, scale) = decimal_payload_mantissa_and_scale(*value);
    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, 2);
    push_binary_bytes(&mut encoded, &mantissa.to_be_bytes());
    push_binary_nat64(&mut encoded, u64::from(scale));

    Ok(encoded)
}

// Encode one duration payload into canonical millis.
fn encode_duration_value_bytes(value: &Value, field_name: &str) -> Result<Vec<u8>, InternalError> {
    let Value::Duration(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    let mut encoded = Vec::new();
    push_binary_nat64(&mut encoded, encode_duration_payload_millis(*value));
    Ok(encoded)
}

// Encode one bounded signed big-integer payload as `(sign, limbs)`.
fn encode_int_big_value_bytes(
    value: &Value,
    max_bytes: u32,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let Value::IntBig(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };
    ensure_int_big_max_bytes(value, max_bytes)
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field_name))?;

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

// Encode one bounded unsigned big-integer payload as a canonical limb sequence.
fn encode_nat_big_value_bytes(
    value: &Value,
    max_bytes: u32,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let Value::NatBig(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };
    ensure_nat_big_max_bytes(value, max_bytes)
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field_name))?;

    let mut encoded = Vec::new();
    push_binary_u32_digit_list(&mut encoded, value.u32_digits().as_slice());

    Ok(encoded)
}

fn ensure_int_big_max_bytes(value: &IntBig, max_bytes: u32) -> Result<(), FieldDecodeError> {
    let len = value.to_leb128().len();
    if len > max_bytes as usize {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

fn ensure_nat_big_max_bytes(value: &NatBig, max_bytes: u32) -> Result<(), FieldDecodeError> {
    let len = value.to_leb128().len();
    if len > max_bytes as usize {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

// Emit one canonical big-integer magnitude limb sequence.
fn push_binary_u32_digit_list(out: &mut Vec<u8>, digits: &[u32]) {
    push_binary_list_len(out, digits.len());
    for digit in digits {
        push_binary_nat64(out, u64::from(*digit));
    }
}

// Decode one `int_big` sign payload serialized as -1, 0, or 1.
fn decode_big_integer_sign_payload(raw_bytes: &[u8]) -> Result<BigIntSign, FieldDecodeError> {
    match decode_required_i64_payload(raw_bytes)? {
        -1 => Ok(BigIntSign::Minus),
        0 => Ok(BigIntSign::NoSign),
        1 => Ok(BigIntSign::Plus),
        _ => Err(FieldDecodeError::new()),
    }
}

// Decode one big-integer magnitude payload serialized as a canonical sequence
// of base-2^32 limbs.
fn decode_big_integer_magnitude_payload(raw_bytes: &[u8]) -> Result<BigUint, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    let mut limbs = Vec::new();
    for _ in 0..len {
        limbs.try_reserve(1).map_err(|_| FieldDecodeError::new())?;
        let limb_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        limbs.push(decode_required_u32_payload(&raw_bytes[limb_start..cursor])?);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(BigUint::new(limbs))
}

// Decode one required top-level `null` payload and enforce full-byte
// consumption.
fn decode_required_null_payload(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    let (tag, _, _) = parse_complete_binary_value(raw_bytes)?;
    if tag != TAG_NULL {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

// Decode one required top-level byte-string payload and enforce full-byte
// consumption.
fn decode_required_bytes_payload(raw_bytes: &[u8]) -> Result<&[u8], FieldDecodeError> {
    let (tag, len, payload_start) = parse_complete_binary_value(raw_bytes)?;
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }

    binary_payload_bytes(raw_bytes, len, payload_start)
}

// Decode one required top-level `u32` payload and enforce full-byte
// consumption.
fn decode_required_u32_payload(raw_bytes: &[u8]) -> Result<u32, FieldDecodeError> {
    u32::try_from(decode_required_u64_payload(raw_bytes)?).map_err(|_| FieldDecodeError::new())
}

// Decode one required top-level `u64` payload and enforce full-byte
// consumption.
fn decode_required_u64_payload(raw_bytes: &[u8]) -> Result<u64, FieldDecodeError> {
    let (tag, len, payload_start) = parse_complete_binary_value(raw_bytes)?;
    if tag != TAG_NAT64 || len != 8 {
        return Err(FieldDecodeError::new());
    }

    decode_u64_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)
}

// Decode one required top-level `i64` payload and enforce full-byte
// consumption.
fn decode_required_i64_payload(raw_bytes: &[u8]) -> Result<i64, FieldDecodeError> {
    let (tag, len, payload_start) = parse_complete_binary_value(raw_bytes)?;
    if tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new());
    }

    decode_i64_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)
}

// Split one fixed-length binary tuple into self-contained item slices.
fn split_binary_tuple_items(
    raw_bytes: &[u8],
    expected_len: u32,
) -> Result<Vec<&[u8]>, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST || len != expected_len {
        return Err(FieldDecodeError::new());
    }

    let mut items = Vec::with_capacity(expected_len as usize);
    let mut cursor = payload_start;
    for _ in 0..expected_len {
        let item_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        items.push(&raw_bytes[item_start..cursor]);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(items)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        decode_leaf_field_by_kind_bytes, encode_leaf_field_binary_bytes, push_binary_bytes,
        push_binary_int64, push_binary_list_len, push_binary_nat64,
    };
    use crate::{
        db::data::structural_field::{
            binary::push_binary_text, validate_structural_field_by_accepted_kind_bytes,
        },
        db::schema::AcceptedFieldKind,
        types::{Date, Decimal, Duration, IntBig, NatBig},
        value::Value,
    };
    use icydb_schema::DEFAULT_BIG_INT_MAX_BYTES;
    #[test]
    fn leaf_field_binary_roundtrips_supported_leaf_wrappers() {
        let cases = vec![
            (
                AcceptedFieldKind::Date,
                Value::Date(Date::try_new(2025, 10, 19).expect("valid date")),
            ),
            (
                AcceptedFieldKind::Decimal { scale: 2 },
                Value::Decimal(Decimal::from_i128_with_scale(12_345, 2)),
            ),
            (
                AcceptedFieldKind::Duration,
                Value::Duration(Duration::from_secs(5)),
            ),
            (
                AcceptedFieldKind::IntBig {
                    max_bytes: DEFAULT_BIG_INT_MAX_BYTES,
                },
                Value::IntBig(IntBig::from(123_456_789_i64)),
            ),
            (
                AcceptedFieldKind::NatBig {
                    max_bytes: DEFAULT_BIG_INT_MAX_BYTES,
                },
                Value::NatBig(NatBig::from(987_654_321_u64)),
            ),
        ];

        for (kind, value) in cases {
            let encoded = encode_leaf_field_binary_bytes(&kind, &value, "field")
                .expect("leaf payload should encode")
                .expect("leaf kind should be owned by the leaf lane");
            let decoded = decode_leaf_field_by_kind_bytes(encoded.as_slice(), &kind)
                .expect("leaf payload should decode")
                .expect("leaf kind should decode through the leaf lane");

            validate_structural_field_by_accepted_kind_bytes(encoded.as_slice(), &kind)
                .expect("leaf payload should validate");

            assert_eq!(decoded, value, "leaf roundtrip mismatch for {kind:?}");
        }
    }

    #[test]
    fn leaf_field_binary_rejects_malformed_decimal_payload() {
        let mut bytes = Vec::new();
        push_binary_list_len(&mut bytes, 2);
        push_binary_bytes(&mut bytes, &1_i128.to_be_bytes());
        push_binary_nat64(&mut bytes, u64::from(Decimal::max_supported_scale() + 1));

        let kind = AcceptedFieldKind::Decimal { scale: 2 };

        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), &kind);
        let validate = validate_structural_field_by_accepted_kind_bytes(bytes.as_slice(), &kind);

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
    fn leaf_field_binary_rejects_invalid_int_big_sign() {
        let mut bytes = Vec::new();
        push_binary_list_len(&mut bytes, 2);
        push_binary_int64(&mut bytes, 2);
        push_binary_list_len(&mut bytes, 0);

        let kind = AcceptedFieldKind::IntBig {
            max_bytes: DEFAULT_BIG_INT_MAX_BYTES,
        };
        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), &kind);
        let validate = validate_structural_field_by_accepted_kind_bytes(bytes.as_slice(), &kind);

        assert!(decode.is_err(), "invalid int_big sign must fail decode");
        assert!(validate.is_err(), "invalid int_big sign must fail validate");
    }

    #[test]
    fn leaf_field_binary_rejects_non_list_nat_big_payload() {
        let mut bytes = Vec::new();
        push_binary_text(&mut bytes, "not-a-limb-list");

        let kind = AcceptedFieldKind::NatBig {
            max_bytes: DEFAULT_BIG_INT_MAX_BYTES,
        };
        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), &kind);
        let validate = validate_structural_field_by_accepted_kind_bytes(bytes.as_slice(), &kind);

        assert!(decode.is_err(), "non-list nat_big payload must fail decode");
        assert!(
            validate.is_err(),
            "non-list nat_big payload must fail validate"
        );
    }
}
