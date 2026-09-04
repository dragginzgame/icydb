//! Module: data::structural_field::leaf
//! Responsibility: typed wrapper and structured leaf decoding that still has fixed payload semantics.
//! Does not own: scalar primitive fast paths, composite recursion, or `Value` storage envelopes.
//! Boundary: sibling modules use this file for leaf contracts like decimal, duration, `int_big`, `nat_big`, and date.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        decode_binary_decimal_payload, decode_binary_int_big_payload,
        decode_binary_nat_big_payload, decode_binary_required_i64, decode_binary_required_null,
        decode_binary_required_u64, push_binary_decimal_payload, push_binary_int_big_payload,
        push_binary_int64, push_binary_nat_big_payload, push_binary_nat64, push_binary_null,
    },
    primary_key_component::{
        decode_primary_key_component_binary_value_bytes,
        encode_primary_key_component_binary_value_bytes,
    },
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
    decode_binary_required_null(raw_bytes)?;

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
    decode_date_payload_days(decode_binary_required_i64(raw_bytes)?).map(Value::Date)
}

// Decode one decimal payload from the canonical `(mantissa_bytes, scale)`
// tuple.
fn decode_decimal_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let (mantissa, scale) = decode_binary_decimal_payload(raw_bytes)?;

    Ok(Value::Decimal(decode_decimal_payload_mantissa_and_scale(
        mantissa, scale,
    )?))
}

// Decode one duration payload from its canonical millis form.
fn decode_duration_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    Ok(Value::Duration(decode_duration_payload_millis(
        decode_binary_required_u64(raw_bytes)?,
    )))
}

// Decode one bounded signed big-integer payload from the canonical `(sign,
// limbs)` tuple used by `int_big`.
fn decode_int_big_value_bytes(raw_bytes: &[u8], max_bytes: u32) -> Result<Value, FieldDecodeError> {
    let value = IntBig::from_bigint(decode_binary_int_big_payload(raw_bytes)?);
    ensure_int_big_max_bytes(&value, max_bytes)?;

    Ok(Value::IntBig(value))
}

// Decode one bounded unsigned big-integer payload from the canonical limb
// sequence used by `nat_big`.
fn decode_nat_big_value_bytes(raw_bytes: &[u8], max_bytes: u32) -> Result<Value, FieldDecodeError> {
    let value = NatBig::from_biguint(decode_binary_nat_big_payload(raw_bytes)?);
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
    push_binary_decimal_payload(&mut encoded, mantissa, scale);

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
    let mut encoded = Vec::new();
    push_binary_int_big_payload(&mut encoded, is_negative, digits.as_slice());

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
    push_binary_nat_big_payload(&mut encoded, value.u32_digits().as_slice());

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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{decode_leaf_field_by_kind_bytes, encode_leaf_field_binary_bytes};
    use crate::{
        db::data::structural_field::{
            binary::{
                push_binary_bytes, push_binary_int64, push_binary_list_len, push_binary_nat64,
                push_binary_text,
            },
            validate_structural_field_by_accepted_kind_bytes,
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
