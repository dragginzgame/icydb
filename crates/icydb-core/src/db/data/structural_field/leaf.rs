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
        push_primary_key_component_binary_value_bytes,
    },
    typed::{
        decode_date_payload_days, decode_duration_payload_millis, encode_date_payload_days,
        encode_duration_payload_millis,
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
pub(super) fn push_leaf_field_binary_bytes(
    out: &mut Vec<u8>,
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<bool, InternalError> {
    match kind {
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => {
            return push_primary_key_component_binary_value_bytes(out, kind, value, field_name);
        }
        AcceptedFieldKind::Date => push_date_value_bytes(out, value, field_name)?,
        AcceptedFieldKind::Decimal { .. } => push_decimal_value_bytes(out, value, field_name)?,
        AcceptedFieldKind::Duration => push_duration_value_bytes(out, value, field_name)?,
        AcceptedFieldKind::IntBig { max_bytes } => {
            push_int_big_value_bytes(out, value, *max_bytes, field_name)?;
        }
        AcceptedFieldKind::Composite { .. } => {
            push_structured_leaf_null_bytes(out, value, field_name)?;
        }
        AcceptedFieldKind::NatBig { max_bytes } => {
            push_nat_big_value_bytes(out, value, *max_bytes, field_name)?;
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
        | AcceptedFieldKind::Set(_) => return Ok(false),
    }

    Ok(true)
}

// Decode the only supported structured leaf `ByKind` case: explicit null.
fn decode_structured_leaf_null_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_binary_required_null(raw_bytes)?;

    Ok(Value::Null)
}

// Encode the only supported structured leaf `ByKind` case: explicit null.
fn push_structured_leaf_null_bytes(
    out: &mut Vec<u8>,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Null = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    push_binary_null(out);

    Ok(())
}

// Decode one date payload from its canonical signed day-count form.
fn decode_date_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_date_payload_days(decode_binary_required_i64(raw_bytes)?).map(Value::Date)
}

// Decode one Decimal through its shared fixed payload owner.
fn decode_decimal_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_binary_decimal_payload(raw_bytes).map(Value::Decimal)
}

// Decode one duration payload from its canonical millis form.
fn decode_duration_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    Ok(Value::Duration(decode_duration_payload_millis(
        decode_binary_required_u64(raw_bytes)?,
    )))
}

// Decode one bounded signed big-integer byte payload.
fn decode_int_big_value_bytes(raw_bytes: &[u8], max_bytes: u32) -> Result<Value, FieldDecodeError> {
    let value = IntBig::from_bigint(decode_binary_int_big_payload(raw_bytes)?);
    ensure_int_big_max_bytes(&value, max_bytes)?;

    Ok(Value::IntBig(value))
}

// Decode one bounded unsigned big-integer magnitude byte payload.
fn decode_nat_big_value_bytes(raw_bytes: &[u8], max_bytes: u32) -> Result<Value, FieldDecodeError> {
    let value = NatBig::from_biguint(decode_binary_nat_big_payload(raw_bytes)?);
    ensure_nat_big_max_bytes(&value, max_bytes)?;

    Ok(Value::NatBig(value))
}

// Encode one date payload into canonical signed day-count form.
fn push_date_value_bytes(
    out: &mut Vec<u8>,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Date(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    push_binary_int64(out, encode_date_payload_days(*value));
    Ok(())
}

// Encode one Decimal with its shared fixed payload and generic byte frame.
fn push_decimal_value_bytes(
    out: &mut Vec<u8>,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Decimal(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    push_binary_decimal_payload(out, *value);

    Ok(())
}

// Encode one duration payload into canonical millis.
fn push_duration_value_bytes(
    out: &mut Vec<u8>,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Duration(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };

    push_binary_nat64(out, encode_duration_payload_millis(*value));
    Ok(())
}

// Encode one bounded signed big-integer sign and minimal magnitude.
fn push_int_big_value_bytes(
    out: &mut Vec<u8>,
    value: &Value,
    max_bytes: u32,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::IntBig(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };
    ensure_int_big_max_bytes(value, max_bytes)
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field_name))?;

    let (is_negative, digits) = value.sign_and_u32_digits();
    push_binary_int_big_payload(out, is_negative, digits);

    Ok(())
}

// Encode one bounded unsigned big-integer minimal magnitude.
fn push_nat_big_value_bytes(
    out: &mut Vec<u8>,
    value: &Value,
    max_bytes: u32,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::NatBig(value) = value else {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    };
    ensure_nat_big_max_bytes(value, max_bytes)
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field_name))?;

    push_binary_nat_big_payload(out, value.u32_digits());

    Ok(())
}

fn ensure_int_big_max_bytes(value: &IntBig, max_bytes: u32) -> Result<(), FieldDecodeError> {
    if value.leb128_len() > u64::from(max_bytes) {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

fn ensure_nat_big_max_bytes(value: &NatBig, max_bytes: u32) -> Result<(), FieldDecodeError> {
    if value.leb128_len() > u64::from(max_bytes) {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::decode_leaf_field_by_kind_bytes;
    use crate::db::data::structural_field::encode_structural_field_by_accepted_kind_bytes;
    use crate::{
        db::data::structural_field::{
            binary::{push_binary_bytes, push_binary_text},
            validate_structural_field_by_accepted_kind_bytes,
        },
        db::schema::AcceptedFieldKind,
        types::{Date, Decimal, Duration, IntBig, NatBig},
        value::Value,
    };
    use icydb_schema::DEFAULT_BIG_INT_MAX_BYTES;
    #[test]
    fn bigint_leaf_byte_limits_preserve_exact_and_rejected_boundaries() {
        for integer in [-8193_i32, -8192, -65, -64, -1, 0, 63, 64, 8191, 8192] {
            let signed = IntBig::from(integer);
            let unsigned = NatBig::from(integer.unsigned_abs());
            let cases = [
                (
                    u32::try_from(signed.to_leb128().len()).unwrap(),
                    Value::IntBig(signed),
                ),
                (
                    u32::try_from(unsigned.to_leb128().len()).unwrap(),
                    Value::NatBig(unsigned),
                ),
            ];
            for (length, value) in cases {
                let admitted_kind = match &value {
                    Value::IntBig(_) => AcceptedFieldKind::IntBig { max_bytes: length },
                    _ => AcceptedFieldKind::NatBig { max_bytes: length },
                };
                let admitted =
                    encode_structural_field_by_accepted_kind_bytes(&admitted_kind, &value, "field")
                        .unwrap();
                for max_bytes in [length - 1, length, length + 1] {
                    let kind = match &value {
                        Value::IntBig(_) => AcceptedFieldKind::IntBig { max_bytes },
                        _ => AcceptedFieldKind::NatBig { max_bytes },
                    };
                    let encoded =
                        encode_structural_field_by_accepted_kind_bytes(&kind, &value, "field");
                    assert_eq!(encoded.is_ok(), max_bytes >= length);
                    assert_eq!(
                        decode_leaf_field_by_kind_bytes(&admitted, &kind).is_ok(),
                        max_bytes >= length
                    );
                    assert_eq!(
                        validate_structural_field_by_accepted_kind_bytes(&admitted, &kind).is_ok(),
                        max_bytes >= length
                    );
                    if let Ok(bytes) = encoded {
                        assert_eq!(
                            decode_leaf_field_by_kind_bytes(&bytes, &kind)
                                .unwrap()
                                .unwrap(),
                            value
                        );
                        validate_structural_field_by_accepted_kind_bytes(&bytes, &kind).unwrap();
                    }
                }
            }
        }
    }

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
            let encoded = encode_structural_field_by_accepted_kind_bytes(&kind, &value, "field")
                .expect("leaf payload should encode");
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
        let mut payload = vec![0; 17];
        payload[15] = 1;
        payload[16] = u8::try_from(Decimal::max_supported_scale() + 1).unwrap();
        push_binary_bytes(&mut bytes, &payload);

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
        push_binary_bytes(&mut bytes, &[3, 1]);

        let kind = AcceptedFieldKind::IntBig {
            max_bytes: DEFAULT_BIG_INT_MAX_BYTES,
        };
        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), &kind);
        let validate = validate_structural_field_by_accepted_kind_bytes(bytes.as_slice(), &kind);

        assert!(decode.is_err(), "invalid int_big sign must fail decode");
        assert!(validate.is_err(), "invalid int_big sign must fail validate");
    }

    #[test]
    fn leaf_field_binary_rejects_non_bytes_nat_big_payload() {
        let mut bytes = Vec::new();
        push_binary_text(&mut bytes, "not-a-magnitude");

        let kind = AcceptedFieldKind::NatBig {
            max_bytes: DEFAULT_BIG_INT_MAX_BYTES,
        };
        let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), &kind);
        let validate = validate_structural_field_by_accepted_kind_bytes(bytes.as_slice(), &kind);

        assert!(
            decode.is_err(),
            "non-bytes nat_big payload must fail decode"
        );
        assert!(
            validate.is_err(),
            "non-bytes nat_big payload must fail validate"
        );
    }
}
