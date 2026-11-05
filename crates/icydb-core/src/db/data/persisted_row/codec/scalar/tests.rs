use super::*;

#[test]
fn narrow_integer_slots_preserve_boundaries_and_exact_widths() {
    for (codec, width, min, max) in [
        (ScalarCodec::Int8, 1, i64::from(i8::MIN), i64::from(i8::MAX)),
        (
            ScalarCodec::Int16,
            2,
            i64::from(i16::MIN),
            i64::from(i16::MAX),
        ),
        (
            ScalarCodec::Int32,
            4,
            i64::from(i32::MIN),
            i64::from(i32::MAX),
        ),
    ] {
        for value in [min, -1, 0, 1, max] {
            let encoded = encode_scalar_slot_value(
                ScalarSlotValueRef::Value(ScalarValueRef::Int(value)),
                codec,
            )
            .unwrap();
            assert_eq!(encoded.len(), 2 + width);
            assert_eq!(&encoded[2..], &value.to_le_bytes()[..width]);
            assert_eq!(
                decode_scalar_slot_value(&encoded, codec)
                    .unwrap()
                    .into_value(),
                Value::Int64(value)
            );
        }
        for value in [min - 1, max + 1] {
            assert!(
                encode_scalar_slot_value(
                    ScalarSlotValueRef::Value(ScalarValueRef::Int(value)),
                    codec,
                )
                .is_err()
            );
        }
    }
    for (codec, width, max) in [
        (ScalarCodec::Nat8, 1, u64::from(u8::MAX)),
        (ScalarCodec::Nat16, 2, u64::from(u16::MAX)),
        (ScalarCodec::Nat32, 4, u64::from(u32::MAX)),
    ] {
        for value in [0, 1, max] {
            let encoded = encode_scalar_slot_value(
                ScalarSlotValueRef::Value(ScalarValueRef::Nat(value)),
                codec,
            )
            .unwrap();
            assert_eq!(encoded.len(), 2 + width);
            assert_eq!(&encoded[2..], &value.to_le_bytes()[..width]);
            assert_eq!(
                decode_scalar_slot_value(&encoded, codec)
                    .unwrap()
                    .into_value(),
                Value::Nat64(value)
            );
        }
        assert!(
            encode_scalar_slot_value(
                ScalarSlotValueRef::Value(ScalarValueRef::Nat(max + 1)),
                codec,
            )
            .is_err()
        );
    }
}

#[test]
fn narrow_integer_slots_validate_payload_lengths_and_null_envelopes() {
    for (codec, width) in [
        (ScalarCodec::Int8, 1),
        (ScalarCodec::Int16, 2),
        (ScalarCodec::Int32, 4),
        (ScalarCodec::Nat8, 1),
        (ScalarCodec::Nat16, 2),
        (ScalarCodec::Nat32, 4),
    ] {
        for len in [width - 1, width + 1] {
            let mut encoded = vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_VALUE];
            encoded.resize(2 + len, 0);
            assert_eq!(
                decode_scalar_slot_value(&encoded, codec)
                    .unwrap_err()
                    .class(),
                crate::error::ErrorClass::Corruption
            );
        }
        let encoded = encode_scalar_slot_value(ScalarSlotValueRef::Null, codec).unwrap();
        assert_eq!(encoded, [SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_NULL]);
        assert_eq!(
            decode_scalar_slot_value(&encoded, codec)
                .unwrap()
                .into_value(),
            Value::Null
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_scalar_slot_value(&trailing, codec).is_err());
    }
}

fn encoded_date_slot(days: i32) -> Vec<u8> {
    let mut encoded = vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_VALUE];
    encoded.extend_from_slice(&days.to_le_bytes());
    encoded
}

#[test]
fn date_slot_decode_rejects_days_outside_bounded_calendar() {
    let valid = encoded_date_slot(Date::MAX.as_days_since_epoch());
    let invalid = encoded_date_slot(Date::MAX.as_days_since_epoch() + 1);

    assert!(matches!(
        decode_scalar_slot_value(&valid, ScalarCodec::Date),
        Ok(ScalarSlotValueRef::Value(ScalarValueRef::Date(Date::MAX))),
    ));
    assert!(decode_scalar_slot_value(&invalid, ScalarCodec::Date).is_err());
}

#[test]
fn scalar_slot_materialization_preserves_null_and_payload_values() {
    assert_eq!(ScalarSlotValueRef::Null.into_value(), Value::Null);
    assert_eq!(
        ScalarSlotValueRef::Value(ScalarValueRef::Text("current")).into_value(),
        Value::Text("current".to_string()),
    );
    assert_eq!(
        ScalarSlotValueRef::Value(ScalarValueRef::Nat(42)).into_value(),
        Value::Nat64(42),
    );
}

#[test]
fn scalar_primary_key_conversion_keeps_one_supported_family_authority() {
    assert_eq!(
        ScalarValueRef::Int(-7).into_primary_key_component(),
        Some(PrimaryKeyComponent::Int64(-7)),
    );
    assert_eq!(
        ScalarValueRef::Nat(42).into_primary_key_component(),
        Some(PrimaryKeyComponent::Nat64(42)),
    );
    assert_eq!(
        ScalarValueRef::Unit.into_primary_key_component(),
        Some(PrimaryKeyComponent::Unit),
    );
    assert_eq!(
        ScalarValueRef::U256(U256::ONE).into_primary_key_component(),
        Some(PrimaryKeyComponent::U256(U256::ONE)),
    );
    assert_eq!(
        ScalarValueRef::Text("not-a-key").into_primary_key_component(),
        None,
    );
}

#[test]
fn time_scalar_slots_roundtrip_exact_primitive_payloads() {
    let duration = Duration::from_millis(u64::MAX);
    let encoded_duration = encode_scalar_slot_value(
        ScalarSlotValueRef::Value(ScalarValueRef::Duration(duration)),
        ScalarCodec::Duration,
    )
    .unwrap();
    assert_eq!(&encoded_duration[2..], &duration.as_millis().to_le_bytes());
    assert!(matches!(
        decode_scalar_slot_value(&encoded_duration, ScalarCodec::Duration),
        Ok(ScalarSlotValueRef::Value(ScalarValueRef::Duration(decoded))) if decoded == duration,
    ));

    let timestamp = Timestamp::from_millis(i64::MIN);
    let encoded_timestamp = encode_scalar_slot_value(
        ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(timestamp)),
        ScalarCodec::Timestamp,
    )
    .unwrap();
    assert_eq!(
        &encoded_timestamp[2..],
        &timestamp.as_millis().to_le_bytes()
    );
    assert!(matches!(
        decode_scalar_slot_value(&encoded_timestamp, ScalarCodec::Timestamp),
        Ok(ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(decoded))) if decoded == timestamp,
    ));
}

#[test]
fn u256_scalar_slot_roundtrips_exact_fixed_width_payload() {
    for value in [U256::ZERO, U256::ONE, U256::MAX] {
        let encoded = encode_scalar_slot_value(
            ScalarSlotValueRef::Value(ScalarValueRef::U256(value)),
            ScalarCodec::U256,
        )
        .unwrap();

        assert_eq!(encoded.len(), 34);
        assert_eq!(&encoded[..2], &[SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_VALUE]);
        assert_eq!(&encoded[2..], value.to_be_bytes());
        assert!(matches!(
            decode_scalar_slot_value(&encoded, ScalarCodec::U256),
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::U256(decoded))) if decoded == value,
        ));
    }
}

#[test]
fn u256_scalar_slot_rejects_non_exact_payload_lengths() {
    for payload_len in [31, 33] {
        let mut encoded = vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_VALUE];
        encoded.resize(2 + payload_len, 0);

        assert!(decode_scalar_slot_value(&encoded, ScalarCodec::U256).is_err());
    }
}
