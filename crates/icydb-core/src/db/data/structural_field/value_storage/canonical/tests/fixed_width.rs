//! Exact widths and traversal boundaries for current fixed local scalar tags.

use super::*;
use crate::{
    db::data::{
        ValueStorageView,
        structural_field::{
            binary::{decode_binary_decimal_payload, push_binary_decimal_payload},
            typed::encode_decimal_payload_bytes,
            value_storage::{skip::skip_value_storage_binary_value, tags::*},
        },
    },
    types::{
        Account, AccountStorageCodec, Date, Duration, Float32, Float64, Principal, Subaccount,
        Timestamp, U256, Ulid,
    },
};

fn fixed_values() -> Vec<(CanonicalValue, usize)> {
    vec![
        (
            CanonicalValue::Account(Account::new(Principal::MAX, None::<Subaccount>)),
            63,
        ),
        (
            CanonicalValue::Account(Account::new(Principal::MAX, Some(Subaccount::MAX))),
            63,
        ),
        (CanonicalValue::Date(Date::MIN), 5),
        (CanonicalValue::Date(Date::MAX), 5),
        (
            CanonicalValue::Decimal(Decimal::from_i128_with_scale(i128::MIN, 28)),
            18,
        ),
        (CanonicalValue::Duration(Duration::from_millis(u64::MAX)), 9),
        (
            CanonicalValue::Float32(Float32::try_new(f32::MIN).unwrap()),
            5,
        ),
        (
            CanonicalValue::Float64(Float64::try_new(f64::MAX).unwrap()),
            9,
        ),
        (CanonicalValue::Int128(i128::MIN), 17),
        (CanonicalValue::Nat128(u128::MAX), 17),
        (CanonicalValue::Subaccount(Subaccount::MAX), 33),
        (
            CanonicalValue::Timestamp(Timestamp::from_millis(i64::MIN)),
            9,
        ),
        (CanonicalValue::Ulid(Ulid::MAX), 17),
        (CanonicalValue::U256(U256::MAX), 33),
    ]
}

#[test]
fn fixed_scalar_storage_preserves_exact_widths_and_nested_boundaries() {
    for (value, width) in fixed_values() {
        let encoded = encode_canonical_value_storage_bytes(&value).unwrap();
        assert_eq!(encoded.len(), width);
        assert_eq!(
            decode_canonical_value_storage_bytes(&encoded).unwrap(),
            value
        );
        assert_eq!(skip_value_storage_binary_value(&encoded, 0).unwrap(), width);
        if let CanonicalValue::Account(account) = value {
            assert_eq!(&encoded[1..], account.to_stored_bytes().unwrap());
        }
        let list = CanonicalValue::List(vec![value.clone(); 1000]);
        let bytes = encode_canonical_value_storage_bytes(&list).unwrap();
        assert_eq!(bytes.len(), 5 + 1000 * width);
        assert_eq!(decode_canonical_value_storage_bytes(&bytes).unwrap(), list);
        let nested = CanonicalValue::Map(vec![
            (CanonicalValue::Text("a".into()), value.clone()),
            (CanonicalValue::Text("b".into()), CanonicalValue::Nat64(7)),
        ]);
        let bytes = encode_canonical_value_storage_bytes(&nested).unwrap();
        let view = ValueStorageView::from_raw_validated(&bytes).unwrap();
        assert_eq!(
            view.map_text_key_bytes(b"b")
                .unwrap()
                .unwrap()
                .as_u64()
                .unwrap(),
            7
        );
        assert_eq!(
            decode_canonical_value_storage_bytes(&bytes).unwrap(),
            nested
        );
        let with_enum = canonical_enum(Some(CanonicalValue::List(vec![value])));
        let bytes = encode_canonical_value_storage_bytes(&with_enum).unwrap();
        assert_eq!(
            decode_canonical_value_storage_bytes(&bytes).unwrap(),
            with_enum
        );
    }
}

#[test]
fn fixed_scalar_storage_rejects_truncation_trailing_and_invalid_domains() {
    for (value, _) in fixed_values() {
        let mut encoded = encode_canonical_value_storage_bytes(&value).unwrap();
        for end in 0..encoded.len() {
            assert!(decode_canonical_value_storage_bytes(&encoded[..end]).is_err());
            assert!(ValueStorageView::from_raw_validated(&encoded[..end]).is_err());
        }
        encoded.push(0);
        assert!(decode_canonical_value_storage_bytes(&encoded).is_err());
        assert!(ValueStorageView::from_raw_validated(&encoded).is_err());
    }
    for (tag, payload) in [
        (VALUE_BINARY_TAG_DATE, i32::MAX.to_be_bytes().to_vec()),
        (
            VALUE_BINARY_TAG_FLOAT32,
            f32::NAN.to_bits().to_be_bytes().to_vec(),
        ),
        (
            VALUE_BINARY_TAG_FLOAT64,
            f64::INFINITY.to_bits().to_be_bytes().to_vec(),
        ),
        (VALUE_BINARY_TAG_ACCOUNT, vec![255; 62]),
    ] {
        let mut encoded = vec![tag];
        encoded.extend(payload);
        assert!(decode_canonical_value_storage_bytes(&encoded).is_err());
    }
}

#[test]
fn decimal_storage_preserves_full_mantissa_and_scale_in_seventeen_bytes() {
    for mantissa in [i128::MIN, -1200, 0, 1, 1200, i128::MAX] {
        for scale in [0, 2, Decimal::max_supported_scale()] {
            let value = Decimal::from_i128_with_scale(mantissa, scale);
            let mut direct = Vec::new();
            push_binary_decimal_payload(&mut direct, value);
            assert_eq!(direct.len(), 22);
            assert_eq!(
                decode_binary_decimal_payload(&direct).unwrap().parts(),
                value.parts()
            );
            let encoded =
                encode_canonical_value_storage_bytes(&CanonicalValue::Decimal(value)).unwrap();
            assert_eq!(encoded.len(), 18);
            assert_eq!(&encoded[1..17], mantissa.to_be_bytes());
            assert_eq!(u32::from(encoded[17]), scale);
            let CanonicalValue::Decimal(decoded) =
                decode_canonical_value_storage_bytes(&encoded).unwrap()
            else {
                panic!("decimal")
            };
            assert_eq!(decoded.parts(), value.parts());
        }
    }
    // Even zero or trailing-zero mantissas cannot normalize invalid stored scales.
    for mantissa in [0, 10, -100, i128::MAX] {
        for scale in [29, 255] {
            let mut payload =
                encode_decimal_payload_bytes(Decimal::from_i128_with_scale(mantissa, 0));
            payload[16] = scale;
            let mut encoded = vec![VALUE_BINARY_TAG_DECIMAL];
            encoded.extend(payload);
            assert!(decode_canonical_value_storage_bytes(&encoded).is_err());
            let mut direct = Vec::new();
            crate::db::data::structural_field::binary::push_binary_bytes(&mut direct, &payload);
            assert!(decode_binary_decimal_payload(&direct).is_err());
        }
    }
}
