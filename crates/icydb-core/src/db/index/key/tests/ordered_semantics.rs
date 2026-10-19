//! Module: db::index::key::tests::ordered_semantics
//! Covers ordered-semantic invariants for encoded index keys.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        index::key::{
            OrderedValueEncodeError,
            ordered::{
                compare_index_component_values, encode_canonical_index_component,
                encode_canonical_index_component_from_primary_key_value,
            },
        },
        key_taxonomy::{EncodedPrimaryKey, PrimaryKeyComponent},
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256,
        Ulid,
    },
    value::Value,
};

use proptest::prelude::*;
use std::cmp::Ordering;

fn assert_encoded_order(left: Value, right: Value, expected: Ordering) {
    let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
    let right_bytes = encode_canonical_index_component(&right).expect("right should encode");

    assert_eq!(left_bytes.cmp(&right_bytes), expected);
}

#[test]
fn canonical_encoder_rejects_non_indexable_and_unsupported_values() {
    assert!(encode_canonical_index_component(&Value::Null).is_err());
    assert!(encode_canonical_index_component(&Value::Blob(vec![1u8, 2u8])).is_err());
    assert!(encode_canonical_index_component(&Value::List(vec![Value::Int64(1)])).is_err());
    assert!(encode_canonical_index_component(&Value::Map(vec![])).is_err());
}

#[test]
fn principal_ordered_components_match_semantic_and_primary_key_order() {
    let mut principals = vec![Principal::from_slice(&[])];
    for len in 1..=29 {
        for byte in [0, 1, 4, 255] {
            principals.push(Principal::from_slice(&vec![byte; len]));
        }
    }
    for &left in &principals {
        let left_value = Value::Principal(left);
        let left_bytes = encode_canonical_index_component(&left_value).unwrap();
        assert_eq!(
            left_bytes,
            encode_canonical_index_component_from_primary_key_value(
                PrimaryKeyComponent::Principal(left)
            )
            .unwrap()
        );
        for &right in &principals {
            let right_value = Value::Principal(right);
            let expected = left.cmp(&right);
            assert_eq!(
                compare_index_component_values(&left_value, &right_value),
                expected
            );
            assert_eq!(
                left_bytes.cmp(&encode_canonical_index_component(&right_value).unwrap()),
                expected,
                "index order for {left:?} vs {right:?}"
            );
            assert_eq!(
                EncodedPrimaryKey::encode(PrimaryKeyComponent::Principal(left))
                    .unwrap()
                    .as_bytes()
                    .cmp(
                        EncodedPrimaryKey::encode(PrimaryKeyComponent::Principal(right))
                            .unwrap()
                            .as_bytes()
                    ),
                expected,
            );
        }
    }
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn canonical_encoder_account_payload_uses_exact_owner_length_tag() {
    let account = Account::new(Principal::MAX, None::<Subaccount>);
    let value = Value::Account(account);

    let encoded =
        encode_canonical_index_component(&value).expect("max-length account should encode");
    assert_eq!(
        encoded[1],
        Principal::MAX_LENGTH_IN_BYTES as u8,
        "account payload owner-length tag should preserve the full principal length"
    );
}

#[test]
fn primary_key_value_encoder_matches_value_encoder_for_all_primary_key_variants() {
    let samples = [
        PrimaryKeyComponent::Account(Account::from_owner_and_subaccount(
            Principal::from_slice(&[7]),
            Some(Subaccount::from_array([7; 32])),
        )),
        PrimaryKeyComponent::Int64(-7),
        PrimaryKeyComponent::Principal(Principal::from_slice(&[1u8, 2u8, 3u8])),
        PrimaryKeyComponent::Subaccount(Subaccount::from_array([7u8; 32])),
        PrimaryKeyComponent::Timestamp(Timestamp::from_secs(7)),
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Ulid(Ulid::from_u128(7)),
        PrimaryKeyComponent::Unit,
        PrimaryKeyComponent::U256(U256::MAX),
    ];

    for sample in samples {
        let primary_key_bytes = encode_canonical_index_component_from_primary_key_value(sample)
            .expect("primary key should encode");
        let value_bytes = encode_canonical_index_component(&sample.as_runtime_value())
            .expect("value should encode");

        assert_eq!(
            primary_key_bytes, value_bytes,
            "primary-key encoder diverged from canonical value encoder for {sample:?}"
        );
    }
}

#[test]
fn canonical_encoder_respects_numeric_order_for_scalars() {
    assert_encoded_order(Value::Int64(-2), Value::Int64(7), Ordering::Less);
    assert_encoded_order(Value::Int128(-2i128), Value::Int128(7i128), Ordering::Less);
    assert_encoded_order(Value::Nat64(2), Value::Nat64(7), Ordering::Less);
    assert_encoded_order(Value::Nat128(2u128), Value::Nat128(7u128), Ordering::Less);
    assert_encoded_order(
        Value::U256(U256::ZERO),
        Value::U256(U256::MAX),
        Ordering::Less,
    );
}

#[test]
fn canonical_encoder_respects_decimal_order_and_normalization() {
    let one = Value::Decimal(Decimal::new(1, 0));
    let one_point_zero = Value::Decimal(Decimal::new(10, 1));
    let one_point_one = Value::Decimal(Decimal::new(11, 1));

    let one_bytes = encode_canonical_index_component(&one).expect("one should encode");
    let one_point_zero_bytes =
        encode_canonical_index_component(&one_point_zero).expect("one_point_zero should encode");
    let one_point_one_bytes =
        encode_canonical_index_component(&one_point_one).expect("one_point_one should encode");

    assert_eq!(one_bytes, one_point_zero_bytes);
    assert!(one_bytes < one_point_one_bytes);
}

#[test]
fn canonical_encoder_supports_decimal_max_scale() {
    let max_scale = Decimal::max_supported_scale();
    let value = Value::Decimal(Decimal::new(1, max_scale));

    let encoded = encode_canonical_index_component(&value).expect("decimal max scale encodes");
    assert!(!encoded.is_empty());
}

#[test]
#[expect(clippy::unreadable_literal)]
fn canonical_encoder_decimal_large_mantissa_regression() {
    let lhs = Decimal::from_i128_with_scale(100000000000000003890313744798756555321, 2);
    let rhs = Decimal::from_i128_with_scale(158022371435723639313993729503199393550, 2);

    let lhs_value = Value::Decimal(lhs);
    let rhs_value = Value::Decimal(rhs);
    let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
    let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

    assert_eq!(
        lhs.cmp(&rhs),
        lhs_bytes.cmp(&rhs_bytes),
        "lhs={lhs:?} rhs={rhs:?} lhs_bytes={lhs_bytes:?} rhs_bytes={rhs_bytes:?}"
    );
}

#[test]
fn canonical_encoder_respects_float_order() {
    let neg = Value::Float64(Float64::try_new(-1.5).expect("finite"));
    let zero = Value::Float64(Float64::try_new(0.0).expect("finite"));
    let pos = Value::Float64(Float64::try_new(1.5).expect("finite"));

    assert_encoded_order(neg, zero.clone(), Ordering::Less);
    assert_encoded_order(zero, pos, Ordering::Less);

    let neg32 = Value::Float32(Float32::try_new(-1.5).expect("finite"));
    let zero32 = Value::Float32(Float32::try_new(0.0).expect("finite"));
    let pos32 = Value::Float32(Float32::try_new(1.5).expect("finite"));

    assert_encoded_order(neg32, zero32.clone(), Ordering::Less);
    assert_encoded_order(zero32, pos32, Ordering::Less);
}

#[test]
fn canonical_encoder_respects_text_and_identifier_order() {
    assert_encoded_order(
        Value::Text("a".to_string()),
        Value::Text("b".to_string()),
        Ordering::Less,
    );

    assert_encoded_order(
        Value::Principal(Principal::from_slice(&[1u8])),
        Value::Principal(Principal::from_slice(&[2u8])),
        Ordering::Less,
    );

    assert_encoded_order(
        Value::Ulid(Ulid::from_u128(1)),
        Value::Ulid(Ulid::from_u128(2)),
        Ordering::Less,
    );
}

#[test]
fn canonical_ordered_encoder_rejects_enums_without_accepted_key_proof() {
    assert!(matches!(
        encode_canonical_index_component(&Value::Enum(crate::value::ValueEnum::test_unit(1, 1),)),
        Err(OrderedValueEncodeError::UnsupportedValueKind),
    ));
    assert!(matches!(
        encode_canonical_index_component(&Value::Enum(
            crate::value::ValueEnum::test_unit(1, 1).test_with_payload(Value::Nat64(1)),
        )),
        Err(OrderedValueEncodeError::UnsupportedValueKind),
    ));
}

#[test]
fn canonical_encoder_golden_vectors_freeze_primitive_bytes() {
    let cases: Vec<(&str, Value, Vec<u8>)> = vec![
        ("Bool(false)", Value::Bool(false), vec![0x03, 0x00]),
        ("Bool(true)", Value::Bool(true), vec![0x03, 0x01]),
        (
            "Int(-1)",
            Value::Int64(-1),
            vec![0x0A, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        ),
        (
            "Nat(1)",
            Value::Nat64(1),
            vec![0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        ),
        (
            "Decimal(10,1)",
            Value::Decimal(Decimal::new(10, 1)),
            vec![0x05, 0x02, 0x80, 0x00, 0x00, 0x00, 0x31, 0x00],
        ),
        (
            "Float64(-1.0)",
            Value::Float64(Float64::try_new(-1.0).expect("finite")),
            vec![0x09, 0x40, 0x0F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        ),
        (
            "Float64(0.0)",
            Value::Float64(Float64::try_new(0.0).expect("finite")),
            vec![0x09, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
        (
            "Text(\"a\")",
            Value::Text("a".to_string()),
            vec![0x11, b'a', 0x00, 0x00],
        ),
        (
            "Principal([1,0,2])",
            Value::Principal(Principal::from_slice(&[1u8, 0u8, 2u8])),
            vec![0x0F, 0x03, 0x01, 0x00, 0x02],
        ),
        (
            "Ulid(1)",
            Value::Ulid(Ulid::from_u128(1)),
            vec![
                0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01,
            ],
        ),
        ("Unit", Value::Unit, vec![0x16]),
    ];

    for (name, value, expected) in cases {
        let actual =
            encode_canonical_index_component(&value).expect("golden-vector sample should encode");
        assert_eq!(
            actual, expected,
            "golden vector drift for {name}: {value:?}"
        );
    }
}

#[test]
fn canonical_encoder_total_order_matches_value_canonical_cmp_for_supported_samples() {
    let samples = vec![
        Value::Account(Account::from_owner_and_subaccount(
            Principal::from_slice(&[1]),
            Some(Subaccount::from_array([1; 32])),
        )),
        Value::Bool(false),
        Value::Bool(true),
        Value::Date(Date::try_new(2024, 1, 1).expect("valid date")),
        Value::Decimal(Decimal::new(-11, 1)),
        Value::Decimal(Decimal::new(0, 0)),
        Value::Decimal(Decimal::new(11, 1)),
        Value::Duration(Duration::from_secs(1)),
        Value::Float32(Float32::try_new(-1.0).expect("finite")),
        Value::Float32(Float32::try_new(1.0).expect("finite")),
        Value::Float64(Float64::try_new(-1.0).expect("finite")),
        Value::Float64(Float64::try_new(1.0).expect("finite")),
        Value::Int64(-2),
        Value::Int64(7),
        Value::Int128(-2i128),
        Value::Int128(7i128),
        Value::Principal(Principal::from_slice(&[1u8])),
        Value::Principal(Principal::from_slice(&[2u8])),
        Value::Subaccount(Subaccount::from_array([1; 32])),
        Value::Subaccount(Subaccount::from_array([2; 32])),
        Value::Text("a".to_string()),
        Value::Text("b".to_string()),
        Value::Timestamp(Timestamp::from_secs(1)),
        Value::Timestamp(Timestamp::from_secs(2)),
        Value::Nat64(1),
        Value::Nat64(2),
        Value::Nat128(1u128),
        Value::Nat128(2u128),
        Value::Ulid(Ulid::from_u128(1)),
        Value::Ulid(Ulid::from_u128(2)),
        Value::Unit,
    ];

    let mut by_value = samples.clone();
    by_value.sort_by(compare_index_component_values);

    let mut by_bytes = samples;
    by_bytes.sort_by(|left, right| {
        let left_bytes = encode_canonical_index_component(left).expect("left should encode");
        let right_bytes = encode_canonical_index_component(right).expect("right should encode");
        left_bytes.cmp(&right_bytes)
    });

    assert_eq!(by_value, by_bytes);
}

#[test]
fn unit_ordered_encoding_matches_its_trivial_total_order() {
    let dynamic = encode_canonical_index_component(&Value::Unit)
        .expect("Unit should encode as an ordered component");
    let primary_key =
        encode_canonical_index_component_from_primary_key_value(PrimaryKeyComponent::Unit)
            .expect("Unit should encode from primary-key authority");

    assert_eq!(
        Value::strict_order_cmp(&Value::Unit, &Value::Unit),
        Some(Ordering::Equal),
    );
    assert_eq!(
        compare_index_component_values(&Value::Unit, &Value::Unit),
        Ordering::Equal,
    );
    assert_eq!(dynamic, vec![Value::Unit.canonical_tag().to_u8()]);
    assert_eq!(dynamic, primary_key);
}

#[test]
fn index_component_compare_requires_strict_variant_match_for_numeric_widening() {
    let left = Value::Int64(7);
    let right = Value::Nat64(7);

    assert_eq!(
        compare_index_component_values(&left, &right),
        Value::canonical_cmp_key(&left, &right),
        "cross-variant numeric values should not use numeric widening in index component comparison",
    );
}

#[test]
fn timestamp_ordered_encoding_preserves_signed_millisecond_order() {
    let values = [
        i64::MIN,
        i64::MIN + 1,
        -1_000,
        -1,
        0,
        1,
        1_000,
        1_001,
        i64::MAX,
    ];
    let encoded = values.map(|millis| {
        let timestamp = Timestamp::from_millis(millis);
        let bytes = encode_canonical_index_component(&Value::Timestamp(timestamp)).unwrap();
        assert_eq!(bytes.len(), 9);
        assert_eq!(
            bytes,
            encode_canonical_index_component_from_primary_key_value(
                PrimaryKeyComponent::Timestamp(timestamp),
            )
            .unwrap(),
        );
        bytes
    });
    // Qualify the current representation at both extrema and the epoch.
    assert_eq!(&encoded[0][1..], &[0; 8]);
    assert_eq!(&encoded[4][1..], &0x8000_0000_0000_0000_u64.to_be_bytes());
    assert_eq!(&encoded[8][1..], &[0xff; 8]);
    for (left, left_bytes) in values.iter().zip(&encoded) {
        for (right, right_bytes) in values.iter().zip(&encoded) {
            let expected = left.cmp(right);
            assert_eq!(left_bytes.cmp(right_bytes), expected);
            assert_eq!(
                compare_index_component_values(
                    &Value::Timestamp(Timestamp::from_millis(*left)),
                    &Value::Timestamp(Timestamp::from_millis(*right)),
                ),
                expected,
            );
        }
    }
}

// Deterministic property-style check: for each primitive family fixture,
// canonical value ordering must match canonical encoded-byte ordering.
#[test]
#[expect(clippy::too_many_lines)]
fn canonical_encoder_pairwise_cmp_matches_bytes_for_primitive_families() {
    let families: Vec<(&str, Vec<Value>)> = vec![
        ("Bool", vec![Value::Bool(false), Value::Bool(true)]),
        (
            "Int",
            vec![
                Value::Int64(-2),
                Value::Int64(-1),
                Value::Int64(0),
                Value::Int64(7),
            ],
        ),
        (
            "Nat",
            vec![Value::Nat64(0), Value::Nat64(1), Value::Nat64(7)],
        ),
        (
            "Int128",
            vec![
                Value::Int128(-2i128),
                Value::Int128(0i128),
                Value::Int128(7i128),
            ],
        ),
        (
            "Nat128",
            vec![
                Value::Nat128(0u128),
                Value::Nat128(1u128),
                Value::Nat128(7u128),
            ],
        ),
        (
            "Decimal",
            vec![
                Value::Decimal(Decimal::new(-11, 1)),
                Value::Decimal(Decimal::new(-10, 1)),
                Value::Decimal(Decimal::new(0, 0)),
                Value::Decimal(Decimal::new(10, 1)),
                Value::Decimal(Decimal::new(11, 1)),
            ],
        ),
        (
            "Float32",
            vec![
                Value::Float32(Float32::try_new(-1.0).expect("finite")),
                Value::Float32(Float32::try_new(-0.0).expect("finite")),
                Value::Float32(Float32::try_new(0.0).expect("finite")),
                Value::Float32(Float32::try_new(1.0).expect("finite")),
            ],
        ),
        (
            "Float64",
            vec![
                Value::Float64(Float64::try_new(-1.0).expect("finite")),
                Value::Float64(Float64::try_new(-0.0).expect("finite")),
                Value::Float64(Float64::try_new(0.0).expect("finite")),
                Value::Float64(Float64::try_new(1.0).expect("finite")),
            ],
        ),
        (
            "Text",
            vec![
                Value::Text("a".to_string()),
                Value::Text("aa".to_string()),
                Value::Text("b".to_string()),
            ],
        ),
        (
            "Ulid",
            vec![
                Value::Ulid(Ulid::from_u128(1)),
                Value::Ulid(Ulid::from_u128(2)),
                Value::Ulid(Ulid::from_u128(3)),
            ],
        ),
        (
            "U256",
            vec![
                Value::U256(U256::ZERO),
                Value::U256(U256::ONE),
                Value::U256(U256::MAX),
            ],
        ),
        ("Unit", vec![Value::Unit, Value::Unit]),
        (
            "Timestamp",
            [i64::MIN, -1, 0, 1, i64::MAX]
                .map(|millis| Value::Timestamp(Timestamp::from_millis(millis)))
                .to_vec(),
        ),
    ];

    for (family_name, values) in families {
        for left in &values {
            for right in &values {
                let value_cmp = Value::canonical_cmp_key(left, right);
                let component_cmp = compare_index_component_values(left, right);
                let left_bytes =
                    encode_canonical_index_component(left).expect("left should encode");
                let right_bytes =
                    encode_canonical_index_component(right).expect("right should encode");
                let byte_cmp = left_bytes.cmp(&right_bytes);

                assert_eq!(
                    value_cmp, byte_cmp,
                    "encoded-byte ordering mismatch for family {family_name}: left={left:?} right={right:?}",
                );
                assert_eq!(
                    component_cmp, byte_cmp,
                    "index-component comparator drift for family {family_name}: left={left:?} right={right:?}",
                );
            }
        }
    }
}

#[test]
fn canonical_encoder_decimal_negative_domain_matrix() {
    let cases = vec![
        (Decimal::new(-2, 1), Decimal::new(-21, 2), Ordering::Greater),
        (Decimal::new(-21, 2), Decimal::new(-2001, 4), Ordering::Less),
        (Decimal::new(-1, 0), Decimal::new(-9, 1), Ordering::Less),
        (Decimal::new(-11, 1), Decimal::new(-1, 0), Ordering::Less),
        (Decimal::new(-10, 0), Decimal::new(-2, 0), Ordering::Less),
        (Decimal::new(-100, 2), Decimal::new(-1, 0), Ordering::Equal),
    ];

    for (left, right, expected) in cases {
        let left_value = Value::Decimal(left);
        let right_value = Value::Decimal(right);
        let left_bytes = encode_canonical_index_component(&left_value).expect("left should encode");
        let right_bytes =
            encode_canonical_index_component(&right_value).expect("right should encode");

        assert_eq!(
            left.cmp(&right),
            expected,
            "numeric matrix expectation drifted: left={left:?}, right={right:?}"
        );
        assert_eq!(
            left_bytes.cmp(&right_bytes),
            expected,
            "decimal encoded ordering mismatch: left={left:?}, right={right:?}"
        );
    }
}

#[test]
fn canonical_encoder_decimal_small_domain_matches_numeric_order() {
    let mut decimals = Vec::new();
    for mantissa in -200i64..=200 {
        for scale in 0u32..=8 {
            decimals.push(Decimal::new(mantissa, scale));
        }
    }

    for left in &decimals {
        for right in &decimals {
            let left_bytes = encode_canonical_index_component(&Value::Decimal(*left))
                .expect("left should encode");
            let right_bytes = encode_canonical_index_component(&Value::Decimal(*right))
                .expect("right should encode");
            assert_eq!(
                left.cmp(right),
                left_bytes.cmp(&right_bytes),
                "small-domain ordering mismatch: left={left:?}, right={right:?}"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn u256_ordered_encoding_matches_numeric_order_property(
        lhs_bytes in any::<[u8; 32]>(),
        rhs_bytes in any::<[u8; 32]>(),
    ) {
        let lhs = U256::from_be_bytes(lhs_bytes);
        let rhs = U256::from_be_bytes(rhs_bytes);
        let lhs_key = encode_canonical_index_component(&Value::U256(lhs)).expect("lhs should encode");
        let rhs_key = encode_canonical_index_component(&Value::U256(rhs)).expect("rhs should encode");

        prop_assert_eq!(lhs.cmp(&rhs), lhs_key.cmp(&rhs_key));
    }

    #[test]
    fn decimal_ordered_encoding_matches_numeric_order_property(
        lhs_mantissa in any::<i128>(),
        rhs_mantissa in any::<i128>(),
        lhs_scale in 0u32..=28,
        rhs_scale in 0u32..=28,
    ) {
        let lhs = Decimal::from_i128_with_scale(lhs_mantissa, lhs_scale);
        let rhs = Decimal::from_i128_with_scale(rhs_mantissa, rhs_scale);

        let lhs_value = Value::Decimal(lhs);
        let rhs_value = Value::Decimal(rhs);
        let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
        let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

        prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
    }

    #[test]
    fn decimal_ordered_encoding_negative_domain_property(
        lhs_mantissa in -i128::MAX..=-1i128,
        rhs_mantissa in -i128::MAX..=-1i128,
        lhs_scale in 0u32..=28,
        rhs_scale in 0u32..=28,
    ) {
        let lhs = Decimal::from_i128_with_scale(lhs_mantissa, lhs_scale);
        let rhs = Decimal::from_i128_with_scale(rhs_mantissa, rhs_scale);

        let lhs_value = Value::Decimal(lhs);
        let rhs_value = Value::Decimal(rhs);
        let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
        let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

        prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
    }
}
