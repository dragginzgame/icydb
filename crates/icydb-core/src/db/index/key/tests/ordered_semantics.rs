use crate::{
    db::index::key::{OrderedValueEncodeError, ordered::encode_canonical_index_component},
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use proptest::prelude::*;
use std::cmp::Ordering;

const NEGATIVE_MARKER: u8 = 0x00;
const ZERO_MARKER: u8 = 0x01;
const POSITIVE_MARKER: u8 = 0x02;

fn assert_encoded_order(left: Value, right: Value, expected: Ordering) {
    let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
    let right_bytes = encode_canonical_index_component(&right).expect("right should encode");

    assert_eq!(left_bytes.cmp(&right_bytes), expected);
}

fn normalize_unsigned_decimal_text(raw: &str) -> String {
    let trimmed = raw.trim_start_matches('0');
    if trimmed.is_empty() {
        return "0".to_string();
    }

    trimmed.to_string()
}

fn normalize_signed_decimal_text(raw: &str) -> String {
    if let Some(unsigned) = raw.strip_prefix('-') {
        let digits = normalize_unsigned_decimal_text(unsigned);
        if digits == "0" {
            return digits;
        }

        return format!("-{digits}");
    }

    normalize_unsigned_decimal_text(raw)
}

fn decode_int_big_payload(encoded: &[u8]) -> (u8, Vec<u8>) {
    assert!(
        encoded.len() >= 2,
        "int-big payload must include tag + marker: {encoded:?}"
    );
    assert_eq!(
        encoded[0],
        Value::IntBig(Int::from(0)).canonical_tag().to_u8()
    );

    let marker = encoded[1];
    match marker {
        ZERO_MARKER => {
            assert_eq!(
                encoded.len(),
                2,
                "zero int-big payload must be exactly two bytes: {encoded:?}"
            );
            (marker, vec![b'0'])
        }
        POSITIVE_MARKER => {
            assert!(
                encoded.len() >= 4,
                "positive int-big payload must include length: {encoded:?}"
            );
            let len = usize::from(u16::from_be_bytes([encoded[2], encoded[3]]));
            let digits = encoded[4..].to_vec();
            assert_eq!(len, digits.len(), "int-big length mismatch");
            (marker, digits)
        }
        NEGATIVE_MARKER => {
            assert!(
                encoded.len() >= 4,
                "negative int-big payload must include length: {encoded:?}"
            );
            let len = usize::from(u16::from_be_bytes([!encoded[2], !encoded[3]]));
            let digits: Vec<u8> = encoded[4..].iter().map(|byte| !byte).collect();
            assert_eq!(len, digits.len(), "int-big length mismatch");
            (marker, digits)
        }
        other => panic!("unexpected int-big marker {other:#x}"),
    }
}

fn decode_uint_big_payload(encoded: &[u8]) -> Vec<u8> {
    assert!(
        encoded.len() >= 3,
        "uint-big payload must include tag + length: {encoded:?}"
    );
    assert_eq!(
        encoded[0],
        Value::UintBig(Nat::from(0u64)).canonical_tag().to_u8()
    );

    let len = usize::from(u16::from_be_bytes([encoded[1], encoded[2]]));
    let digits = encoded[3..].to_vec();
    assert_eq!(len, digits.len(), "uint-big length mismatch");
    digits
}

fn decode_int_big_from_ordered_bytes(encoded: &[u8]) -> Int {
    let (marker, digits) = decode_int_big_payload(encoded);
    let digits_text = String::from_utf8(digits).expect("int-big digits must be utf8");

    let text = match marker {
        ZERO_MARKER | POSITIVE_MARKER => digits_text,
        NEGATIVE_MARKER => format!("-{digits_text}"),
        _ => unreachable!("unexpected int-big marker"),
    };

    text.parse().expect("decoded int-big text should parse")
}

fn decode_uint_big_from_ordered_bytes(encoded: &[u8]) -> Nat {
    let digits = decode_uint_big_payload(encoded);
    let digits_text = String::from_utf8(digits).expect("uint-big digits must be utf8");
    digits_text
        .parse()
        .expect("decoded uint-big text should parse")
}

#[test]
fn canonical_encoder_rejects_non_indexable_and_unsupported_values() {
    assert!(encode_canonical_index_component(&Value::Null).is_err());
    assert!(encode_canonical_index_component(&Value::Blob(vec![1u8, 2u8])).is_err());
    assert!(encode_canonical_index_component(&Value::List(vec![Value::Int(1)])).is_err());
    assert!(encode_canonical_index_component(&Value::Map(vec![])).is_err());
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn canonical_encoder_account_payload_uses_exact_owner_length_tag() {
    let account = Account::new(Principal::max_storable(), None::<Subaccount>);
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
fn canonical_encoder_respects_numeric_order_for_scalars() {
    assert_encoded_order(Value::Int(-2), Value::Int(7), Ordering::Less);
    assert_encoded_order(
        Value::Int128(Int128::from(-2i128)),
        Value::Int128(Int128::from(7i128)),
        Ordering::Less,
    );
    assert_encoded_order(Value::Uint(2), Value::Uint(7), Ordering::Less);
    assert_encoded_order(
        Value::Uint128(Nat128::from(2u128)),
        Value::Uint128(Nat128::from(7u128)),
        Ordering::Less,
    );
    assert_encoded_order(
        Value::IntBig(Int::from(-20i32)),
        Value::IntBig(Int::from(-7i32)),
        Ordering::Less,
    );
    assert_encoded_order(
        Value::UintBig(Nat::from(20u64)),
        Value::UintBig(Nat::from(700u64)),
        Ordering::Less,
    );
    assert_encoded_order(
        Value::IntBig(Int::from(-1i32)),
        Value::IntBig(Int::from(0i32)),
        Ordering::Less,
    );
    assert_encoded_order(
        Value::IntBig(Int::from(0i32)),
        Value::IntBig(Int::from(1i32)),
        Ordering::Less,
    );
}

#[test]
fn canonical_encoder_bigint_same_value_same_bytes_across_construction_paths() {
    let int_cases = vec![
        (
            Value::IntBig(Int::from(70i32)),
            Value::IntBig("00070".parse().expect("int literal")),
        ),
        (
            Value::IntBig(Int::from(-70i32)),
            Value::IntBig("-00070".parse().expect("int literal")),
        ),
        (
            Value::IntBig(Int::from(0i32)),
            Value::IntBig("-0".parse().expect("int literal")),
        ),
        (
            Value::IntBig("4294967296".parse().expect("int literal")),
            Value::IntBig("00004294967296".parse().expect("int literal")),
        ),
    ];

    for (left, right) in int_cases {
        let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
        let right_bytes = encode_canonical_index_component(&right).expect("right should encode");
        assert_eq!(left_bytes, right_bytes, "int-big canonical bytes diverged");
    }

    let uint_cases = vec![
        (
            Value::UintBig(Nat::from(70u64)),
            Value::UintBig("00070".parse().expect("nat literal")),
        ),
        (
            Value::UintBig("4294967296".parse().expect("nat literal")),
            Value::UintBig("00004294967296".parse().expect("nat literal")),
        ),
        (
            Value::UintBig("18446744073709551616".parse().expect("nat literal")),
            Value::UintBig("000018446744073709551616".parse().expect("nat literal")),
        ),
    ];

    for (left, right) in uint_cases {
        let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
        let right_bytes = encode_canonical_index_component(&right).expect("right should encode");
        assert_eq!(left_bytes, right_bytes, "uint-big canonical bytes diverged");
    }
}

#[test]
fn canonical_encoder_int_big_negative_zero_collapses_to_zero_marker() {
    let zero = Value::IntBig(Int::from(0i32));
    let negative_zero = Value::IntBig("-0".parse().expect("int literal"));

    let zero_bytes = encode_canonical_index_component(&zero).expect("zero should encode");
    let negative_zero_bytes =
        encode_canonical_index_component(&negative_zero).expect("negative zero should encode");

    assert_eq!(zero_bytes, negative_zero_bytes);
    assert_eq!(zero_bytes, vec![zero.canonical_tag().to_u8(), ZERO_MARKER]);
}

#[test]
fn canonical_encoder_bigint_payload_uses_minimal_digits() {
    let int_literals = vec![
        "-000123456789",
        "-18446744073709551616",
        "0",
        "0004294967296",
        "340282366920938463463374607431768211455",
    ];

    for literal in int_literals {
        let value = Value::IntBig(literal.parse().expect("int literal"));
        let encoded = encode_canonical_index_component(&value).expect("int-big should encode");
        let (_, digits) = decode_int_big_payload(&encoded);
        assert!(digits.iter().all(u8::is_ascii_digit));

        let expected = normalize_signed_decimal_text(literal);
        let expected_digits = expected.strip_prefix('-').unwrap_or(&expected);
        assert_eq!(digits, expected_digits.as_bytes());

        if digits != b"0" {
            assert_ne!(digits[0], b'0', "int-big payload must not lead with zero");
        }
    }

    let uint_literals = vec![
        "0",
        "000123456789",
        "4294967296",
        "00018446744073709551616",
        "340282366920938463463374607431768211455",
    ];

    for literal in uint_literals {
        let value = Value::UintBig(literal.parse().expect("nat literal"));
        let encoded = encode_canonical_index_component(&value).expect("uint-big should encode");
        let digits = decode_uint_big_payload(&encoded);
        assert!(digits.iter().all(u8::is_ascii_digit));

        let expected = normalize_unsigned_decimal_text(literal);
        assert_eq!(digits, expected.as_bytes());

        if digits != b"0" {
            assert_ne!(digits[0], b'0', "uint-big payload must not lead with zero");
        }
    }
}

#[test]
fn canonical_encoder_bigint_limb_boundary_ordering_is_monotonic() {
    let one_limb_max = "4294967295";
    let two_limb_min = "4294967296";
    let two_limb_next = "4294967297";

    assert_encoded_order(
        Value::UintBig(one_limb_max.parse().expect("nat literal")),
        Value::UintBig(two_limb_min.parse().expect("nat literal")),
        Ordering::Less,
    );
    assert_encoded_order(
        Value::UintBig(two_limb_min.parse().expect("nat literal")),
        Value::UintBig(two_limb_next.parse().expect("nat literal")),
        Ordering::Less,
    );

    assert_encoded_order(
        Value::IntBig(format!("-{two_limb_next}").parse().expect("int literal")),
        Value::IntBig(format!("-{two_limb_min}").parse().expect("int literal")),
        Ordering::Less,
    );
    assert_encoded_order(
        Value::IntBig(format!("-{two_limb_min}").parse().expect("int literal")),
        Value::IntBig(format!("-{one_limb_max}").parse().expect("int literal")),
        Ordering::Less,
    );
}

#[test]
fn canonical_encoder_bigint_roundtrip_stable_for_valid_bytes() {
    let int_literals = vec![
        "-18446744073709551616",
        "-1",
        "0",
        "1",
        "340282366920938463463374607431768211455",
    ];

    for literal in int_literals {
        let value: Int = literal.parse().expect("int literal");
        let encoded =
            encode_canonical_index_component(&Value::IntBig(value.clone())).expect("encode");
        let decoded = decode_int_big_from_ordered_bytes(&encoded);
        assert_eq!(decoded, value, "int-big decode(encode(x)) mismatch");

        let reencoded =
            encode_canonical_index_component(&Value::IntBig(decoded)).expect("reencode");
        assert_eq!(reencoded, encoded, "int-big encode(decode(bytes)) mismatch");
    }

    let uint_literals = vec![
        "0",
        "1",
        "4294967296",
        "18446744073709551616",
        "340282366920938463463374607431768211455",
    ];

    for literal in uint_literals {
        let value: Nat = literal.parse().expect("nat literal");
        let encoded =
            encode_canonical_index_component(&Value::UintBig(value.clone())).expect("encode");
        let decoded = decode_uint_big_from_ordered_bytes(&encoded);
        assert_eq!(decoded, value, "uint-big decode(encode(x)) mismatch");

        let reencoded =
            encode_canonical_index_component(&Value::UintBig(decoded)).expect("reencode");
        assert_eq!(
            reencoded, encoded,
            "uint-big encode(decode(bytes)) mismatch"
        );
    }
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
fn canonical_encoder_rejects_decimal_scale_above_max() {
    let over_scale = Decimal::max_supported_scale().saturating_add(1);
    let value = Value::Decimal(Decimal::new_unchecked(1, over_scale));

    let err = encode_canonical_index_component(&value)
        .expect_err("unordered decimal scale should fail ordered encoding");
    assert!(matches!(
        err,
        OrderedValueEncodeError::DecimalExponentOverflow
    ));
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
fn canonical_encoder_respects_enum_order() {
    let left = Value::Enum(ValueEnum::new("A", Some("Path")));
    let right = Value::Enum(ValueEnum::new("B", Some("Path")));
    assert_encoded_order(left, right, Ordering::Less);

    let no_payload = Value::Enum(ValueEnum::new("A", Some("Path")));
    let with_payload = Value::Enum(ValueEnum::new("A", Some("Path")).with_payload(Value::Uint(1)));
    assert_encoded_order(no_payload, with_payload, Ordering::Less);
}

#[test]
fn canonical_encoder_golden_vectors_freeze_primitive_bytes() {
    let cases: Vec<(&str, Value, Vec<u8>)> = vec![
        ("Bool(false)", Value::Bool(false), vec![0x03, 0x00]),
        ("Bool(true)", Value::Bool(true), vec![0x03, 0x01]),
        (
            "Int(-1)",
            Value::Int(-1),
            vec![0x0A, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        ),
        (
            "Uint(1)",
            Value::Uint(1),
            vec![0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
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
            vec![0x12, b'a', 0x00, 0x00],
        ),
        (
            "Principal([1,0,2])",
            Value::Principal(Principal::from_slice(&[1u8, 0u8, 2u8])),
            vec![0x10, 0x01, 0x00, 0xFF, 0x02, 0x00, 0x00],
        ),
        (
            "IntBig(-7)",
            Value::IntBig(Int::from(-7i32)),
            vec![0x0C, 0x00, 0xFF, 0xFE, 0xC8],
        ),
        (
            "UintBig(70)",
            Value::UintBig(Nat::from(70u64)),
            vec![0x16, 0x00, 0x02, 0x37, 0x30],
        ),
        (
            "Enum(State::MyPath(7))",
            Value::Enum(ValueEnum::new("State", Some("MyPath")).with_payload(Value::Int(7))),
            vec![
                0x07, b'S', b't', b'a', b't', b'e', 0x00, 0x00, 0x01, b'M', b'y', b'P', b'a', b't',
                b'h', 0x00, 0x00, 0x01, 0x00, 0x09, 0x0A, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x07,
            ],
        ),
        (
            "Ulid(1)",
            Value::Ulid(Ulid::from_u128(1)),
            vec![
                0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01,
            ],
        ),
        ("Unit", Value::Unit, vec![0x18]),
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
        Value::Account(Account::dummy(1)),
        Value::Bool(false),
        Value::Bool(true),
        Value::Date(Date::new(2024, 1, 1)),
        Value::Decimal(Decimal::new(-11, 1)),
        Value::Decimal(Decimal::new(0, 0)),
        Value::Decimal(Decimal::new(11, 1)),
        Value::Duration(Duration::from_secs(1)),
        Value::Enum(ValueEnum::new("A", Some("E"))),
        Value::Float32(Float32::try_new(-1.0).expect("finite")),
        Value::Float32(Float32::try_new(1.0).expect("finite")),
        Value::Float64(Float64::try_new(-1.0).expect("finite")),
        Value::Float64(Float64::try_new(1.0).expect("finite")),
        Value::Int(-2),
        Value::Int(7),
        Value::Int128(Int128::from(-2i128)),
        Value::Int128(Int128::from(7i128)),
        Value::IntBig(Int::from(-7i32)),
        Value::IntBig(Int::from(7i32)),
        Value::Principal(Principal::from_slice(&[1u8])),
        Value::Principal(Principal::from_slice(&[2u8])),
        Value::Subaccount(Subaccount::dummy(1)),
        Value::Subaccount(Subaccount::dummy(2)),
        Value::Text("a".to_string()),
        Value::Text("b".to_string()),
        Value::Timestamp(Timestamp::from_secs(1)),
        Value::Timestamp(Timestamp::from_secs(2)),
        Value::Uint(1),
        Value::Uint(2),
        Value::Uint128(Nat128::from(1u128)),
        Value::Uint128(Nat128::from(2u128)),
        Value::UintBig(Nat::from(1u64)),
        Value::UintBig(Nat::from(2u64)),
        Value::Ulid(Ulid::from_u128(1)),
        Value::Ulid(Ulid::from_u128(2)),
        Value::Unit,
    ];

    let mut by_value = samples.clone();
    by_value.sort_by(Value::canonical_cmp_key);

    let mut by_bytes = samples;
    by_bytes.sort_by(|left, right| {
        let left_bytes = encode_canonical_index_component(left).expect("left should encode");
        let right_bytes = encode_canonical_index_component(right).expect("right should encode");
        left_bytes.cmp(&right_bytes)
    });

    assert_eq!(by_value, by_bytes);
}

#[test]
fn timestamp_ordered_encoding_is_monotonic_for_millisecond_values() {
    let t1000 = Value::Timestamp(Timestamp::from_millis(1_000));
    let t1001 = Value::Timestamp(Timestamp::from_millis(1_001));

    let b1000 = encode_canonical_index_component(&t1000).expect("timestamp 1000 should encode");
    let b1001 = encode_canonical_index_component(&t1001).expect("timestamp 1001 should encode");

    assert!(b1001 > b1000);
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
            vec![Value::Int(-2), Value::Int(-1), Value::Int(0), Value::Int(7)],
        ),
        ("Uint", vec![Value::Uint(0), Value::Uint(1), Value::Uint(7)]),
        (
            "Int128",
            vec![
                Value::Int128(Int128::from(-2i128)),
                Value::Int128(Int128::from(0i128)),
                Value::Int128(Int128::from(7i128)),
            ],
        ),
        (
            "Uint128",
            vec![
                Value::Uint128(Nat128::from(0u128)),
                Value::Uint128(Nat128::from(1u128)),
                Value::Uint128(Nat128::from(7u128)),
            ],
        ),
        (
            "IntBig",
            vec![
                Value::IntBig(Int::from(-10i32)),
                Value::IntBig(Int::from(-1i32)),
                Value::IntBig(Int::from(0i32)),
                Value::IntBig(Int::from(7i32)),
            ],
        ),
        (
            "UintBig",
            vec![
                Value::UintBig(Nat::from(0u64)),
                Value::UintBig(Nat::from(1u64)),
                Value::UintBig(Nat::from(70u64)),
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
        ("Unit", vec![Value::Unit, Value::Unit]),
    ];

    for (family_name, values) in families {
        for left in &values {
            for right in &values {
                let value_cmp = Value::canonical_cmp_key(left, right);
                let left_bytes =
                    encode_canonical_index_component(left).expect("left should encode");
                let right_bytes =
                    encode_canonical_index_component(right).expect("right should encode");
                let byte_cmp = left_bytes.cmp(&right_bytes);

                assert_eq!(
                    value_cmp, byte_cmp,
                    "encoded-byte ordering mismatch for family {family_name}: left={left:?} right={right:?}",
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

fn normalize_decimal_text(raw: String) -> String {
    let trimmed = raw.trim_start_matches('0');
    if trimmed.is_empty() {
        return "0".to_string();
    }

    trimmed.to_string()
}

fn unsigned_decimal_text_strategy(max_len: usize) -> BoxedStrategy<String> {
    proptest::collection::vec(proptest::char::range('0', '9'), 1..=max_len)
        .prop_map(|digits: Vec<char>| {
            let raw: String = digits.into_iter().collect();
            normalize_decimal_text(raw)
        })
        .boxed()
}

fn non_zero_unsigned_decimal_text_strategy(max_len: usize) -> BoxedStrategy<String> {
    unsigned_decimal_text_strategy(max_len)
        .prop_filter("non-zero decimal", |digits| digits != "0")
        .boxed()
}

fn signed_decimal_text_strategy(max_len: usize) -> BoxedStrategy<String> {
    (any::<bool>(), unsigned_decimal_text_strategy(max_len))
        .prop_map(|(negative, digits)| {
            if negative && digits != "0" {
                format!("-{digits}")
            } else {
                digits
            }
        })
        .boxed()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn int_big_cross_sign_ordered_encoding_property(
        magnitude in non_zero_unsigned_decimal_text_strategy(256),
    ) {
        let negative: Int = format!("-{magnitude}").parse().expect("negative literal should parse");
        let positive: Int = magnitude.parse().expect("positive literal should parse");

        let negative_bytes = encode_canonical_index_component(&Value::IntBig(negative)).expect("negative should encode");
        let positive_bytes = encode_canonical_index_component(&Value::IntBig(positive)).expect("positive should encode");

        prop_assert!(negative_bytes < positive_bytes);
    }

    #[test]
    fn int_big_ordered_encoding_matches_numeric_order_property(
        lhs_text in signed_decimal_text_strategy(96),
        rhs_text in signed_decimal_text_strategy(96),
    ) {
        let lhs: Int = lhs_text.parse().expect("lhs int literal should parse");
        let rhs: Int = rhs_text.parse().expect("rhs int literal should parse");

        let lhs_value = Value::IntBig(lhs.clone());
        let rhs_value = Value::IntBig(rhs.clone());
        let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
        let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

        prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
    }

    #[test]
    fn uint_big_ordered_encoding_matches_numeric_order_property(
        lhs_text in unsigned_decimal_text_strategy(96),
        rhs_text in unsigned_decimal_text_strategy(96),
    ) {
        let lhs: Nat = lhs_text.parse().expect("lhs nat literal should parse");
        let rhs: Nat = rhs_text.parse().expect("rhs nat literal should parse");

        let lhs_value = Value::UintBig(lhs.clone());
        let rhs_value = Value::UintBig(rhs.clone());
        let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
        let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

        prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
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
