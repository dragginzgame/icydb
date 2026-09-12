//! Bigint bytes and exact sizes are checked against Candid's independent encoder.

use crate::{IntBig, NatBig};
use num_bigint::{BigInt, BigUint};

#[test]
fn bigint_literal_admission_matches_exact_encoded_boundaries() {
    use crate::{ScalarLiteral, ScalarType};

    for integer in [-8193_i32, -8192, -65, -64, -1, 0, 63, 64, 8191, 8192] {
        let signed = IntBig::from(integer);
        let unsigned = NatBig::from(integer.unsigned_abs());
        for (length, literal) in [
            (
                u32::try_from(signed.to_leb128().len()).unwrap(),
                ScalarLiteral::IntBig(signed),
            ),
            (
                u32::try_from(unsigned.to_leb128().len()).unwrap(),
                ScalarLiteral::NatBig(unsigned),
            ),
        ] {
            for max_bytes in [length - 1, length, length + 1] {
                let kind = match literal {
                    ScalarLiteral::IntBig(_) => ScalarType::IntBig { max_bytes },
                    _ => ScalarType::NatBig { max_bytes },
                };
                assert_eq!(kind.accepts_literal(&literal), max_bytes >= length);
            }
        }
    }
}

#[test]
fn bigint_proposal_literal_cap_preserves_signed_and_unsigned_boundaries() {
    use crate::{MAX_PROPOSAL_LITERAL_BYTES, ScalarLiteral, SchemaContractError};

    // A positive signed integer needs a sign bit; the negative power of two
    // at the same boundary fits without another group. Test both sides.
    let bit = MAX_PROPOSAL_LITERAL_BYTES * 7 - 1;
    let power = BigInt::from(1_u8) << bit;
    for integer in [&power - 1_u8, power.clone(), -power] {
        let value = IntBig::from_bigint(integer);
        let fits = value.to_leb128().len() <= MAX_PROPOSAL_LITERAL_BYTES;
        assert_eq!(
            ScalarLiteral::IntBig(value).validate(),
            if fits {
                Ok(())
            } else {
                Err(SchemaContractError::InvalidLiteral)
            }
        );
    }
    let power = BigUint::from(1_u8) << (MAX_PROPOSAL_LITERAL_BYTES * 7);
    for integer in [&power - 1_u8, power] {
        let value = NatBig::from_biguint(integer);
        let fits = value.to_leb128().len() <= MAX_PROPOSAL_LITERAL_BYTES;
        assert_eq!(
            ScalarLiteral::NatBig(value).validate(),
            if fits {
                Ok(())
            } else {
                Err(SchemaContractError::InvalidLiteral)
            }
        );
    }
}

#[test]
fn signed_leb128_lengths_match_dense_small_values() {
    for integer in -20_000_i64..=20_000 {
        let value = IntBig::from(integer);
        let mut encoded = Vec::new();
        candid::Int::from(integer).encode(&mut encoded).unwrap();
        assert_eq!(value.to_leb128(), encoded);
        assert_eq!(value.leb128_len(), encoded.len() as u64, "{integer}");
    }
}

#[test]
fn signed_leb128_lengths_match_wide_sign_and_limb_boundaries() {
    for bit in 0_usize..=2048 {
        let power = BigInt::from(1_u8) << bit;
        for delta in [-1_i32, 0, 1] {
            let magnitude = &power + BigInt::from(delta);
            for integer in [magnitude.clone(), -magnitude] {
                let mut encoded = Vec::new();
                candid::Int::from(integer.clone())
                    .encode(&mut encoded)
                    .unwrap();
                let value = IntBig::from_bigint(integer);
                assert_eq!(value.to_leb128(), encoded);
                assert_eq!(
                    value.leb128_len(),
                    encoded.len() as u64,
                    "bit={bit}, delta={delta}, value={value}"
                );
            }
        }
    }
}

#[test]
fn unsigned_leb128_lengths_match_zero_and_wide_boundaries() {
    let zero = NatBig::default();
    assert_eq!(zero.leb128_len(), 1);
    assert_eq!(zero.to_leb128(), [0]);
    for bit in 0_usize..=2048 {
        let power = BigUint::from(1_u8) << bit;
        for integer in [&power - 1_u8, power.clone(), &power + 1_u8] {
            let mut encoded = Vec::new();
            candid::Nat::from(integer.clone())
                .encode(&mut encoded)
                .unwrap();
            let value = NatBig::from_biguint(integer);
            assert_eq!(value.to_leb128(), encoded);
            assert_eq!(
                value.leb128_len(),
                encoded.len() as u64,
                "bit={bit}, value={value}"
            );
        }
    }
}

#[test]
fn bigint_leb128_streams_mixed_limbs_and_large_values() {
    // Deterministic mixed limbs exercise carry propagation beyond powers of two.
    let mut seed = 0x1234_5678_u32;
    let limbs: Vec<_> = (0..4096)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed
        })
        .collect();
    for count in [1, 2, 3, 7, 15, 64, 4096] {
        let magnitude = BigUint::new(limbs[..count].to_vec());
        let unsigned = NatBig::from_biguint(magnitude.clone());
        let mut expected = Vec::new();
        candid::Nat::from(magnitude.clone())
            .encode(&mut expected)
            .unwrap();
        assert!(unsigned.leb128_bytes().eq(expected));
        for integer in [BigInt::from(magnitude.clone()), -BigInt::from(magnitude)] {
            let mut expected = Vec::new();
            candid::Int::from(integer.clone())
                .encode(&mut expected)
                .unwrap();
            let signed = IntBig::from_bigint(integer);
            let mut bytes = signed.leb128_bytes();
            assert!(bytes.by_ref().eq(expected));
            assert_eq!(bytes.next(), None);
            assert_eq!(bytes.next(), None);
        }
    }
}

// Freeze complete current messages, including their type tables and vector framing.
fn assert_candid_sizes<T>(value: T, one_bytes: usize, batch_bytes: usize)
where
    T: candid::CandidType + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Debug,
{
    let encoded = candid::encode_one(&value).expect("the integer should encode");
    assert_eq!(encoded.len(), one_bytes);
    assert_eq!(
        candid::decode_one::<T>(&encoded).expect("the integer should decode"),
        value,
    );
    let values = vec![value; 1_000];
    let encoded = candid::encode_one(&values).expect("the integer batch should encode");
    assert_eq!(encoded.len(), batch_bytes);
    assert_eq!(
        candid::decode_one::<Vec<T>>(&encoded).expect("the integer batch should decode"),
        values,
    );
}

#[test]
fn public_128_bit_integers_use_compact_native_candid_values() {
    use candid::CandidType;

    assert_eq!(<u128 as candid::CandidType>::ty(), candid::Nat::ty());
    assert_eq!(<i128 as candid::CandidType>::ty(), candid::Int::ty());
    for (value, one, batch) in [(0_u128, 8, 1_011), (128, 9, 2_011), (u128::MAX, 26, 19_011)] {
        assert_candid_sizes(value, one, batch);
    }
    for (value, one, batch) in [
        (0_i128, 8, 1_011),
        (-64, 8, 1_011),
        (64, 9, 2_011),
        (-65, 9, 2_011),
        (i128::MIN, 26, 19_011),
        (i128::MAX, 26, 19_011),
    ] {
        assert_candid_sizes(value, one, batch);
    }
}

#[test]
fn public_bigints_and_u256_share_native_candid_integer_sizes() {
    use candid::CandidType;

    assert_eq!(NatBig::ty(), candid::Nat::ty());
    assert_eq!(IntBig::ty(), candid::Int::ty());
    assert_eq!(crate::U256::ty(), candid::Nat::ty());
    for (bits, one, batch) in [
        (0_usize, 8, 1_011),
        (1, 8, 1_011),
        (64, 17, 10_011),
        (128, 26, 19_011),
        (256, 44, 37_011),
        (1024, 154, 147_011),
    ] {
        let magnitude = (BigUint::from(1_u8) << bits) - 1_u8;
        assert_candid_sizes(NatBig::from_biguint(magnitude.clone()), one, batch);
        let signed = BigInt::from(magnitude.clone());
        assert_candid_sizes(IntBig::from_bigint(signed.clone()), one, batch);
        assert_candid_sizes(IntBig::from_bigint(-signed), one, batch);
        if bits <= 256 {
            let value = magnitude
                .to_string()
                .parse::<crate::U256>()
                .expect("the magnitude should fit U256");
            assert_candid_sizes(value, one, batch);
        }
    }
}

fn cbor_bytes<T: serde::Serialize + ?Sized>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes).expect("the value should encode as CBOR");
    bytes
}

fn assert_integer_serde_roundtrip<T>(value: T, text: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = cbor_bytes(&value);
    assert_eq!(
        ciborium::de::from_reader::<T, _>(bytes.as_slice()).expect("CBOR should round-trip"),
        value,
    );
    let json = serde_json::to_string(&value).expect("JSON should encode");
    assert_eq!(
        json,
        serde_json::to_string(text).expect("text should encode")
    );
    assert_eq!(
        serde_json::from_str::<T>(&json).expect("JSON should round-trip"),
        value
    );
}

#[test]
fn integer_serde_roundtrips_native_and_wide_boundaries() {
    for bits in [
        0_usize, 1, 7, 8, 31, 32, 63, 64, 65, 95, 96, 127, 128, 255, 256, 257, 1024,
    ] {
        let power = BigUint::from(1_u8) << bits;
        for magnitude in [&power - 1_u8, power.clone(), &power + 1_u8] {
            assert_integer_serde_roundtrip(
                NatBig::from_biguint(magnitude.clone()),
                &magnitude.to_string(),
            );
            if magnitude.bits() <= 256 {
                let value = magnitude
                    .to_string()
                    .parse::<crate::U256>()
                    .expect("U256 should fit");
                assert_integer_serde_roundtrip(value, &magnitude.to_string());
            }
            let integer = BigInt::from(magnitude);
            for integer in [integer.clone(), -integer] {
                assert_integer_serde_roundtrip(
                    IntBig::from_bigint(integer.clone()),
                    &integer.to_string(),
                );
            }
        }
    }
}

#[test]
fn tagged_integer_bodies_match_independent_bigint_byte_encoders() {
    for bits in 0_usize..=512 {
        let magnitude = BigUint::from(1_u8) << bits;
        for magnitude in [&magnitude - 1_u8, magnitude.clone(), &magnitude + 1_u8] {
            let unsigned = NatBig::from_biguint(magnitude.clone());
            let mut expected = vec![1];
            expected.extend_from_slice(&magnitude.to_bytes_le());
            assert_eq!(
                crate::integer_wire::unsigned_bytes(unsigned.u32_digits()),
                expected
            );
            let integer = BigInt::from(magnitude);
            for integer in [integer.clone(), -integer] {
                let signed = IntBig::from_bigint(integer.clone());
                let (negative, limbs) = signed.sign_and_u32_digits();
                let mut expected = vec![0];
                expected.extend_from_slice(&integer.to_signed_bytes_le());
                assert_eq!(crate::integer_wire::signed_bytes(negative, limbs), expected);
            }
        }
    }
}

#[test]
fn binary_integer_ingress_rejects_malformed_current_bodies() {
    for body in [vec![], vec![1], vec![2, 1], vec![1, 1, 0]] {
        let encoded = cbor_bytes(serde_bytes::Bytes::new(&body));
        assert!(ciborium::de::from_reader::<NatBig, _>(encoded.as_slice()).is_err());
        assert!(ciborium::de::from_reader::<crate::U256, _>(encoded.as_slice()).is_err());
    }
    for body in [
        vec![],
        vec![0],
        vec![2, 1],
        vec![0, 0, 0],
        vec![0, 0xff, 0xff],
    ] {
        let encoded = cbor_bytes(serde_bytes::Bytes::new(&body));
        assert!(ciborium::de::from_reader::<IntBig, _>(encoded.as_slice()).is_err());
    }
    let negative = cbor_bytes(&-1_i64);
    assert!(ciborium::de::from_reader::<NatBig, _>(negative.as_slice()).is_err());
    assert!(ciborium::de::from_reader::<crate::U256, _>(negative.as_slice()).is_err());
    for len in [31, 32, 33] {
        let mut body = vec![1];
        body.extend(std::iter::repeat_n(0xff, len));
        let encoded = cbor_bytes(serde_bytes::Bytes::new(&body));
        let decoded = ciborium::de::from_reader::<crate::U256, _>(encoded.as_slice());
        assert_eq!(decoded.is_ok(), len <= 32);
        for prefix in 0..encoded.len() {
            assert!(ciborium::de::from_reader::<NatBig, _>(&encoded[..prefix]).is_err());
        }
    }
}

#[test]
fn native_candid_naturals_still_widen_to_signed_bigints() {
    for bits in [0_usize, 63, 64, 65, 128, 256] {
        let value = BigUint::from(1_u8) << bits;
        let encoded = candid::encode_one(candid::Nat(value.clone())).expect("Nat should encode");
        assert_eq!(
            candid::decode_one::<IntBig>(&encoded).expect("Nat should widen to IntBig"),
            IntBig::from_bigint(BigInt::from(value)),
        );
    }
}

fn assert_cbor_integer_size<T>(value: T, expected_size: usize)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Debug,
{
    assert_eq!(cbor_bytes(&value).len(), expected_size);
    let values = vec![value; 1_000];
    let bytes = cbor_bytes(&values);
    assert_eq!(bytes.len(), 3 + 1_000 * expected_size);
    assert_eq!(
        ciborium::de::from_reader::<Vec<T>, _>(bytes.as_slice())
            .expect("the batch should round-trip"),
        values,
    );
}

#[test]
fn integer_binary_serde_sizes_cover_dense_and_sparse_batches() {
    assert_cbor_integer_size(NatBig::from(0_u64), 1);
    assert_cbor_integer_size(IntBig::from(0_i64), 1);
    assert_cbor_integer_size(IntBig::from(-1_i64), 1);
    assert_cbor_integer_size(crate::U256::ZERO, 1);
    assert_cbor_integer_size(NatBig::from(u64::MAX), 9);
    for (bits, unsigned_size, signed_size, sparse_size) in [
        (128_usize, 18, 19, 19),
        (256, 35, 36, 36),
        (1024, 131, 132, 132),
    ] {
        let power = BigUint::from(1_u8) << bits;
        let magnitude = &power - 1_u8;
        assert_cbor_integer_size(NatBig::from_biguint(magnitude.clone()), unsigned_size);
        assert_cbor_integer_size(NatBig::from_biguint(power), sparse_size);
        for signed in [BigInt::from(magnitude.clone()), -BigInt::from(magnitude)] {
            assert_cbor_integer_size(IntBig::from_bigint(signed), signed_size);
        }
    }
    assert_cbor_integer_size(crate::U256::MAX, 35);
}
