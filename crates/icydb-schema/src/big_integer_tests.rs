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
