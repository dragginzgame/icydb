//! Binary magnitudes preserve numeric order, canonical zero and segment limits.

use super::*;
use crate::{db::index::key::ordered::encode_canonical_index_component, value::Value};
use num_bigint::{BigInt, BigUint, Sign};

#[test]
fn integer_components_match_binary_magnitudes_at_byte_and_limb_boundaries() {
    for bits in [0_usize, 1, 8, 9, 31, 32, 33, 64, 65, 256, 1024] {
        let magnitude = (BigUint::from(1_u8) << bits) - BigUint::from(1_u8);
        let bytes = if bits == 0 {
            vec![]
        } else {
            magnitude.to_bytes_be()
        };
        let length = u16::try_from(bytes.len()).unwrap().to_be_bytes();
        let unsigned = Value::NatBig(NatBig::from_biguint(magnitude.clone()));
        let mut expected = vec![unsigned.canonical_tag().to_u8()];
        expected.extend_from_slice(&length);
        expected.extend_from_slice(&bytes);
        assert_eq!(
            encode_canonical_index_component(&unsigned).unwrap(),
            expected
        );
        for negative in [false, true] {
            let sign = if negative { Sign::Minus } else { Sign::Plus };
            let signed = Value::IntBig(IntBig::from_bigint(BigInt::from_biguint(
                sign,
                magnitude.clone(),
            )));
            let mut expected = vec![signed.canonical_tag().to_u8()];
            if bits == 0 {
                expected.push(ZERO_MARKER);
            } else {
                expected.push(if negative {
                    NEGATIVE_MARKER
                } else {
                    POSITIVE_MARKER
                });
                expected.extend(
                    length
                        .iter()
                        .chain(&bytes)
                        .map(|&byte| if negative { !byte } else { byte }),
                );
            }
            assert_eq!(encode_canonical_index_component(&signed).unwrap(), expected);
        }
    }
}

#[test]
fn binary_magnitude_length_checks_precede_destination_growth() {
    for len in [u16::MAX as usize, u16::MAX as usize + 1] {
        let magnitude = BigUint::from(1_u8) << (len * 8 - 1);
        let value = NatBig::from_biguint(magnitude);
        for negative in [None, Some(false), Some(true)] {
            let mut out = vec![0x42];
            let result = push_big_integer_magnitude(&mut out, negative, value.u32_digits());
            if len > u16::MAX as usize {
                assert!(matches!(
                    result,
                    Err(OrderedValueEncodeError::SegmentTooLarge)
                ));
                assert_eq!(out, [0x42]);
            } else {
                result.unwrap();
                assert_eq!(out.len(), 1 + usize::from(negative.is_some()) + 2 + len);
            }
        }
    }
}

#[test]
fn binary_magnitude_order_crosses_byte_limb_and_length_prefix_boundaries() {
    for bits in [8_usize, 32, 64, 2040, 2048] {
        let middle = BigInt::from(1_u8) << bits;
        let lower = &middle - 1_u8;
        let upper = &middle + 1_u8;
        let ordered = [
            -&upper,
            -&middle,
            -&lower,
            BigInt::from(0_u8),
            lower,
            middle,
            upper,
        ];
        for pair in ordered.windows(2) {
            let left = encode_canonical_index_component(&Value::IntBig(IntBig::from_bigint(
                pair[0].clone(),
            )))
            .unwrap();
            let right = encode_canonical_index_component(&Value::IntBig(IntBig::from_bigint(
                pair[1].clone(),
            )))
            .unwrap();
            assert_eq!(left.cmp(&right), pair[0].cmp(&pair[1]));
        }
    }
}
