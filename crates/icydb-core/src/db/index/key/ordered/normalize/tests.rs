//! Integer chunk emission preserves canonical digits, signs and segment limits.

use super::*;
use crate::{db::index::key::ordered::encode_canonical_index_component, value::Value};

#[test]
fn integer_components_match_decimal_reference_at_chunk_boundaries() {
    for digits in [
        "0",
        "1",
        "9",
        "10",
        "999999999",
        "1000000000",
        "1000000001",
        "999999999999999999",
        "1000000000000000000",
        "1000000000000000001",
        "1000000000000000000000000001",
        "340282366920938463463374607431768211455",
    ] {
        let length = u16::try_from(digits.len()).unwrap().to_be_bytes();
        let unsigned = Value::NatBig(digits.parse().unwrap());
        let mut expected = vec![unsigned.canonical_tag().to_u8()];
        expected.extend_from_slice(&length);
        expected.extend_from_slice(digits.as_bytes());
        assert_eq!(
            encode_canonical_index_component(&unsigned).unwrap(),
            expected
        );

        for negative in [false, true] {
            let text = if negative {
                format!("-{digits}")
            } else {
                digits.to_string()
            };
            let signed = Value::IntBig(text.parse().unwrap());
            let mut expected = vec![signed.canonical_tag().to_u8()];
            if digits == "0" {
                expected.push(ZERO_MARKER);
            } else {
                expected.push(if negative {
                    NEGATIVE_MARKER
                } else {
                    POSITIVE_MARKER
                });
                expected.extend(
                    length.iter().chain(digits.as_bytes()).map(
                        |&byte| {
                            if negative { !byte } else { byte }
                        },
                    ),
                );
            }
            assert_eq!(encode_canonical_index_component(&signed).unwrap(), expected);
        }
    }
}

#[test]
fn chunk_digit_count_and_emission_agree_at_segment_limit() {
    // Exercise the existing u16 digit-length boundary directly, without a huge
    // binary-to-decimal conversion hiding which construction contract is tested.
    for len in [
        1_usize,
        8,
        9,
        10,
        18,
        19,
        u16::MAX as usize,
        u16::MAX as usize + 1,
    ] {
        let leading_width = (len - 1) % BIGINT_DECIMAL_CHUNK_WIDTH + 1;
        let mut chunks = vec![0; len.div_ceil(BIGINT_DECIMAL_CHUNK_WIDTH)];
        *chunks.last_mut().unwrap() = 10_u32.pow(u32::try_from(leading_width).unwrap() - 1);
        let actual = decimal_chunk_digit_count(&chunks).unwrap();
        assert_eq!(actual, len);
        if len > u16::MAX as usize {
            assert!(matches!(
                encode_segment_len(actual),
                Err(OrderedValueEncodeError::SegmentTooLarge)
            ));
            continue;
        }
        assert_eq!(
            u16::from_be_bytes(encode_segment_len(actual).unwrap()) as usize,
            len
        );
        for negative in [false, true] {
            let mut out = Vec::with_capacity(len);
            push_decimal_chunk_digits(&mut out, &chunks, negative);
            let mut expected = vec![b'0'; len];
            expected[0] = b'1';
            if negative {
                for byte in &mut expected {
                    *byte = !*byte;
                }
            }
            assert_eq!(out, expected);
        }
    }
    assert_eq!(decimal_chunk_digit_count(&[]).unwrap(), 1);
    let mut zero = Vec::new();
    push_decimal_chunk_digits(&mut zero, &[], false);
    assert_eq!(zero, b"0");
}

#[test]
fn chunk_conversion_handles_zero_limbs_and_binary_radix_boundaries() {
    for (limbs, decimal) in [
        (vec![], "0"),
        (vec![0, 0], "0"),
        (vec![1, 0], "1"),
        (vec![u32::MAX], "4294967295"),
        (vec![0, 1], "4294967296"),
        (vec![u32::MAX, u32::MAX], "18446744073709551615"),
        (vec![0, 0, 1], "18446744073709551616"),
    ] {
        let chunks = u32_limbs_to_decimal_chunks(limbs);
        let mut out = Vec::new();
        push_decimal_chunk_digits(&mut out, &chunks, false);
        assert_eq!(out, decimal.as_bytes());
        assert_eq!(decimal_chunk_digit_count(&chunks).unwrap(), out.len());
    }
}
