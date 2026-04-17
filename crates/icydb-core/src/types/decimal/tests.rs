use crate::types::Decimal;
use crate::types::decimal::{DEFAULT_DIVISION_SCALE, MAX_SUPPORTED_SCALE};
use candid::{decode_one, encode_one};
use proptest::prelude::*;
use std::str::FromStr;

#[test]
fn decimal_candid_roundtrip() {
    let cases = [
        "0",
        "1",
        "-1",
        "42.5",
        "1234567890.123456789",
        "0.00000001",
        "1000000000000000000.000000000000000001",
    ];

    for s in cases {
        let d1 = Decimal::from_str(s).expect("parse decimal");

        // encode via Candid (should encode as text)
        let bytes = encode_one(d1).expect("candid encode");

        // decode back to Decimal
        let d2: Decimal = decode_one(&bytes).expect("candid decode to Decimal");
        assert_eq!(d2, d1, "roundtrip mismatch for {s}");

        // also ensure the on-wire representation is text by decoding as String
        let wire_str: String = decode_one(&bytes).expect("candid decode to String");
        assert_eq!(wire_str, d1.to_string(), "wire text mismatch for {s}");
    }
}

#[test]
fn decimal_division_is_fixed_scale_and_rounded() {
    let one = Decimal::new(1, 0);
    let third = one / Decimal::new(3, 0);
    let sixth = one / Decimal::new(6, 0);
    let neg_sixth = Decimal::new(-1, 0) / Decimal::new(6, 0);

    assert_eq!(third.to_string(), "0.333333333333333333");
    assert_eq!(sixth.to_string(), "0.166666666666666667");
    assert_eq!(neg_sixth.to_string(), "-0.166666666666666667");
}

#[test]
fn decimal_div_by_zero_returns_zero() {
    let value = Decimal::new(123, 2);
    assert_eq!(value / Decimal::ZERO, Decimal::ZERO);
}

#[test]
fn decimal_parse_rejects_mantissa_overflow_without_float_fallback() {
    let too_large = "340282366920938463463374607431768211456";
    assert!(Decimal::from_str(too_large).is_err());
}

#[test]
fn decimal_parse_rejects_exponent_notation() {
    assert!(Decimal::from_str("1e3").is_err());
    assert!(Decimal::from_str("1E3").is_err());
}

#[test]
fn decimal_try_new_rejects_scale_over_max() {
    assert!(Decimal::try_new(1, MAX_SUPPORTED_SCALE).is_some());
    assert!(Decimal::try_new(1, MAX_SUPPORTED_SCALE + 1).is_none());
}

#[test]
#[should_panic(expected = "decimal scale exceeds supported range")]
fn decimal_new_panics_on_scale_over_max() {
    let _ = Decimal::new(1, MAX_SUPPORTED_SCALE + 1);
}

#[test]
fn decimal_new_unchecked_allows_scale_over_max() {
    let d = Decimal::new_unchecked(1, MAX_SUPPORTED_SCALE + 1);
    assert_eq!(d.scale(), MAX_SUPPORTED_SCALE + 1);
}

#[test]
fn decimal_add_overflow_saturates() {
    let max = Decimal::from_i128_with_scale(i128::MAX, 0);
    let min = Decimal::from_i128_with_scale(i128::MIN, 0);

    assert_eq!((max + Decimal::new(1, 0)).mantissa(), i128::MAX);
    assert_eq!((min + Decimal::new(-1, 0)).mantissa(), i128::MIN);
}

#[test]
fn decimal_mul_overflow_saturates() {
    let positive = Decimal::from_i128_with_scale(i128::MAX / 2 + 1, 0);
    let negative = Decimal::from_i128_with_scale(i128::MIN, 0);

    assert_eq!((positive * Decimal::new(2, 0)).mantissa(), i128::MAX);
    assert_eq!((negative * Decimal::new(2, 0)).mantissa(), i128::MIN);
}

#[test]
fn decimal_division_sign_scale_matrix() {
    let sign_cases = [
        (1i128, 1i128, false),
        (1i128, -1i128, true),
        (-1i128, 1i128, true),
        (-1i128, -1i128, false),
    ];
    let scales = [0u32, 1u32, 8u32, 18u32];

    for (lhs_sign, rhs_sign, expected_negative) in sign_cases {
        for lhs_scale in scales {
            for rhs_scale in scales {
                let lhs = Decimal::from_i128_with_scale(lhs_sign * 25, lhs_scale);
                let rhs = Decimal::from_i128_with_scale(rhs_sign * 5, rhs_scale);
                let out = lhs / rhs;

                assert!(
                    out.scale() <= DEFAULT_DIVISION_SCALE,
                    "lhs={lhs:?}, rhs={rhs:?}, out={out:?}"
                );
                assert!(
                    !out.is_zero(),
                    "division matrix should not produce zero for non-zero operands"
                );
                assert_eq!(
                    out.is_sign_negative(),
                    expected_negative,
                    "lhs={lhs:?}, rhs={rhs:?}, out={out:?}"
                );
            }
        }
    }
}

proptest! {
    #[test]
    fn decimal_add_saturation_boundary_property(
        lhs_m in any::<i128>(),
        rhs_m in any::<i128>(),
        lhs_scale in 0u32..=18,
        rhs_scale in 0u32..=18,
    ) {
        let lhs = Decimal::from_i128_with_scale(lhs_m, lhs_scale);
        let rhs = Decimal::from_i128_with_scale(rhs_m, rhs_scale);
        let out = lhs + rhs;
        let target_scale = lhs_scale.max(rhs_scale);

        prop_assert_eq!(
            out.scale(),
            target_scale,
            "addition result scale must stay on max operand scale"
        );

        if let Some(exact) = lhs.checked_add_impl(rhs) {
            prop_assert_eq!(out, exact);
        } else {
            prop_assert!(
                out.mantissa() == i128::MAX
                    || out.mantissa() == i128::MIN
                    || out.mantissa() == 0,
                "overflow path must saturate deterministically"
            );
        }
    }

    #[test]
    fn decimal_division_non_zero_sign_property(
        lhs_m in any::<i128>().prop_filter("lhs non-zero", |v| *v != 0),
        rhs_m in any::<i128>().prop_filter("rhs non-zero", |v| *v != 0),
        lhs_scale in 0u32..=18,
        rhs_scale in 0u32..=18,
    ) {
        let lhs = Decimal::from_i128_with_scale(lhs_m, lhs_scale);
        let rhs = Decimal::from_i128_with_scale(rhs_m, rhs_scale);
        let out = lhs / rhs;

        prop_assert!(out.scale() <= DEFAULT_DIVISION_SCALE);

        if !out.is_zero() {
            prop_assert_eq!(
                out.is_sign_negative(),
                lhs.is_sign_negative() ^ rhs.is_sign_negative(),
                "non-zero quotient sign must follow operand signs"
            );
        }
    }
}
