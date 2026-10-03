use crate::Decimal;
use crate::decimal::{DEFAULT_DIVISION_SCALE, MAX_SUPPORTED_SCALE, ParseDecimalErrorReason};
use candid::{CandidType, decode_one, encode_one};
use num_bigint::{BigInt, Sign};
use proptest::prelude::*;
use std::str::FromStr;

fn assert_decimal_parse_reason(input: &str, reason: ParseDecimalErrorReason) {
    let err = Decimal::from_str(input).expect_err("decimal input should reject");

    assert_eq!(err.reason(), reason);
    assert_eq!(err.to_string(), "decimal parse error");
}

#[test]
fn decimal_candid_roundtrip() {
    assert_eq!(Decimal::ty(), String::ty());

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
fn decimal_remainder_preserves_exact_values_after_scale_alignment() {
    let value: Decimal = "5.0000000000000000000000000001".parse().unwrap();
    let divisor = Decimal::new(10_000_000_000_000, 0);
    for lhs in [value, -value] {
        for rhs in [divisor, -divisor] {
            assert_eq!(lhs.checked_rem(rhs), Some(lhs));
            assert_eq!(lhs % rhs, lhs);
            let mut assigned = lhs;
            assigned %= rhs;
            assert_eq!(assigned, lhs);
        }
    }
    for scale in 0..=MAX_SUPPORTED_SCALE {
        let value = Decimal::from_i128_with_scale(i128::MIN, scale);
        assert_eq!(value.checked_rem(Decimal::ZERO), None);
        assert_eq!(value % Decimal::ZERO, Decimal::ZERO);
        let mut assigned = value;
        assigned %= Decimal::ZERO;
        assert_eq!(assigned, Decimal::ZERO);
    }
}

#[test]
fn decimal_remainder_matches_exact_integer_oracle_at_every_scale_pair() {
    for lhs_scale in 0..=MAX_SUPPORTED_SCALE {
        for rhs_scale in 0..=MAX_SUPPORTED_SCALE {
            let scale = lhs_scale.max(rhs_scale);
            for lhs_mantissa in [i128::MIN, i128::MAX, -17, 0, 17] {
                for rhs_mantissa in [i128::MIN, i128::MAX, -3, -1, 1, 3] {
                    // An arbitrary-precision integer oracle qualifies exact
                    // results independently of the bounded runtime arithmetic.
                    let lhs_integer =
                        BigInt::from(lhs_mantissa) * BigInt::from(10).pow(scale - lhs_scale);
                    let rhs_integer =
                        BigInt::from(rhs_mantissa) * BigInt::from(10).pow(scale - rhs_scale);
                    let expected_mantissa = i128::try_from(lhs_integer % rhs_integer).unwrap();
                    let expected = Decimal::from_i128_with_scale(expected_mantissa, scale);
                    let lhs = Decimal::from_i128_with_scale(lhs_mantissa, lhs_scale);
                    let rhs = Decimal::from_i128_with_scale(rhs_mantissa, rhs_scale);
                    let result = lhs.checked_rem(rhs).expect("exact remainder fits");
                    assert_eq!(result.parts(), expected.parts(), "{lhs} % {rhs}");
                    assert_eq!(lhs % rhs, expected);
                    let mut assigned = lhs;
                    assigned %= rhs;
                    assert_eq!(assigned, expected);
                }
            }
        }
    }
}

#[test]
fn decimal_operator_completion_preserves_saturating_semantics() {
    let mut remainder = Decimal::new(17, 0);
    remainder %= Decimal::new(5, 0);
    assert_eq!(remainder, Decimal::new(2, 0));

    let product: Decimal = [Decimal::new(2, 0), Decimal::new(3, 0)]
        .into_iter()
        .product();
    assert_eq!(product, Decimal::new(6, 0));
    assert_eq!(
        std::iter::empty::<Decimal>().product::<Decimal>(),
        Decimal::new(1, 0)
    );
    assert_eq!(-Decimal::new(25, 1), Decimal::new(-25, 1));

    let minimum = Decimal::from_i128_with_scale(i128::MIN, 0);
    assert_eq!((-minimum).mantissa(), i128::MAX);
}

#[test]
fn decimal_parse_rejects_mantissa_overflow_without_float_fallback() {
    let too_large = "340282366920938463463374607431768211456";
    assert_decimal_parse_reason(too_large, ParseDecimalErrorReason::MantissaOverflow);
}

#[test]
fn decimal_parse_rejects_exponent_notation() {
    assert_decimal_parse_reason("1e3", ParseDecimalErrorReason::ExponentNotationUnsupported);
    assert_decimal_parse_reason("1E3", ParseDecimalErrorReason::ExponentNotationUnsupported);
}

#[test]
fn decimal_parse_rejects_invalid_significand_and_digits_with_reason_codes() {
    assert_decimal_parse_reason("", ParseDecimalErrorReason::Empty);
    assert_decimal_parse_reason(".", ParseDecimalErrorReason::InvalidSignificand);
    assert_decimal_parse_reason("1.2.3", ParseDecimalErrorReason::InvalidSignificand);
    assert_decimal_parse_reason("abc", ParseDecimalErrorReason::InvalidDigits);
    assert_decimal_parse_reason("1.x", ParseDecimalErrorReason::InvalidDigits);
}

#[test]
fn decimal_parse_preserves_signed_limits_scale_and_zero_forms() {
    for (input, mantissa, scale) in [
        ("170141183460469231731687303715884105727", i128::MAX, 0),
        ("-170141183460469231731687303715884105728", i128::MIN, 0),
        ("-17014118346046923173168730371588410572.8", i128::MIN, 1),
        (" \u{2003}+00012.3400\u{2003} ", 123_400, 4),
        ("-.5", -5, 1),
        ("+1.", 1, 0),
        ("-0.000", 0, 3),
        ("0.00000000000000000000000000010", 1, 28),
        ("-0.00000000000000000000000000000", 0, 28),
    ] {
        let parsed = input.parse::<Decimal>().unwrap();
        assert_eq!((parsed.mantissa(), parsed.scale()), (mantissa, scale));
    }
    let padded = format!("-{}1.20", "0".repeat(4096));
    assert_eq!(
        padded.parse::<Decimal>().unwrap().parts(),
        Decimal::new(-120, 2).parts()
    );
}

#[test]
fn decimal_parse_preserves_error_precedence() {
    for (input, reason) in [
        (
            "170141183460469231731687303715884105728",
            ParseDecimalErrorReason::MantissaOverflow,
        ),
        (
            "-170141183460469231731687303715884105729",
            ParseDecimalErrorReason::MantissaOverflow,
        ),
        (
            "9999999999999999999999999999999999999999x",
            ParseDecimalErrorReason::InvalidDigits,
        ),
        (
            "9999999999999999999999999999999999999999.0.0",
            ParseDecimalErrorReason::InvalidSignificand,
        ),
        (
            "9999999999999999999999999999999999999999e?",
            ParseDecimalErrorReason::ExponentNotationUnsupported,
        ),
        (
            "9999999999999999999999999999999999999999.00000000000000000000000000001",
            ParseDecimalErrorReason::MantissaOverflow,
        ),
        (
            "0.00000000000000000000000000001",
            ParseDecimalErrorReason::ScaleExceedsSupportedRange,
        ),
        ("--1", ParseDecimalErrorReason::InvalidDigits),
        ("+", ParseDecimalErrorReason::InvalidSignificand),
    ] {
        assert_decimal_parse_reason(input, reason);
    }
}

#[test]
fn decimal_float_conversion_preserves_display_contract_across_exponents() {
    // Include signed zero, nonfinite values, subnormals, mantissa extremes and
    // scale boundaries. Raw parts also protect scale, not just numeric equality.
    for exponent in 0u64..=0x7ff {
        for fraction in [0, 1, (1u64 << 51) - 1, (1u64 << 52) - 1] {
            for sign in [0, 1u64 << 63] {
                let value = f64::from_bits(sign | (exponent << 52) | fraction);
                assert_eq!(
                    Decimal::from_f64_lossy(value).map(|d| d.parts()),
                    value.to_string().parse::<Decimal>().ok().map(|d| d.parts()),
                    "f64 bits {:x}",
                    value.to_bits(),
                );
            }
        }
    }
    for exponent in 0u32..=0xff {
        for fraction in [0, 1, (1u32 << 22) - 1, (1u32 << 23) - 1] {
            for sign in [0, 1u32 << 31] {
                let value = f32::from_bits(sign | (exponent << 23) | fraction);
                assert_eq!(
                    Decimal::from_f32_lossy(value).map(|d| d.parts()),
                    value.to_string().parse::<Decimal>().ok().map(|d| d.parts()),
                    "f32 bits {:x}",
                    value.to_bits(),
                );
            }
        }
    }
    for value in [1e-28f64, 1e-29, 0.1, 1.0, 2.0f64.powi(127)] {
        for value in [value.next_down(), value, value.next_up()] {
            assert_eq!(
                Decimal::from_f64_lossy(value).map(|d| d.parts()),
                value.to_string().parse::<Decimal>().ok().map(|d| d.parts()),
            );
        }
    }
}

#[test]
fn decimal_try_new_rejects_scale_over_max() {
    assert!(Decimal::try_new(1, MAX_SUPPORTED_SCALE).is_some());
    assert!(Decimal::try_new(1, MAX_SUPPORTED_SCALE + 1).is_none());
}

#[test]
fn decimal_new_panics_on_scale_over_max() {
    assert!(
        std::panic::catch_unwind(|| {
            let _ = Decimal::new(1, MAX_SUPPORTED_SCALE + 1);
        })
        .is_err(),
        "scale over max should panic",
    );
}

#[test]
fn decimal_new_unchecked_is_internal_invariant_bypass() {
    let d = Decimal::new_unchecked(1, MAX_SUPPORTED_SCALE + 1);
    assert_eq!(d.scale(), MAX_SUPPORTED_SCALE + 1);
}

#[test]
fn decimal_try_from_i128_with_scale_rejects_unrepresentable_scale() {
    assert_eq!(
        Decimal::try_from_i128_with_scale(1, MAX_SUPPORTED_SCALE + 1),
        None,
    );
}

#[test]
fn decimal_from_i128_with_scale_panics_on_unrepresentable_scale() {
    assert!(
        std::panic::catch_unwind(|| {
            let _ = Decimal::from_i128_with_scale(1, MAX_SUPPORTED_SCALE + 1);
        })
        .is_err(),
        "unrepresentable scale should panic",
    );
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
fn decimal_multiplication_ignores_fixed_scale_padding() {
    for scale in [0, 8, 18, 28] {
        let padding = 10_i128.pow(scale);
        for left in [0_i64, 1, 20, -20] {
            for right in [0_i64, 2, 20, -20] {
                let expected = Decimal::new(left * right, 0);
                let left = Decimal::from_i128_with_scale(i128::from(left) * padding, scale);
                let right = Decimal::from_i128_with_scale(i128::from(right) * padding, scale);

                assert_eq!(left.checked_mul(right), Some(expected));
                assert_eq!(left * right, expected);
                let mut assigned = left;
                assigned *= right;
                assert_eq!(assigned, expected);
                assert_eq!([left, right].into_iter().product::<Decimal>(), expected);
            }
        }
    }
}

#[test]
fn decimal_multiplication_normalizes_mixed_scale_operands() {
    let large = Decimal::from_i128_with_scale(200_000_000_000_000_000_000, 0);
    let padded_fraction = Decimal::from_i128_with_scale(1_500_000_000_000_000_000, 18);
    let expected = Decimal::from_i128_with_scale(300_000_000_000_000_000_000, 0);

    assert_eq!(large.checked_mul(padded_fraction), Some(expected));
    assert_eq!(padded_fraction.checked_mul(large), Some(expected));
    let padded = Decimal::from_i128_with_scale(20_000_000_000_000_000_000, 18);
    assert_eq!(padded.checked_powu(2), Some(Decimal::new(400, 0)));
    assert_eq!(padded.powu(2), Decimal::new(400, 0));
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

#[test]
fn decimal_multiplication_rounds_precision_across_operator_surfaces() {
    for (input, expected) in [
        ("0.000000000000001", "0"),
        ("1.123456789012345678", "1.2621551567779301925279682998"),
        (
            "17014118346.0469231731687303715884105727",
            "289480223093290488558.92746252171976963",
        ),
    ] {
        let input = Decimal::from_str(input).unwrap();
        let expected = Decimal::from_str(expected).unwrap();
        for (left, right, expected) in [
            (input, input, expected),
            (-input, input, -expected),
            (input, -input, -expected),
            (-input, -input, expected),
        ] {
            assert_eq!(left.checked_mul(right), Some(expected));
            assert_eq!(left * right, expected);
            let mut assigned = left;
            assigned *= right;
            assert_eq!(assigned, expected);
            assert_eq!([left, right].into_iter().product::<Decimal>(), expected);
        }
        assert_eq!(input.checked_powu(2), Some(expected));
        assert_eq!(input.powu(2), expected);
    }
    let expected = Decimal::from_str("17.4494022688864073185588037538").unwrap();
    assert_eq!(Decimal::new(11, 1).checked_powu(30), Some(expected));
    assert_eq!(Decimal::new(11, 1).powu(30), expected);
}

#[test]
fn decimal_multiplication_qualifies_all_scale_pairs_and_signs() {
    for left_scale in 0..=MAX_SUPPORTED_SCALE {
        for right_scale in 0..=MAX_SUPPORTED_SCALE {
            for left_sign in [-1, 1] {
                for right_sign in [-1, 1] {
                    let scale = left_scale + right_scale;
                    let expected = if scale <= MAX_SUPPORTED_SCALE {
                        Decimal::new(left_sign * right_sign, scale)
                    } else {
                        Decimal::ZERO
                    };
                    assert_eq!(
                        Decimal::new(left_sign, left_scale)
                            .checked_mul(Decimal::new(right_sign, right_scale)),
                        Some(expected),
                    );
                }
            }
        }
    }
}

#[test]
fn decimal_multiplication_rounds_ties_once_at_the_final_scale() {
    for sign in [-1, 1] {
        for (fraction, rounded) in [(4, 0), (5, 1), (6, 1), (14, 1), (15, 2), (16, 2)] {
            assert_eq!(
                Decimal::new(sign, 28).checked_mul(Decimal::new(fraction, 1)),
                Some(Decimal::new(sign * rounded, 28)),
            );
        }
        // The first 28-scale rounding cannot fit the mantissa. Rounding that
        // rounded intermediate again would incorrectly raise the final digit.
        let input = Decimal::from_i128_with_scale(i128::from(sign) * i128::MAX, 28);
        let expected = Decimal::from_str("17541556014.7743777915369610131076513").unwrap();
        let expected = if sign < 0 { -expected } else { expected };
        let result = input.checked_mul(Decimal::new(1031, 3)).unwrap();
        assert_eq!(result, expected);
        assert_eq!(result.scale(), 27);
    }
}

#[test]
fn decimal_multiplication_preserves_signed_magnitude_limits() {
    for mantissa in [i128::MIN, i128::MAX] {
        let value = Decimal::from_i128_with_scale(mantissa, 0);
        assert_eq!(value.checked_mul(Decimal::new(1, 0)), Some(value));
        assert_eq!(value.checked_mul(Decimal::ZERO), Some(Decimal::ZERO));
        assert_eq!(value.checked_mul(Decimal::new(2, 0)), None);
        assert_eq!(value * Decimal::new(2, 0), value);
    }
    assert_eq!(
        Decimal::from_i128_with_scale(i128::MIN, 0).checked_mul(Decimal::new(-1, 0)),
        None,
    );
    let minimum = Decimal::from_i128_with_scale(i128::MIN, 28);
    assert_eq!(
        minimum.checked_mul(minimum),
        Some("289480223093290488558.92746252171976963".parse().unwrap()),
    );
}

// Round an arbitrary-precision rational independently at each candidate scale.
// This oracle has no bounded intermediate arithmetic or runtime fitting helper.
fn rounded_ratio_oracle(
    numerator: &BigInt,
    denominator: &BigInt,
    max_scale: u32,
) -> Option<Decimal> {
    if denominator == &BigInt::from(0) {
        return None;
    }
    for scale in (0..=max_scale).rev() {
        let scaled = numerator * BigInt::from(10).pow(scale);
        let quotient = &scaled / denominator;
        let remainder = &scaled % denominator;
        let rounded = if &(remainder.magnitude() * 2_u8) >= denominator.magnitude() {
            quotient
                + if numerator.sign() == denominator.sign() {
                    1
                } else {
                    -1
                }
        } else {
            quotient
        };
        if let Ok(mantissa) = i128::try_from(rounded) {
            return Some(Decimal::from_i128_with_scale(mantissa, scale));
        }
    }
    None
}

#[test]
fn decimal_add_sub_fit_alignment_cancellation_and_signed_limits() {
    for (large, tiny) in [
        ("1000000000000000000000", "0.000000000000000001"),
        (
            "1000000000000000000000000000000",
            "0.0000000000000000000000000001",
        ),
    ] {
        let large: Decimal = large.parse().unwrap();
        let tiny: Decimal = tiny.parse().unwrap();
        for (lhs, rhs) in [(large, tiny), (-large, -tiny)] {
            assert_eq!(lhs.checked_add(rhs), Some(lhs));
            assert_eq!(lhs.checked_sub(rhs), Some(lhs));
            assert_eq!(lhs + rhs, lhs);
            assert_eq!(lhs - rhs, lhs);
        }
    }
    let large: Decimal = "20000000000000000000000000000000000000".parse().unwrap();
    let other = Decimal::from_i128_with_scale(-10_i128.pow(38), 1);
    let expected = Decimal::from_i128_with_scale(10_i128.pow(38), 1);
    assert_eq!(large.checked_add(other), Some(expected));
    for scale in 0..=28 {
        let minimum = Decimal::from_i128_with_scale(i128::MIN, scale);
        assert_eq!(minimum.checked_sub(minimum), Some(Decimal::new(0, scale)));
        assert_eq!(minimum - minimum, Decimal::ZERO);
    }
    for sign in [-1_i128, 1] {
        let lhs = Decimal::from_i128_with_scale(sign * i128::MAX, 28);
        let rhs = Decimal::from_i128_with_scale(sign * 8, 28);
        let expected = Decimal::from_i128_with_scale(sign * (i128::MAX / 10 + 2), 27);
        assert_eq!(lhs.checked_add(rhs), Some(expected));
        assert_eq!(lhs.checked_sub(-rhs), Some(expected));
    }
}

#[test]
fn decimal_division_fits_large_scaled_quotients_before_overflow() {
    for scale in 0..=28 {
        for lhs_sign in [-1_i128, 1] {
            for rhs_sign in [-1_i128, 1] {
                let lhs = Decimal::from_i128_with_scale(lhs_sign * i128::MAX, 0);
                let rhs = Decimal::from_i128_with_scale(rhs_sign * i128::MAX, scale);
                let expected =
                    Decimal::from_i128_with_scale(lhs_sign * rhs_sign * 10_i128.pow(scale), 0);
                assert_eq!(lhs.checked_div(rhs), Some(expected));
                assert_eq!(lhs / rhs, expected);
                let mut assigned = lhs;
                assigned /= rhs;
                assert_eq!(assigned, expected);
            }
        }
    }
    let minimum = Decimal::from_i128_with_scale(i128::MIN, 18);
    assert_eq!(
        minimum.checked_div(Decimal::new(-1, 0)),
        Some("170141183460469231731.68730371588410573".parse().unwrap()),
    );
}

#[test]
fn decimal_true_magnitude_overflow_uses_global_primitive_bounds() {
    for scale in 0..=28 {
        for lhs_sign in [-1_i128, 1] {
            for rhs_sign in [-1_i128, 1] {
                let lhs = Decimal::from_i128_with_scale(lhs_sign * i128::MAX, 0);
                let rhs = Decimal::from_i128_with_scale(rhs_sign * i128::MAX, scale);
                let bound = Decimal::from_i128_with_scale(
                    if lhs_sign == rhs_sign {
                        i128::MAX
                    } else {
                        i128::MIN
                    },
                    0,
                );
                assert_eq!(lhs.checked_mul(rhs), None);
                assert_eq!((lhs * rhs).parts(), bound.parts());
                let mut assigned = lhs;
                assigned *= rhs;
                assert_eq!(assigned.parts(), bound.parts());
                assert_eq!([lhs, rhs].into_iter().product::<Decimal>(), bound);
            }
        }
    }
    let large: Decimal = "10000000000000000000000000000000000000".parse().unwrap();
    for lhs in [large, -large] {
        assert_eq!(lhs.checked_div(Decimal::new(1, 28)), None);
        assert_eq!((lhs / Decimal::new(1, 28)).scale(), 0);
    }
}

#[test]
fn decimal_add_sub_div_match_exact_ratio_oracle_at_all_scale_pairs() {
    for lhs_scale in 0..=28 {
        for rhs_scale in 0..=28 {
            let scale = lhs_scale.max(rhs_scale);
            let denominator = BigInt::from(10).pow(scale);
            for lhs_m in [i128::MIN, i128::MAX, -17, 0, 17] {
                for rhs_m in [i128::MIN, i128::MAX, -17, 0, 17] {
                    let lhs = Decimal::from_i128_with_scale(lhs_m, lhs_scale);
                    let rhs = Decimal::from_i128_with_scale(rhs_m, rhs_scale);
                    let lhs_integer = BigInt::from(lhs_m) * BigInt::from(10).pow(scale - lhs_scale);
                    let rhs_integer = BigInt::from(rhs_m) * BigInt::from(10).pow(scale - rhs_scale);
                    let sum = &lhs_integer + &rhs_integer;
                    let difference = &lhs_integer - &rhs_integer;
                    for (exact, checked, primitive, subtract) in [
                        (&sum, lhs.checked_add(rhs), lhs + rhs, false),
                        (&difference, lhs.checked_sub(rhs), lhs - rhs, true),
                    ] {
                        let expected = rounded_ratio_oracle(exact, &denominator, scale);
                        assert_eq!(
                            checked.map(|value| value.parts()),
                            expected.map(|value| value.parts())
                        );
                        let bound = Decimal::from_i128_with_scale(
                            if exact.sign() == Sign::Minus {
                                i128::MIN
                            } else {
                                i128::MAX
                            },
                            0,
                        );
                        assert_eq!(primitive, expected.unwrap_or(bound));
                        let mut assigned = lhs;
                        if subtract {
                            assigned -= rhs;
                        } else {
                            assigned += rhs;
                        }
                        assert_eq!(assigned, primitive);
                    }
                    assert_eq!([lhs, rhs].into_iter().sum::<Decimal>(), lhs + rhs);
                    if rhs_m != 0 {
                        let expected = rounded_ratio_oracle(&lhs_integer, &rhs_integer, 18)
                            .map(|value| value.normalize());
                        assert_eq!(
                            lhs.checked_div(rhs).map(|value| value.parts()),
                            expected.map(|value| value.parts())
                        );
                        let bound = Decimal::from_i128_with_scale(
                            if lhs_m.is_negative() == rhs_m.is_negative() {
                                i128::MAX
                            } else {
                                i128::MIN
                            },
                            0,
                        );
                        assert_eq!(lhs / rhs, expected.unwrap_or(bound));
                    }
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn decimal_parse_preserves_mantissa_and_scale(
        mantissa in any::<i128>(),
        scale in 0u32..=MAX_SUPPORTED_SCALE,
        padding in 0usize..64,
    ) {
        let sign = if mantissa < 0 { "-" } else { "+" };
        let mut digits = format!("{}{}", "0".repeat(padding + scale as usize), mantissa.unsigned_abs());
        if scale > 0 {
            digits.insert(digits.len() - scale as usize, '.');
        }
        let parsed = format!("{sign}{digits}").parse::<Decimal>().unwrap();
        prop_assert_eq!(parsed.parts(), Decimal::from_i128_with_scale(mantissa, scale).parts());
    }

    #[test]
    fn decimal_float_conversion_matches_text_for_arbitrary_bits(bits64 in any::<u64>(), bits32 in any::<u32>()) {
        let value = f64::from_bits(bits64);
        prop_assert_eq!(
            Decimal::from_f64_lossy(value).map(|d| d.parts()),
            value.to_string().parse::<Decimal>().ok().map(|d| d.parts()),
        );
        let value = f32::from_bits(bits32);
        prop_assert_eq!(
            Decimal::from_f32_lossy(value).map(|d| d.parts()),
            value.to_string().parse::<Decimal>().ok().map(|d| d.parts()),
        );
    }

    #[test]
    fn decimal_add_rounding_and_saturation_matches_exact_oracle(
        lhs_m in any::<i128>(),
        rhs_m in any::<i128>(),
        lhs_scale in 0u32..=28,
        rhs_scale in 0u32..=28,
    ) {
        let lhs = Decimal::from_i128_with_scale(lhs_m, lhs_scale);
        let rhs = Decimal::from_i128_with_scale(rhs_m, rhs_scale);
        let out = lhs + rhs;
        let target_scale = lhs_scale.max(rhs_scale);

        let exact = BigInt::from(lhs_m) * BigInt::from(10).pow(target_scale - lhs_scale)
            + BigInt::from(rhs_m) * BigInt::from(10).pow(target_scale - rhs_scale);
        let expected = rounded_ratio_oracle(&exact, &BigInt::from(10).pow(target_scale), target_scale);
        let bound = Decimal::from_i128_with_scale(if exact.sign() == Sign::Minus { i128::MIN } else { i128::MAX }, 0);
        prop_assert_eq!(lhs.checked_add(rhs), expected);
        prop_assert_eq!(out, expected.unwrap_or(bound));
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

#[test]
fn decimal_text_transports_preserve_values_and_measured_sizes() {
    for (text, one, batch, binary) in [
        ("0", 9, 2_011, 2),
        ("-1", 10, 3_011, 3),
        ("42.5", 12, 5_011, 5),
        ("123.45", 14, 7_011, 7),
        ("0.00000001", 18, 11_011, 11),
        ("1234567890.123456789", 28, 21_011, 21),
        ("170141183460469231731687303715884105727", 47, 40_011, 41),
        ("-170141183460469231731687303715884105728", 48, 41_011, 42),
    ] {
        let value: Decimal = text.parse().expect("the decimal should parse");
        assert_eq!(encode_one(value).expect("Candid should encode").len(), one);
        let values = vec![value; 1_000];
        let encoded = encode_one(&values).expect("the decimal batch should encode");
        assert_eq!(encoded.len(), batch);
        assert_eq!(
            decode_one::<Vec<Decimal>>(&encoded).expect("the decimal batch should decode"),
            values,
        );
        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&value, &mut encoded).expect("CBOR should encode");
        assert_eq!(encoded.len(), binary);
        assert_eq!(
            ciborium::de::from_reader::<String, _>(encoded.as_slice())
                .expect("CBOR should contain decimal text"),
            text,
        );
        assert_eq!(
            ciborium::de::from_reader::<Decimal, _>(encoded.as_slice())
                .expect("CBOR should decode the decimal"),
            value,
        );
        let json = serde_json::to_string(&value).expect("JSON should encode");
        assert_eq!(
            serde_json::from_str::<Decimal>(&json).expect("JSON should decode the decimal"),
            value,
        );
    }
}

#[test]
fn decimal_division_signed_overflow_is_checked_and_saturating() {
    let divisor = Decimal::new(-1, 0);
    let saturated = Decimal::from_i128_with_scale(i128::MAX, 0);
    let minimum = Decimal::from_i128_with_scale(i128::MIN, 0);
    assert_eq!(minimum.checked_div(divisor), None);
    assert_eq!(minimum / divisor, saturated);
    let mut assigned = minimum;
    assigned /= divisor;
    assert_eq!(assigned, saturated);
    assert_eq!(minimum.checked_div(Decimal::new(1, 0)), Some(minimum));
    assert_eq!(minimum.checked_div(Decimal::ZERO), None);
}
