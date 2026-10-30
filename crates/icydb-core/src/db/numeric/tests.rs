//! Module: db::numeric::tests
//! Covers numeric comparison, coercion, and ordering helper behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::numeric::{
        NumericArithmeticOp, NumericEvalError, add_decimal_terms_checked,
        apply_numeric_arithmetic_checked, apply_value_arithmetic_checked,
        average_decimal_terms_checked, canonical_value_compare, coerce_numeric_decimal,
        compare_numeric_eq, compare_numeric_or_strict_order, compare_numeric_order,
        decimal_cbrt_checked, decimal_exp_checked, decimal_ln_checked, decimal_log2_checked,
        decimal_log10_checked, decimal_power_checked, decimal_sqrt_checked,
        divide_decimal_terms_checked,
    },
    types::{Decimal, Duration, Float64 as F64, Timestamp, U256},
    value::{Value, ops::ordering as value_ordering},
};
use std::cmp::Ordering;

#[test]
fn numeric_order_preserves_decimal_coercion_and_exact_nat128_pairs() {
    let mut cases = Vec::new();
    for value in [i64::MIN, -1, 0, 1, i64::MAX] {
        cases.push(Value::Int64(value));
        cases.push(Value::Timestamp(Timestamp::from_millis(value)));
    }
    for value in [i128::MIN, -1, 0, 1, i128::MAX] {
        cases.push(Value::Int128(value));
        cases.push(Value::Decimal(Decimal::from_i128_with_scale(value, 2)));
    }
    for value in [0, 1, u64::MAX] {
        cases.push(Value::Nat64(value));
        cases.push(Value::Duration(Duration::from_millis(value)));
    }
    for value in [
        0,
        1,
        u128::from(u64::MAX),
        u128::from(u64::MAX) + 1,
        i128::MAX.cast_unsigned() - 1,
        i128::MAX.cast_unsigned(),
        i128::MAX.cast_unsigned() + 1,
        u128::MAX,
    ] {
        cases.push(Value::Nat128(value));
    }
    cases.extend([
        Value::Float64(F64::try_new(1.0e-30).unwrap()),
        Value::Float64(F64::try_new(f64::MAX).unwrap()),
        Value::Text("1".to_string()),
        Value::Null,
    ]);
    for left in &cases {
        for right in &cases {
            let expected = match (left, right) {
                (Value::Nat128(left), Value::Nat128(right)) => Some(left.cmp(right)),
                _ => coerce_numeric_decimal(left)
                    .zip(coerce_numeric_decimal(right))
                    .map(|(left, right)| left.cmp(&right)),
            };
            assert_eq!(
                compare_numeric_order(left, right),
                expected,
                "{left:?}, {right:?}"
            );
            assert_eq!(
                compare_numeric_eq(left, right),
                expected.map(Ordering::is_eq)
            );
        }
    }
}

#[test]
fn nat128_comparison_uses_the_complete_unsigned_domain_without_widening_arithmetic() {
    for left in [0, i128::MAX.cast_unsigned(), 1_u128 << 127, u128::MAX] {
        for right in [0, i128::MAX.cast_unsigned(), 1_u128 << 127, u128::MAX] {
            let expected = left.cmp(&right);
            let left = Value::Nat128(left);
            let right = Value::Nat128(right);
            assert_eq!(left.cmp_numeric(&right), Some(expected));
            assert_eq!(compare_numeric_order(&left, &right), Some(expected));
            assert_eq!(compare_numeric_eq(&left, &right), Some(expected.is_eq()));
            assert_eq!(value_ordering::eq(&left, &right), Some(expected.is_eq()));
            assert_eq!(value_ordering::lt(&left, &right), Some(expected.is_lt()));
            assert_eq!(canonical_value_compare(&left, &right), expected);
        }
    }
    let maximum = Value::Nat128(u128::MAX);
    assert_eq!(coerce_numeric_decimal(&maximum), None);
    for mixed in [
        Value::Nat64(1),
        Value::Int128(i128::MAX),
        Value::Decimal(Decimal::new(1, 0)),
    ] {
        assert_eq!(compare_numeric_order(&maximum, &mixed), None);
        assert_eq!(compare_numeric_order(&mixed, &maximum), None);
    }
}

#[test]
fn numeric_compare_helpers_follow_numeric_widen_domain() {
    assert_eq!(
        compare_numeric_order(&Value::Int64(2), &Value::Nat64(2)),
        Some(Ordering::Equal)
    );
    assert_eq!(
        compare_numeric_eq(&Value::Int64(2), &Value::Nat64(2)),
        Some(true)
    );
    assert_eq!(
        compare_numeric_order(&Value::Text("x".to_string()), &Value::Text("x".to_string())),
        None
    );
}

#[test]
fn numeric_compare_order_matches_value_numeric_cmp_for_shared_domain() {
    let cases = [
        (Value::Int64(42), Value::Nat64(42)),
        (
            Value::Decimal(Decimal::from_i64(10).expect("decimal")),
            Value::Float64(F64::try_new(10.0).expect("finite float")),
        ),
        (
            Value::Int64(9_007_199_254_740_993),
            Value::Float64(F64::try_new(9_007_199_254_740_992.0).expect("finite float")),
        ),
    ];

    for (left, right) in cases {
        assert_eq!(
            compare_numeric_order(&left, &right),
            left.cmp_numeric(&right),
            "numeric comparison authority drifted for left={left:?}, right={right:?}",
        );
    }
}

#[test]
fn numeric_compare_order_requires_both_operands_numeric_coercible() {
    assert_eq!(
        compare_numeric_order(&Value::Int64(2), &Value::Text("2".to_string())),
        None
    );
    assert_eq!(
        compare_numeric_order(&Value::Bool(true), &Value::Bool(false)),
        None
    );
}

#[test]
fn broad_numeric_coercion_matches_value_numeric_decimal_boundary() {
    let cases = [
        Value::Int64(4),
        Value::Nat64(4),
        Value::Decimal(Decimal::new(40, 1)),
        Value::Float64(F64::try_new(4.0).expect("finite float")),
        Value::Text("x".to_string()),
    ];

    for value in cases {
        assert_eq!(
            coerce_numeric_decimal(&value),
            value
                .supports_numeric_coercion()
                .then(|| value.to_numeric_decimal())
                .flatten(),
            "broad numeric coercion drifted for value={value:?}",
        );
    }
}

#[test]
fn numeric_or_strict_compare_prefers_numeric_widen_when_available() {
    assert_eq!(
        compare_numeric_or_strict_order(&Value::Int64(2), &Value::Nat64(2)),
        Some(Ordering::Equal)
    );
}

#[test]
fn canonical_value_ordering_uses_value_canonical_order() {
    assert_eq!(
        canonical_value_compare(&Value::Nat64(7), &Value::Nat64(8)),
        Ordering::Less
    );
    assert_eq!(
        canonical_value_compare(&Value::Text("x".to_string()), &Value::Text("x".to_string())),
        Ordering::Equal
    );
}

#[test]
fn canonical_value_ordering_prefers_shared_numeric_or_strict_authority() {
    assert_eq!(
        canonical_value_compare(&Value::Int64(7), &Value::Nat64(7)),
        Ordering::Equal
    );
}

#[test]
fn numeric_or_strict_compare_falls_back_to_strict_for_non_numeric_values() {
    assert_eq!(
        compare_numeric_or_strict_order(
            &Value::Text("a".to_string()),
            &Value::Text("b".to_string())
        ),
        Some(Ordering::Less)
    );
}

#[test]
fn numeric_decimal_coercion_rejects_non_coercible_variants() {
    assert!(coerce_numeric_decimal(&Value::Int64(4)).is_some());
    assert!(coerce_numeric_decimal(&Value::Text("x".to_string())).is_none());
}

#[test]
fn numeric_arithmetic_promotes_integer_and_decimal_to_decimal_domain() {
    let left = Value::Int64(2);
    let right = Value::Decimal(Decimal::new(15, 1));

    let result = apply_numeric_arithmetic_checked(NumericArithmeticOp::Add, &left, &right)
        .expect("mixed integer/decimal arithmetic should coerce into decimal domain");

    assert_eq!(result, Some(Decimal::new(35, 1)));
}

#[test]
fn numeric_arithmetic_division_rounds_half_away_from_zero() {
    let left = Value::Int64(-1);
    let right = Value::Int64(6);

    let result = apply_numeric_arithmetic_checked(NumericArithmeticOp::Div, &left, &right)
        .expect("numeric division should produce deterministic decimal output");

    assert_eq!(
        result,
        Some(Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18))
    );
}

#[test]
fn numeric_arithmetic_multiplication_ignores_decimal_scale_padding() {
    for scale in [8, 18, 28] {
        let padded = Value::Decimal(Decimal::from_i128_with_scale(
            20 * 10_i128.pow(scale),
            scale,
        ));
        assert_eq!(
            apply_value_arithmetic_checked(NumericArithmeticOp::Mul, &padded, &padded),
            Ok(Some(Value::Decimal(Decimal::new(400, 0))))
        );
    }
    let maximum = Value::Decimal(Decimal::from_i128_with_scale(i128::MAX, 0));
    assert_eq!(
        apply_value_arithmetic_checked(NumericArithmeticOp::Mul, &maximum, &Value::Int64(2)),
        Err(NumericEvalError::Overflow)
    );
}

#[test]
fn numeric_arithmetic_addition_reports_overflow() {
    let left = Value::Decimal(Decimal::from_i128_with_scale(i128::MAX, 0));
    let right = Value::Int64(1);

    let err = apply_numeric_arithmetic_checked(NumericArithmeticOp::Add, &left, &right)
        .expect_err("checked numeric addition should reject overflow");

    assert_eq!(err, NumericEvalError::Overflow);
}

#[test]
fn numeric_multiplication_and_power_round_precision_without_false_overflow() {
    for (left, right, expected) in [
        (Decimal::new(1, 15), Decimal::new(1, 15), Decimal::ZERO),
        (
            Decimal::new(-1, 28),
            Decimal::new(5, 1),
            Decimal::new(-1, 28),
        ),
        (
            Decimal::from_i128_with_scale(i128::MAX, 28),
            Decimal::from_i128_with_scale(i128::MAX, 28),
            "289480223093290488558.92746252171976963".parse().unwrap(),
        ),
    ] {
        assert_eq!(
            apply_value_arithmetic_checked(
                NumericArithmeticOp::Mul,
                &Value::Decimal(left),
                &Value::Decimal(right),
            ),
            Ok(Some(Value::Decimal(expected))),
        );
    }
    assert_eq!(
        decimal_power_checked(Decimal::new(11, 1), Decimal::new(30, 0)),
        Ok("17.4494022688864073185588037538".parse().unwrap()),
    );
    assert_eq!(
        decimal_power_checked(
            Decimal::from_i128_with_scale(i128::MAX, 0),
            Decimal::new(2, 0),
        ),
        Err(NumericEvalError::Overflow),
    );
}

#[test]
fn u256_arithmetic_stays_fixed_width_and_checked() {
    let two = Value::U256(U256::from(2_u64));
    let three = Value::U256(U256::from(3_u64));

    assert_eq!(
        apply_value_arithmetic_checked(NumericArithmeticOp::Add, &two, &three),
        Ok(Some(Value::U256(U256::from(5_u64))))
    );
    assert_eq!(
        apply_value_arithmetic_checked(
            NumericArithmeticOp::Add,
            &Value::U256(U256::MAX),
            &Value::U256(U256::ONE),
        ),
        Err(NumericEvalError::Overflow)
    );
    assert_eq!(
        apply_value_arithmetic_checked(
            NumericArithmeticOp::Sub,
            &Value::U256(U256::ZERO),
            &Value::U256(U256::ONE),
        ),
        Err(NumericEvalError::Overflow)
    );
    assert_eq!(
        apply_value_arithmetic_checked(
            NumericArithmeticOp::Div,
            &Value::U256(U256::ONE),
            &Value::U256(U256::ZERO),
        ),
        Err(NumericEvalError::NotRepresentable)
    );
    assert_eq!(
        apply_value_arithmetic_checked(NumericArithmeticOp::Mul, &two, &Value::Nat64(3)),
        Ok(None)
    );
}

#[test]
fn decimal_term_helpers_share_canonical_add_and_divide_semantics() {
    let overflow = add_decimal_terms_checked(
        Decimal::from_i128_with_scale(i128::MAX, 0),
        Decimal::from_i128_with_scale(1, 0),
    );
    let divided = divide_decimal_terms_checked(
        Decimal::from_num(-1_i64).expect("sum decimal"),
        Decimal::from_num(6_u64).expect("divisor decimal"),
    )
    .expect("division should stay representable");

    assert_eq!(overflow, Err(NumericEvalError::Overflow));
    assert_eq!(
        divided,
        Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18)
    );
}

#[test]
fn average_decimal_terms_uses_canonical_division_and_count_coercion() {
    let avg = average_decimal_terms_checked(Decimal::from_num(65_u64).expect("sum decimal"), 3_u64)
        .expect("count should coerce into decimal divisor");

    assert_eq!(
        avg,
        Decimal::from_i128_with_scale(21_666_666_666_666_666_667, 18)
    );
}

#[test]
fn decimal_division_signed_limits_qualify_fit_and_overflow() {
    assert_eq!(
        divide_decimal_terms_checked(
            Decimal::from_i128_with_scale(i128::MIN, 0),
            Decimal::new(-1, 0)
        ),
        Err(NumericEvalError::Overflow),
    );
    assert_eq!(
        divide_decimal_terms_checked(
            Decimal::from_i128_with_scale(i128::MIN, 18),
            Decimal::new(-1, 0)
        ),
        Ok("170141183460469231731.68730371588410573".parse().unwrap()),
    );
}

#[test]
fn numeric_arithmetic_qualifies_fitting_results_before_overflow() {
    for (op, lhs, rhs, expected) in [
        (
            NumericArithmeticOp::Add,
            Decimal::from_i128_with_scale(2 * 10_i128.pow(37), 0),
            Decimal::from_i128_with_scale(-10_i128.pow(38), 1),
            Decimal::from_i128_with_scale(10_i128.pow(37), 0),
        ),
        (
            NumericArithmeticOp::Sub,
            Decimal::from_i128_with_scale(i128::MIN, 28),
            Decimal::from_i128_with_scale(i128::MIN, 28),
            Decimal::ZERO,
        ),
        (
            NumericArithmeticOp::Div,
            Decimal::from_i128_with_scale(i128::MAX, 0),
            Decimal::from_i128_with_scale(i128::MAX, 28),
            Decimal::from_i128_with_scale(10_i128.pow(28), 0),
        ),
    ] {
        let lhs = Value::Decimal(lhs);
        let rhs = Value::Decimal(rhs);
        assert_eq!(
            apply_numeric_arithmetic_checked(op, &lhs, &rhs),
            Ok(Some(expected))
        );
        assert_eq!(
            apply_value_arithmetic_checked(op, &lhs, &rhs),
            Ok(Some(Value::Decimal(expected)))
        );
    }
    let max = Value::Decimal(Decimal::from_i128_with_scale(i128::MAX, 0));
    let min = Value::Decimal(Decimal::from_i128_with_scale(i128::MIN, 0));
    for (op, lhs, rhs) in [
        (NumericArithmeticOp::Add, max.clone(), max.clone()),
        (NumericArithmeticOp::Sub, min.clone(), max.clone()),
        (
            NumericArithmeticOp::Mul,
            max,
            Value::Decimal(Decimal::from_i128_with_scale(i128::MAX, 28)),
        ),
        (NumericArithmeticOp::Div, min, Value::Int64(-1)),
    ] {
        assert_eq!(
            apply_value_arithmetic_checked(op, &lhs, &rhs),
            Err(NumericEvalError::Overflow)
        );
    }
}

#[test]
fn decimal_sum_and_average_helpers_preserve_fitting_precision() {
    assert_eq!(
        add_decimal_terms_checked(
            Decimal::from_i128_with_scale(2 * 10_i128.pow(37), 0),
            Decimal::from_i128_with_scale(-10_i128.pow(38), 1)
        ),
        Ok(Decimal::from_i128_with_scale(10_i128.pow(37), 0)),
    );
    assert_eq!(
        average_decimal_terms_checked(
            Decimal::from_i128_with_scale(i128::MAX, 0),
            1_000_000_000_000_000_000
        ),
        Ok(Decimal::from_i128_with_scale(i128::MAX, 18)),
    );
    assert_eq!(
        average_decimal_terms_checked(Decimal::new(1, 28), 2),
        Ok(Decimal::ZERO)
    );
    assert_eq!(
        average_decimal_terms_checked(Decimal::new(1, 0), 0),
        Err(NumericEvalError::NotRepresentable)
    );
}

#[test]
fn numeric_remainder_decimal_scale_alignment_preserves_exact_results() {
    let value: Decimal = "5.0000000000000000000000000001".parse().unwrap();
    for (lhs, rhs, expected) in [
        (value, Decimal::new(10_000_000_000_000, 0), value),
        (-value, Decimal::new(-10_000_000_000_000, 0), -value),
        (
            Decimal::from_i128_with_scale(i128::MAX, 0),
            Decimal::new(3, 28),
            Decimal::new(1, 28),
        ),
        (
            Decimal::from_i128_with_scale(i128::MIN, 0),
            Decimal::new(-1, 0),
            Decimal::ZERO,
        ),
    ] {
        assert_eq!(
            apply_numeric_arithmetic_checked(
                NumericArithmeticOp::Rem,
                &Value::Decimal(lhs),
                &Value::Decimal(rhs),
            ),
            Ok(Some(expected)),
        );
        assert_eq!(
            apply_value_arithmetic_checked(
                NumericArithmeticOp::Rem,
                &Value::Decimal(lhs),
                &Value::Decimal(rhs),
            ),
            Ok(Some(Value::Decimal(expected))),
        );
    }
    assert_eq!(
        apply_numeric_arithmetic_checked(
            NumericArithmeticOp::Rem,
            &Value::Decimal(value),
            &Value::Decimal(Decimal::ZERO),
        ),
        Err(NumericEvalError::NotRepresentable),
    );
    assert_eq!(
        apply_value_arithmetic_checked(
            NumericArithmeticOp::Rem,
            &Value::Decimal(value),
            &Value::Decimal(Decimal::ZERO),
        ),
        Err(NumericEvalError::NotRepresentable),
    );
}

#[test]
fn decimal_unary_float_operations_preserve_domains_and_representability() {
    let decimal = |value| Decimal::from_i64(value).unwrap();
    let not_representable = Err(NumericEvalError::NotRepresentable);

    assert_eq!(decimal_sqrt_checked(decimal(-1)), not_representable);
    assert_eq!(decimal_sqrt_checked(Decimal::ZERO), Ok(Decimal::ZERO));
    assert_eq!(decimal_sqrt_checked(decimal(4)), Ok(decimal(2)));
    assert_eq!(decimal_cbrt_checked(decimal(-8)), Ok(decimal(-2)));
    assert_eq!(decimal_exp_checked(Decimal::ZERO), Ok(decimal(1)));
    let negative_exp = decimal_exp_checked(decimal(-1)).unwrap();
    assert!(negative_exp > Decimal::ZERO && negative_exp < decimal(1));
    // A finite float result can still exceed the decimal range; infinity also
    // fails with the same typed conversion error.
    assert_eq!(decimal_exp_checked(decimal(100)), not_representable);
    assert_eq!(decimal_exp_checked(decimal(1000)), not_representable);

    for logarithm in [
        decimal_ln_checked,
        decimal_log2_checked,
        decimal_log10_checked,
    ] {
        assert_eq!(logarithm(decimal(-1)), not_representable);
        assert_eq!(logarithm(Decimal::ZERO), not_representable);
        assert_eq!(logarithm(decimal(1)), Ok(Decimal::ZERO));
    }
    assert_eq!(decimal_log2_checked(decimal(8)), Ok(decimal(3)));
    assert_eq!(decimal_log10_checked(decimal(100)), Ok(decimal(2)));
}
