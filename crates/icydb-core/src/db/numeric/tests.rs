//! Module: db::numeric::tests
//! Covers numeric comparison, coercion, and ordering helper behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::numeric::{
        NumericArithmeticOp, NumericEvalError, add_decimal_terms_checked,
        apply_numeric_arithmetic_checked, average_decimal_terms_checked, canonical_value_compare,
        coerce_numeric_decimal, compare_numeric_eq, compare_numeric_or_strict_order,
        compare_numeric_order, divide_decimal_terms_checked,
    },
    types::{Decimal, Float64 as F64, Int},
    value::Value,
};
use std::cmp::Ordering;

#[test]
fn numeric_compare_helpers_follow_numeric_widen_domain() {
    assert_eq!(
        compare_numeric_order(&Value::Int(2), &Value::Nat(2)),
        Some(Ordering::Equal)
    );
    assert_eq!(
        compare_numeric_eq(&Value::Int(2), &Value::Nat(2)),
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
        (Value::Int(42), Value::Nat(42)),
        (
            Value::Decimal(Decimal::from_i64(10).expect("decimal")),
            Value::Float64(F64::try_new(10.0).expect("finite float")),
        ),
        (
            Value::Int(9_007_199_254_740_993),
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
        compare_numeric_order(&Value::Int(2), &Value::Text("2".to_string())),
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
        Value::Int(4),
        Value::Nat(4),
        Value::Decimal(Decimal::new(40, 1)),
        Value::Float64(F64::try_new(4.0).expect("finite float")),
        Value::Text("x".to_string()),
        Value::IntBig(Int::from(4i32)),
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
        compare_numeric_or_strict_order(&Value::Int(2), &Value::Nat(2)),
        Some(Ordering::Equal)
    );
}

#[test]
fn canonical_value_ordering_uses_value_canonical_order() {
    assert_eq!(
        canonical_value_compare(&Value::Nat(7), &Value::Nat(8)),
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
        canonical_value_compare(&Value::Int(7), &Value::Nat(7)),
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
    assert!(coerce_numeric_decimal(&Value::Int(4)).is_some());
    assert!(coerce_numeric_decimal(&Value::Text("x".to_string())).is_none());
    assert!(coerce_numeric_decimal(&Value::IntBig(Int::from(4i32))).is_none());
}

#[test]
fn numeric_arithmetic_promotes_integer_and_decimal_to_decimal_domain() {
    let left = Value::Int(2);
    let right = Value::Decimal(Decimal::new(15, 1));

    let result = apply_numeric_arithmetic_checked(NumericArithmeticOp::Add, &left, &right)
        .expect("mixed integer/decimal arithmetic should coerce into decimal domain");

    assert_eq!(result, Some(Decimal::new(35, 1)));
}

#[test]
fn numeric_arithmetic_division_rounds_half_away_from_zero() {
    let left = Value::Int(-1);
    let right = Value::Int(6);

    let result = apply_numeric_arithmetic_checked(NumericArithmeticOp::Div, &left, &right)
        .expect("numeric division should produce deterministic decimal output");

    assert_eq!(
        result,
        Some(Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18))
    );
}

#[test]
fn numeric_arithmetic_addition_reports_overflow() {
    let left = Value::Decimal(Decimal::from_i128_with_scale(i128::MAX, 0));
    let right = Value::Int(1);

    let err = apply_numeric_arithmetic_checked(NumericArithmeticOp::Add, &left, &right)
        .expect_err("checked numeric addition should reject overflow");

    assert_eq!(err, NumericEvalError::Overflow);
}

#[test]
fn checked_numeric_arithmetic_reports_overflow() {
    let left = Value::Decimal(Decimal::from_i128_with_scale(i128::MAX, 0));
    let right = Value::Int(1);

    let err = apply_numeric_arithmetic_checked(NumericArithmeticOp::Add, &left, &right)
        .expect_err("checked numeric addition should reject overflow");

    assert_eq!(err, NumericEvalError::Overflow);
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
