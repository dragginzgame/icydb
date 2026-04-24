//! Module: db::numeric::tests
//! Covers numeric comparison, coercion, and ordering helper behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        numeric::{
            NumericArithmeticOp, add_decimal_terms, apply_numeric_arithmetic,
            average_decimal_terms, canonical_value_compare, coerce_numeric_decimal,
            compare_numeric_eq, compare_numeric_or_strict_order, compare_numeric_order,
            divide_decimal_terms, field_kind_supports_aggregate_numeric,
        },
        query::plan::expr::classify_field_kind,
    },
    model::field::FieldKind,
    types::{Decimal, Int},
    value::Value,
};
use std::cmp::Ordering;

#[test]
fn expr_numeric_domain_matches_bootstrap_contract() {
    assert!(classify_field_kind(&FieldKind::Int).supports_expr_numeric());
    assert!(classify_field_kind(&FieldKind::Uint).supports_expr_numeric());
    assert!(classify_field_kind(&FieldKind::Float64).supports_expr_numeric());
    assert!(classify_field_kind(&FieldKind::Decimal { scale: 2 }).supports_expr_numeric());
    assert!(classify_field_kind(&FieldKind::Timestamp).supports_expr_numeric());
    assert!(classify_field_kind(&FieldKind::Duration).supports_expr_numeric());
    assert!(!classify_field_kind(&FieldKind::Text).supports_expr_numeric());
}

#[test]
fn aggregate_numeric_domain_keeps_duration_and_timestamp() {
    assert!(field_kind_supports_aggregate_numeric(&FieldKind::Int));
    assert!(field_kind_supports_aggregate_numeric(&FieldKind::Duration));
    assert!(field_kind_supports_aggregate_numeric(&FieldKind::Timestamp));
    assert!(!field_kind_supports_aggregate_numeric(&FieldKind::Text));
}

#[test]
fn numeric_compare_helpers_follow_numeric_widen_domain() {
    assert_eq!(
        compare_numeric_order(&Value::Int(2), &Value::Uint(2)),
        Some(Ordering::Equal)
    );
    assert_eq!(
        compare_numeric_eq(&Value::Int(2), &Value::Uint(2)),
        Some(true)
    );
    assert_eq!(
        compare_numeric_order(&Value::Text("x".to_string()), &Value::Text("x".to_string())),
        None
    );
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
fn numeric_or_strict_compare_prefers_numeric_widen_when_available() {
    assert_eq!(
        compare_numeric_or_strict_order(&Value::Int(2), &Value::Uint(2)),
        Some(Ordering::Equal)
    );
}

#[test]
fn canonical_value_ordering_uses_value_canonical_order() {
    assert_eq!(
        canonical_value_compare(&Value::Uint(7), &Value::Uint(8)),
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
        canonical_value_compare(&Value::Int(7), &Value::Uint(7)),
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

    let result = apply_numeric_arithmetic(NumericArithmeticOp::Add, &left, &right)
        .expect("mixed integer/decimal arithmetic should coerce into decimal domain");

    assert_eq!(result, Decimal::new(35, 1));
}

#[test]
fn numeric_arithmetic_division_rounds_half_away_from_zero() {
    let left = Value::Int(-1);
    let right = Value::Int(6);

    let result = apply_numeric_arithmetic(NumericArithmeticOp::Div, &left, &right)
        .expect("numeric division should produce deterministic decimal output");

    assert_eq!(
        result,
        Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18)
    );
}

#[test]
fn numeric_arithmetic_addition_saturates_on_overflow() {
    let left = Value::Decimal(Decimal::from_i128_with_scale(i128::MAX, 0));
    let right = Value::Int(1);

    let result = apply_numeric_arithmetic(NumericArithmeticOp::Add, &left, &right)
        .expect("saturating decimal arithmetic should return a value");

    assert_eq!(result, Decimal::from_i128_with_scale(i128::MAX, 0));
}

#[test]
fn decimal_term_helpers_share_canonical_add_and_divide_semantics() {
    let saturated = add_decimal_terms(
        Decimal::from_i128_with_scale(i128::MAX, 0),
        Decimal::from_i128_with_scale(1, 0),
    );
    let divided = divide_decimal_terms(
        Decimal::from_num(-1_i64).expect("sum decimal"),
        Decimal::from_num(6_u64).expect("divisor decimal"),
    );

    assert_eq!(saturated, Decimal::from_i128_with_scale(i128::MAX, 0));
    assert_eq!(
        divided,
        Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18)
    );
}

#[test]
fn average_decimal_terms_uses_canonical_division_and_count_coercion() {
    let avg = average_decimal_terms(Decimal::from_num(65_u64).expect("sum decimal"), 3_u64)
        .expect("count should coerce into decimal divisor");

    assert_eq!(
        avg,
        Decimal::from_i128_with_scale(21_666_666_666_666_666_667, 18)
    );
}
