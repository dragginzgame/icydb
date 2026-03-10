//! Module: db::numeric::tests
//! Responsibility: module-local ownership and contracts for db::numeric::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::numeric::{
        NumericArithmeticOp, apply_numeric_arithmetic, coerce_numeric_decimal, compare_numeric_eq,
        compare_numeric_order, field_kind_supports_aggregate_numeric,
        field_kind_supports_expr_numeric,
    },
    model::field::FieldKind,
    types::{Decimal, Int},
    value::Value,
};
use std::cmp::Ordering;

#[test]
fn expr_numeric_domain_matches_bootstrap_contract() {
    assert!(field_kind_supports_expr_numeric(&FieldKind::Int));
    assert!(field_kind_supports_expr_numeric(&FieldKind::Uint));
    assert!(field_kind_supports_expr_numeric(&FieldKind::Float64));
    assert!(field_kind_supports_expr_numeric(&FieldKind::Decimal {
        scale: 2
    }));
    assert!(field_kind_supports_expr_numeric(&FieldKind::Timestamp));
    assert!(field_kind_supports_expr_numeric(&FieldKind::Duration));
    assert!(!field_kind_supports_expr_numeric(&FieldKind::Text));
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
