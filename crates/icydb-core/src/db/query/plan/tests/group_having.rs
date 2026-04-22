//! Module: db::query::plan::tests::group_having
//! Covers grouped HAVING comparison semantics that are part of the planner's
//! grouped contract surface.
//! Does not own: generic predicate semantics beyond the grouped HAVING
//! compatibility lane.
//! Boundary: keeps grouped HAVING regression coverage in the plan owner
//! `tests/` boundary instead of under one leaf helper file.

use crate::{
    db::{
        predicate::CompareOp,
        query::plan::{
            expr::BinaryOp, grouped_having_binary_compare_op,
            semantics::evaluate_grouped_having_compare,
        },
    },
    value::Value,
};

#[test]
fn grouped_having_numeric_equality_uses_numeric_widen_semantics() {
    let matched = evaluate_grouped_having_compare(&Value::Uint(7), CompareOp::Eq, &Value::Int(7))
        .expect("eq should be supported");

    assert!(matched);
}

#[test]
fn grouped_having_numeric_ordering_uses_numeric_widen_semantics() {
    let matched = evaluate_grouped_having_compare(&Value::Uint(2), CompareOp::Lt, &Value::Int(3))
        .expect("lt should be supported");

    assert!(matched);
}

#[test]
fn grouped_having_numeric_vs_non_numeric_is_fail_closed() {
    let matched = evaluate_grouped_having_compare(
        &Value::Uint(7),
        CompareOp::Eq,
        &Value::Text("7".to_string()),
    )
    .expect("eq should be supported");

    assert!(!matched);
}

#[test]
fn grouped_having_null_eq_matches_only_null_values() {
    let null_eq = evaluate_grouped_having_compare(&Value::Null, CompareOp::Eq, &Value::Null)
        .expect("eq should be supported");
    let uint_eq = evaluate_grouped_having_compare(&Value::Uint(7), CompareOp::Eq, &Value::Null)
        .expect("eq should be supported");

    assert!(null_eq);
    assert!(!uint_eq);
}

#[test]
fn grouped_having_null_ne_matches_only_non_null_values() {
    let null_ne = evaluate_grouped_having_compare(&Value::Null, CompareOp::Ne, &Value::Null)
        .expect("ne should be supported");
    let uint_ne = evaluate_grouped_having_compare(&Value::Uint(7), CompareOp::Ne, &Value::Null)
        .expect("ne should be supported");

    assert!(!null_ne);
    assert!(uint_ne);
}

#[test]
fn grouped_having_binary_compare_family_matches_planner_grouped_compare_support() {
    assert_eq!(
        grouped_having_binary_compare_op(BinaryOp::Eq),
        Some(CompareOp::Eq),
    );
    assert_eq!(
        grouped_having_binary_compare_op(BinaryOp::Gte),
        Some(CompareOp::Gte),
    );
    assert_eq!(grouped_having_binary_compare_op(BinaryOp::And), None);
    assert_eq!(grouped_having_binary_compare_op(BinaryOp::Add), None);
}
