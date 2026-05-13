use crate::{
    db::{
        numeric::compare_numeric_or_strict_order,
        query::plan::planner::range::bounds::compare_range_bound_values,
    },
    value::Value,
};
use std::cmp::Ordering;

#[test]
fn range_bound_numeric_compare_reuses_shared_numeric_authority() {
    let left = Value::Int(10);
    let right = Value::Nat(10);

    assert_eq!(
        compare_range_bound_values(&left, &right),
        compare_numeric_or_strict_order(&left, &right),
        "planner range numeric bounds should delegate to shared numeric comparator",
    );
}

#[test]
fn range_bound_mixed_non_numeric_values_are_incomparable() {
    assert_eq!(
        compare_range_bound_values(&Value::Text("x".to_string()), &Value::Nat(1)),
        None,
        "mixed non-numeric variants should remain incomparable in range planning",
    );
}

#[test]
fn range_bound_same_variant_non_numeric_uses_strict_ordering() {
    assert_eq!(
        compare_range_bound_values(&Value::Text("a".to_string()), &Value::Text("b".to_string())),
        Some(Ordering::Less),
        "same-variant non-numeric bounds should use strict value ordering",
    );
}
