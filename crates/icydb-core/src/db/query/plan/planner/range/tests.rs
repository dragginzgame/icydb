use crate::{
    db::{
        numeric::compare_numeric_or_strict_order,
        query::plan::planner::range::bounds::compare_range_bound_values,
    },
    value::Value,
};
use std::cmp::Ordering;

#[test]
fn range_merges_move_text_bounds_and_keep_strict_ties() {
    use super::{
        RangeConstraint,
        bounds::{merge_range_constraint, merge_range_constraint_bounds},
    };
    use crate::db::predicate::CompareOp;
    use std::ops::Bound;

    let lower = "lower".repeat(128);
    let upper = "upper".repeat(128);
    let lower_pointer = lower.as_ptr();
    let upper_pointer = upper.as_ptr();
    let mut range = RangeConstraint::default();
    assert!(merge_range_constraint(
        &mut range,
        CompareOp::Gte,
        Value::Text(lower)
    ));
    assert!(merge_range_constraint(
        &mut range,
        CompareOp::Lt,
        Value::Text(upper)
    ));
    let Bound::Included(Value::Text(lower)) = &range.lower else {
        panic!("included lower");
    };
    assert_eq!(lower.as_ptr(), lower_pointer);
    let Bound::Excluded(Value::Text(upper)) = &range.upper else {
        panic!("excluded upper");
    };
    assert_eq!(upper.as_ptr(), upper_pointer);

    let stricter = "lower".repeat(128);
    let stricter_pointer = stricter.as_ptr();
    assert!(merge_range_constraint_bounds(
        &mut range,
        RangeConstraint {
            lower: Bound::Excluded(Value::Text(stricter)),
            upper: Bound::Unbounded,
        }
    ));
    let Bound::Excluded(Value::Text(lower)) = &range.lower else {
        panic!("exclusive tie wins");
    };
    assert_eq!(lower.as_ptr(), stricter_pointer);
    // A weaker bound leaves the retained upper allocation unchanged.
    assert!(merge_range_constraint(
        &mut range,
        CompareOp::Lte,
        Value::Text("z".into())
    ));
    let Bound::Excluded(Value::Text(upper)) = &range.upper else {
        panic!("retained upper");
    };
    assert_eq!(upper.as_ptr(), upper_pointer);
}

#[test]
fn range_merges_preserve_singleton_empty_and_incomparable_intervals() {
    use super::{RangeConstraint, bounds::merge_range_constraint};
    use crate::db::predicate::CompareOp;

    for lower in [CompareOp::Gt, CompareOp::Gte] {
        for upper in [CompareOp::Lt, CompareOp::Lte] {
            let mut range = RangeConstraint::default();
            assert!(merge_range_constraint(&mut range, lower, Value::Int64(7)));
            assert_eq!(
                merge_range_constraint(&mut range, upper, Value::Nat64(7)),
                lower == CompareOp::Gte && upper == CompareOp::Lte
            );
        }
    }
    let mut range = RangeConstraint::default();
    assert!(merge_range_constraint(
        &mut range,
        CompareOp::Gte,
        Value::Text("a".into())
    ));
    assert!(!merge_range_constraint(
        &mut range,
        CompareOp::Lt,
        Value::Nat64(9)
    ));
}

#[test]
fn range_bound_numeric_compare_reuses_shared_numeric_authority() {
    let left = Value::Int64(10);
    let right = Value::Nat64(10);

    assert_eq!(
        compare_range_bound_values(&left, &right),
        compare_numeric_or_strict_order(&left, &right),
        "planner range numeric bounds should delegate to shared numeric comparator",
    );
}

#[test]
fn range_bound_mixed_non_numeric_values_are_incomparable() {
    assert_eq!(
        compare_range_bound_values(&Value::Text("x".to_string()), &Value::Nat64(1)),
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
