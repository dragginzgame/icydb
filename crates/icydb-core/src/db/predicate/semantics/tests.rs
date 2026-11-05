use super::{compare_eq, compare_order, eval_equality_compare_result, eval_ordered_compare_result};
use crate::{
    db::predicate::{CoercionId, CoercionSpec, CompareOp},
    value::Value,
};
use std::cmp::Ordering;

#[test]
fn unit_comparisons_follow_singleton_total_order_semantics() {
    let coercion = CoercionSpec::new(CoercionId::Strict);
    let equality = compare_eq(&Value::Unit, &Value::Unit, &coercion);
    let ordering = compare_order(&Value::Unit, &Value::Unit, &coercion);

    assert!(eval_equality_compare_result(CompareOp::Eq, equality));
    assert_eq!(ordering, Some(Ordering::Equal));
    for (op, expected) in [
        (CompareOp::Eq, true),
        (CompareOp::Lt, false),
        (CompareOp::Lte, true),
        (CompareOp::Gt, false),
        (CompareOp::Gte, true),
    ] {
        assert_eq!(
            ordering.map(|ordering| eval_ordered_compare_result(op, ordering)),
            Some(expected),
            "unexpected Unit comparison result for {op:?}",
        );
    }
}
