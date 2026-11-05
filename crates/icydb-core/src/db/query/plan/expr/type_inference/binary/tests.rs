use super::*;

#[test]
fn u256_planner_type_is_strictly_comparable_without_numeric_widening() {
    assert!(binary_equality_comparable(&ExprType::U256, &ExprType::U256));
    assert!(binary_order_comparable(&ExprType::U256, &ExprType::U256));
    assert!(!ExprType::U256.is_numeric_eligible());
}

#[test]
fn u256_planner_type_does_not_widen_opaque_ordering() {
    assert!(!binary_order_comparable(
        &ExprType::Opaque,
        &ExprType::Opaque,
    ));
    assert!(!binary_order_comparable(&ExprType::U256, &ExprType::Opaque,));
}
