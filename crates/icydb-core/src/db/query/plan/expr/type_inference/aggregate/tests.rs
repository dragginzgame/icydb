use super::sum_like_input_type_supported;
use crate::db::query::plan::{AggregateKind, expr::type_inference::ExprType};

#[test]
fn u256_is_admitted_only_by_sum() {
    assert!(sum_like_input_type_supported(
        AggregateKind::Sum,
        &ExprType::U256,
    ));
    assert!(!sum_like_input_type_supported(
        AggregateKind::Avg,
        &ExprType::U256,
    ));
}
