use super::*;
use crate::{
    db::sql::parser::{SqlAggregateKind, SqlExprBinaryOp},
    value::{test_hash_budget_error, with_test_hash_override},
};

fn aggregate() -> SqlAggregateCall {
    SqlAggregateCall {
        kind: SqlAggregateKind::Sum,
        input: Some(Box::new(SqlExpr::Literal(Value::Nat64(7)))),
        filter_expr: None,
        distinct: false,
    }
}

fn assert_hash_failure(error: SqlLoweringError) {
    let SqlLoweringError::Query(error) = error else {
        panic!("hash error must retain the execution diagnostic");
    };
    assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
    assert_eq!(
        error.diagnostic_facts(),
        test_hash_budget_error().diagnostic_facts()
    );
}

#[test]
fn failed_aggregate_hash_does_not_intern_a_substitute_identity() {
    let expr = SqlExpr::Binary {
        op: SqlExprBinaryOp::Add,
        left: Box::new(SqlExpr::Aggregate(aggregate())),
        right: Box::new(SqlExpr::Aggregate(aggregate())),
    };
    let mut interner = SqlAggregateCallInterner::new();
    let mut calls = Vec::new();
    with_test_hash_override(
        Err(test_hash_budget_error),
        std::panic::AssertUnwindSafe(|| {
            assert_hash_failure(interner.extend_expr(&mut calls, &expr).unwrap_err());
            assert!(interner.indices_by_fingerprint.is_empty());
            assert!(calls.is_empty());
            assert_hash_failure(
                SqlAggregateCallInterner::from_existing(&[aggregate()])
                    .err()
                    .unwrap(),
            );
        }),
    );
    interner.extend_expr(&mut calls, &expr).unwrap();
    assert_eq!(calls, vec![aggregate()]);
}
