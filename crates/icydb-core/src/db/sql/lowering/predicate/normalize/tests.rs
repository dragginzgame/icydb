//! Constant-fold exhaustion is an error, never an unfolder or partial predicate.

use super::*;
use crate::db::{
    QueryError, RequestExecutionRoot,
    query::plan::expr::{BinaryOp, FieldId, Function},
    resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use crate::value::Value;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

fn compare_to(value: Expr) -> Expr {
    Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::from("label"))),
        right: Box::new(value),
    }
}

fn normalize(expr: Expr, scalar: bool) -> (QueryError, u64) {
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            1_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 4096),
    );
    let error = PreparationWork::run(&root.scope(), Lane::TrustedRead, |work| {
        if scalar {
            normalize_scalar_where_bool_expr(expr, work)
        } else {
            normalize_where_bool_expr(expr, work)
        }
    })
    .unwrap_err();
    (error, root.observed(Resource::TemporaryBytes))
}

#[test]
fn both_where_normalizers_propagate_replacement_exhaustion_and_stop_later_children() {
    let expanding = compare_to(Expr::FunctionCall {
        function: Function::Replace,
        args: vec![
            Expr::Literal(Value::Text("a".repeat(64))),
            Expr::Literal(Value::Text("a".into())),
            Expr::Literal(Value::Text("z".repeat(64))),
        ],
    });
    let later = compare_to(Expr::FunctionCall {
        function: Function::Lower,
        args: vec![Expr::Literal(Value::Text("later".repeat(2000)))],
    });
    for scalar in [false, true] {
        let (first, first_bytes) = normalize(expanding.clone(), scalar);
        let (second, second_bytes) = normalize(
            Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(expanding.clone()),
                right: Box::new(later.clone()),
            },
            scalar,
        );
        assert_eq!(first.diagnostic(), second.diagnostic());
        assert_eq!(first.diagnostic_facts(), second.diagnostic_facts());
        assert!(first_bytes > 4096);
        assert_eq!(first_bytes, second_bytes);
    }
}
