//! Module: executor::aggregate::runtime
//! Responsibility: aggregate-owned grouped runtime mechanics for read execution.
//! Does not own: grouped route derivation or shared executor contracts.
//! Boundary: grouped fold/distinct/having/output execution for grouped read paths.

mod grouped_distinct;
mod grouped_fold;
mod grouped_output;
mod grouped_row;

use crate::{
    db::{
        executor::projection::{GroupedRowView, ProjectionEvalError, evaluate_grouped_having_expr},
        query::plan::expr::CompiledExpr,
    },
    error::InternalError,
};

#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use grouped_fold::{
    GroupedCountFoldMetrics, with_grouped_count_fold_metrics,
};
pub(in crate::db::executor) use grouped_fold::{
    build_grouped_stream_with_runtime, execute_group_fold_stage,
};
pub(in crate::db::executor) use grouped_output::{
    GroupedOutputRuntimeObserverBindings, finalize_grouped_output_with_observer,
    finalize_path_outcome_for_path,
};
pub(in crate::db) use grouped_row::RuntimeGroupedRow;

// Evaluate one compiled grouped HAVING expression on one finalized grouped output row.
pub(in crate::db::executor) fn group_matches_having_expr(
    expr: &CompiledExpr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<bool, InternalError> {
    evaluate_grouped_having_expr(expr, grouped_row)
        .map_err(ProjectionEvalError::into_grouped_projection_internal_error)
}

// Evaluate one global aggregate HAVING expression through the shared grouped
// post-aggregate evaluator on the implicit single aggregate row.
///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::group_matches_having_expr;
    use crate::{
        db::{
            executor::projection::{GroupedRowView, compile_grouped_projection_expr},
            query::{
                builder::aggregate::AggregateExpr,
                plan::{
                    AggregateKind, FieldSlot,
                    expr::{BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp},
                },
            },
        },
        types::Decimal,
        value::Value,
    };

    #[test]
    fn grouped_having_runtime_accepts_post_aggregate_round_compare() {
        let expr = Expr::Binary {
            op: BinaryOp::Gte,
            left: Box::new(Expr::FunctionCall {
                function: Function::Round,
                args: vec![
                    Expr::Aggregate(AggregateExpr::terminal_for_kind(AggregateKind::Count)),
                    Expr::Literal(Value::Uint(2)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(1000, 2)))),
        };
        let specs = [
            crate::db::query::plan::GroupedAggregateExecutionSpec::from_parts_for_test(
                crate::db::query::plan::AggregateKind::Count,
                None,
                None,
                false,
            ),
        ];
        let compiled = compile_grouped_projection_expr(&expr, &[], &specs)
            .expect("grouped HAVING ROUND compare should compile");
        let aggregate_values = [Value::Decimal(Decimal::new(10049, 3))];
        let grouped_row = GroupedRowView::new(&[], &aggregate_values, &[], &[]);
        let matched = group_matches_having_expr(&compiled, &grouped_row)
            .expect("grouped HAVING ROUND compare should evaluate");

        assert!(matched);
    }

    #[test]
    fn grouped_having_runtime_accepts_post_aggregate_arithmetic_compare() {
        let aggregate_expr = AggregateExpr::terminal_for_kind(AggregateKind::Count);
        let expr = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(aggregate_expr)),
                right: Box::new(Expr::Literal(Value::Uint(1))),
            }),
            right: Box::new(Expr::Literal(Value::Uint(5))),
        };
        let specs = [
            crate::db::query::plan::GroupedAggregateExecutionSpec::from_parts_for_test(
                crate::db::query::plan::AggregateKind::Count,
                None,
                None,
                false,
            ),
        ];
        let compiled = compile_grouped_projection_expr(&expr, &[], &specs)
            .expect("grouped HAVING arithmetic compare should compile");
        let aggregate_values = [Value::Uint(5)];
        let grouped_row = GroupedRowView::new(&[], &aggregate_values, &[], &[]);
        let matched = group_matches_having_expr(&compiled, &grouped_row)
            .expect("grouped HAVING arithmetic compare should evaluate");

        assert!(matched);
    }

    #[test]
    fn grouped_having_runtime_accepts_and_over_group_keys_and_aggregates() {
        let group_field = FieldSlot::from_parts_for_test(1, "class_name");
        let aggregate_expr = AggregateExpr::terminal_for_kind(AggregateKind::Count);
        let expr = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new(group_field.field()))),
                right: Box::new(Expr::Literal(Value::Text("Mage".to_string()))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(aggregate_expr)),
                right: Box::new(Expr::Literal(Value::Uint(10))),
            }),
        };

        let group_fields = [group_field];
        let specs = [
            crate::db::query::plan::GroupedAggregateExecutionSpec::from_parts_for_test(
                crate::db::query::plan::AggregateKind::Count,
                None,
                None,
                false,
            ),
        ];
        let compiled = compile_grouped_projection_expr(&expr, &group_fields, &specs)
            .expect("grouped HAVING AND expression should compile");
        let group_key_values = [Value::Text("Mage".to_string())];
        let aggregate_values = [Value::Uint(11)];
        let grouped_row =
            GroupedRowView::new(&group_key_values, &aggregate_values, &group_fields, &[]);
        let matched = group_matches_having_expr(&compiled, &grouped_row)
            .expect("grouped HAVING AND expression should evaluate");

        assert!(matched);
    }

    #[test]
    fn grouped_having_runtime_accepts_post_aggregate_case_and_not() {
        let aggregate_expr = AggregateExpr::terminal_for_kind(AggregateKind::Count);
        let expr = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(
                    Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(Expr::Literal(Value::Bool(false))),
                    },
                    Expr::Aggregate(aggregate_expr),
                )],
                else_expr: Box::new(Expr::Literal(Value::Uint(0))),
            }),
            right: Box::new(Expr::Literal(Value::Uint(5))),
        };
        let specs = [
            crate::db::query::plan::GroupedAggregateExecutionSpec::from_parts_for_test(
                crate::db::query::plan::AggregateKind::Count,
                None,
                None,
                false,
            ),
        ];
        let compiled = compile_grouped_projection_expr(&expr, &[], &specs)
            .expect("grouped HAVING CASE should compile");
        let aggregate_values = [Value::Uint(6)];
        let grouped_row = GroupedRowView::new(&[], &aggregate_values, &[], &[]);
        let matched = group_matches_having_expr(&compiled, &grouped_row)
            .expect("grouped HAVING CASE should evaluate");

        assert!(matched);
    }
}
