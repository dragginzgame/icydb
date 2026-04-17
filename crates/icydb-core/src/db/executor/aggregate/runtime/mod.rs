//! Module: executor::aggregate::runtime
//! Responsibility: aggregate-owned grouped runtime mechanics for read execution.
//! Does not own: grouped route derivation or shared executor contracts.
//! Boundary: grouped fold/distinct/having/output execution for grouped read paths.

mod grouped_distinct;
mod grouped_fold;
mod grouped_output;

use crate::{
    db::executor::projection::{
        GroupedProjectionExpr, GroupedRowView, ProjectionEvalError, evaluate_grouped_having_expr,
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

// Evaluate one compiled grouped HAVING expression on one finalized grouped output row.
pub(in crate::db::executor) fn group_matches_having_expr(
    expr: &GroupedProjectionExpr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<bool, InternalError> {
    evaluate_grouped_having_expr(expr, grouped_row)
        .map_err(ProjectionEvalError::into_grouped_projection_internal_error)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::group_matches_having_expr;
    use crate::{
        db::{
            executor::projection::{GroupedRowView, compile_grouped_having_expr},
            query::plan::{
                FieldSlot, GroupHavingExpr, GroupHavingValueExpr,
                expr::{BinaryOp, Function},
            },
        },
        types::Decimal,
        value::Value,
    };

    #[test]
    fn grouped_having_runtime_accepts_post_aggregate_round_compare() {
        let expr = GroupHavingExpr::Compare {
            left: GroupHavingValueExpr::FunctionCall {
                function: Function::Round,
                args: vec![
                    GroupHavingValueExpr::AggregateIndex(0),
                    GroupHavingValueExpr::Literal(Value::Uint(2)),
                ],
            },
            op: crate::db::predicate::CompareOp::Gte,
            right: GroupHavingValueExpr::Literal(Value::Decimal(Decimal::new(1000, 2))),
        };

        let compiled = compile_grouped_having_expr(&expr, &[])
            .expect("grouped HAVING ROUND compare should compile");
        let aggregate_values = [Value::Decimal(Decimal::new(10049, 3))];
        let grouped_row = GroupedRowView::new(&[], &aggregate_values, &[], &[]);
        let matched = group_matches_having_expr(&compiled, &grouped_row)
            .expect("grouped HAVING ROUND compare should evaluate");

        assert!(matched);
    }

    #[test]
    fn grouped_having_runtime_accepts_post_aggregate_arithmetic_compare() {
        let expr = GroupHavingExpr::Compare {
            left: GroupHavingValueExpr::Binary {
                op: BinaryOp::Add,
                left: Box::new(GroupHavingValueExpr::AggregateIndex(0)),
                right: Box::new(GroupHavingValueExpr::Literal(Value::Uint(1))),
            },
            op: crate::db::predicate::CompareOp::Gt,
            right: GroupHavingValueExpr::Literal(Value::Uint(5)),
        };

        let compiled = compile_grouped_having_expr(&expr, &[])
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
        let expr = GroupHavingExpr::And(vec![
            GroupHavingExpr::Compare {
                left: GroupHavingValueExpr::GroupField(group_field.clone()),
                op: crate::db::predicate::CompareOp::Eq,
                right: GroupHavingValueExpr::Literal(Value::Text("Mage".to_string())),
            },
            GroupHavingExpr::Compare {
                left: GroupHavingValueExpr::AggregateIndex(0),
                op: crate::db::predicate::CompareOp::Gt,
                right: GroupHavingValueExpr::Literal(Value::Uint(10)),
            },
        ]);

        let group_fields = [group_field];
        let compiled = compile_grouped_having_expr(&expr, &group_fields)
            .expect("grouped HAVING AND expression should compile");
        let group_key_values = [Value::Text("Mage".to_string())];
        let aggregate_values = [Value::Uint(11)];
        let grouped_row =
            GroupedRowView::new(&group_key_values, &aggregate_values, &group_fields, &[]);
        let matched = group_matches_having_expr(&compiled, &grouped_row)
            .expect("grouped HAVING AND expression should evaluate");

        assert!(matched);
    }
}
