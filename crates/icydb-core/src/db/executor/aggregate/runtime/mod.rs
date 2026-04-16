//! Module: executor::aggregate::runtime
//! Responsibility: aggregate-owned grouped runtime mechanics for read execution.
//! Does not own: grouped route derivation or shared executor contracts.
//! Boundary: grouped fold/distinct/having/output execution for grouped read paths.

mod grouped_distinct;
mod grouped_fold;
mod grouped_output;

use crate::{
    db::{
        executor::projection::{
            ProjectionEvalError, eval_binary_expr, eval_projection_function_call,
            projection_function_name,
        },
        predicate::{CompareOp, evaluate_grouped_having_compare},
        query::plan::{
            FieldSlot, GroupHavingClause, GroupHavingExpr, GroupHavingSpec, GroupHavingSymbol,
            GroupHavingValueExpr,
        },
    },
    error::InternalError,
    value::Value,
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

// Evaluate one grouped HAVING expression on one finalized grouped output row.
pub(in crate::db::executor) fn group_matches_having(
    having: &GroupHavingSpec,
    group_fields: &[FieldSlot],
    group_key_value: &Value,
    aggregate_values: &[Value],
) -> Result<bool, InternalError> {
    eval_group_having_expr(
        &GroupHavingExpr::from_legacy_spec(having),
        group_fields,
        group_key_value,
        aggregate_values,
    )
}

pub(in crate::db::executor) fn group_matches_having_expr(
    expr: &GroupHavingExpr,
    group_fields: &[FieldSlot],
    group_key_value: &Value,
    aggregate_values: &[Value],
) -> Result<bool, InternalError> {
    eval_group_having_expr(expr, group_fields, group_key_value, aggregate_values)
}

fn eval_group_having_expr(
    expr: &GroupHavingExpr,
    group_fields: &[FieldSlot],
    group_key_value: &Value,
    aggregate_values: &[Value],
) -> Result<bool, InternalError> {
    match expr {
        GroupHavingExpr::Compare { left, op, right } => {
            let actual = eval_group_having_value_expr(
                left,
                group_fields,
                group_key_value,
                aggregate_values,
                0,
            )?;
            let expected = eval_group_having_value_expr(
                right,
                group_fields,
                group_key_value,
                aggregate_values,
                0,
            )?;

            having_compare_values(&actual, *op, &expected)
        }
        GroupHavingExpr::And(children) => {
            for child in children {
                if !eval_group_having_expr(child, group_fields, group_key_value, aggregate_values)?
                {
                    return Ok(false);
                }
            }

            Ok(true)
        }
    }
}

fn eval_group_having_value_expr(
    expr: &GroupHavingValueExpr,
    group_fields: &[FieldSlot],
    group_key_value: &Value,
    aggregate_values: &[Value],
    compare_index: usize,
) -> Result<Value, InternalError> {
    match expr {
        GroupHavingValueExpr::GroupField(field_slot) => resolve_group_having_group_field_value(
            field_slot,
            group_fields,
            group_key_value,
            compare_index,
        )
        .cloned(),
        GroupHavingValueExpr::AggregateIndex(aggregate_index) => aggregate_values
            .get(*aggregate_index)
            .cloned()
            .ok_or_else(|| {
                GroupHavingSymbol::aggregate_index_out_of_bounds(
                    compare_index,
                    *aggregate_index,
                    aggregate_values.len(),
                )
            }),
        GroupHavingValueExpr::Literal(value) => Ok(value.clone()),
        GroupHavingValueExpr::FunctionCall { function, args } => {
            let mut evaluated_args = Vec::with_capacity(args.len());
            for arg in args {
                evaluated_args.push(eval_group_having_value_expr(
                    arg,
                    group_fields,
                    group_key_value,
                    aggregate_values,
                    compare_index,
                )?);
            }

            eval_projection_function_call(*function, evaluated_args.as_slice()).map_err(|err| {
                ProjectionEvalError::InvalidFunctionCall {
                    function: projection_function_name(*function).to_string(),
                    message: err.to_string(),
                }
                .into_grouped_projection_internal_error()
            })
        }
        GroupHavingValueExpr::Binary { op, left, right } => {
            let left = eval_group_having_value_expr(
                left,
                group_fields,
                group_key_value,
                aggregate_values,
                compare_index,
            )?;
            let right = eval_group_having_value_expr(
                right,
                group_fields,
                group_key_value,
                aggregate_values,
                compare_index,
            )?;

            eval_binary_expr(*op, &left, &right)
                .map_err(ProjectionEvalError::into_grouped_projection_internal_error)
        }
    }
}

fn resolve_group_having_group_field_value<'a>(
    field_slot: &FieldSlot,
    group_fields: &[FieldSlot],
    group_key_value: &'a Value,
    compare_index: usize,
) -> Result<&'a Value, InternalError> {
    let group_key_list = match group_key_value {
        Value::List(values) => values,
        value => return Err(GroupHavingSymbol::grouped_key_must_be_list(value)),
    };
    let Some(group_field_offset) = group_fields
        .iter()
        .position(|group_field| group_field.index() == field_slot.index())
    else {
        return Err(GroupHavingSymbol::field_not_in_group_key_projection(
            field_slot.field(),
        ));
    };

    group_key_list.get(group_field_offset).ok_or_else(|| {
        GroupHavingSymbol::group_key_offset_out_of_bounds(
            compare_index,
            group_field_offset,
            group_key_list.len(),
        )
    })
}

// Evaluate one grouped HAVING compare operator using strict value semantics.
fn having_compare_values(
    actual: &Value,
    op: CompareOp,
    expected: &Value,
) -> Result<bool, InternalError> {
    let Some(matches) = evaluate_grouped_having_compare(actual, op, expected) else {
        return Err(GroupHavingClause::unsupported_operator(op));
    };

    Ok(matches)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::eval_group_having_expr;
    use crate::{
        db::query::plan::{
            FieldSlot, GroupHavingExpr, GroupHavingValueExpr,
            expr::{BinaryOp, Function},
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

        let matched = eval_group_having_expr(
            &expr,
            &[],
            &Value::List(Vec::new()),
            &[Value::Decimal(Decimal::new(10049, 3))],
        )
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

        let matched =
            eval_group_having_expr(&expr, &[], &Value::List(Vec::new()), &[Value::Uint(5)])
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

        let matched = eval_group_having_expr(
            &expr,
            &[group_field],
            &Value::List(vec![Value::Text("Mage".to_string())]),
            &[Value::Uint(11)],
        )
        .expect("grouped HAVING AND expression should evaluate");

        assert!(matched);
    }
}
