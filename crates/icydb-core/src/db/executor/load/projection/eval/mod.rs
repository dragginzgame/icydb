//! Module: executor::load::projection::eval
//! Responsibility: evaluate planned projection expressions against runtime row contexts.
//! Does not own: expression type inference or planner semantic validation policy.
//! Boundary: provides executor-side projection evaluation and typed evaluation errors.

mod operators;

use crate::{
    db::executor::load::projection::grouped::GroupedRowView,
    db::query::plan::expr::Expr,
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    value::Value,
};
use thiserror::Error as ThisError;

///
/// ProjectionEvalError
///
/// Pure expression-evaluation failures for scalar projection execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db::executor) enum ProjectionEvalError {
    #[error("projection expression references unknown field '{field}'")]
    UnknownField { field: String },

    #[error("projection expression could not read field '{field}' at index={index}")]
    MissingFieldValue { field: String, index: usize },

    #[error("projection expression cannot evaluate aggregate '{kind}' in scalar row context")]
    AggregateNotEvaluable { kind: String },

    #[error("projection unary operator '{op}' is incompatible with operand value {found:?}")]
    InvalidUnaryOperand { op: String, found: Box<Value> },

    #[error(
        "projection binary operator '{op}' is incompatible with operand values ({left:?}, {right:?})"
    )]
    InvalidBinaryOperands {
        op: String,
        left: Box<Value>,
        right: Box<Value>,
    },

    #[error(
        "grouped projection expression references unknown aggregate expression kind={kind} target_field={target_field:?} distinct={distinct}"
    )]
    UnknownGroupedAggregateExpression {
        kind: String,
        target_field: Option<String>,
        distinct: bool,
    },

    #[error(
        "grouped projection expression references aggregate output index={aggregate_index} but only {aggregate_count} outputs are available"
    )]
    MissingGroupedAggregateValue {
        aggregate_index: usize,
        aggregate_count: usize,
    },
}

/// Evaluate one projection expression against one entity row.
pub(in crate::db::executor) fn eval_expr<E>(
    expr: &Expr,
    row: &E,
) -> Result<Value, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(field_index) = resolve_field_slot(E::MODEL, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                });
            };
            let Some(value) = row.get_value_by_index(field_index) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field_name.to_string(),
                    index: field_index,
                });
            };

            Ok(value)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr(expr.as_ref(), row)?;
            operators::eval_unary_expr(*op, operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr(left.as_ref(), row)?;
            let right_value = eval_expr(right.as_ref(), row)?;

            operators::eval_binary_expr(*op, left_value, right_value)
        }
        Expr::Aggregate(aggregate) => Err(ProjectionEvalError::AggregateNotEvaluable {
            kind: format!("{:?}", aggregate.kind()),
        }),
        Expr::Alias { expr, .. } => eval_expr(expr.as_ref(), row),
    }
}

/// Evaluate one projection expression against one grouped output row view.
pub(in crate::db::executor) fn eval_expr_grouped(
    expr: &Expr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Value, ProjectionEvalError> {
    match expr {
        Expr::Field(field_id) => {
            let Some(group_field_offset) =
                super::grouped::resolve_group_field_offset(grouped_row, field_id.as_str())
            else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_id.as_str().to_string(),
                });
            };
            let Some(value) = grouped_row.key_values.get(group_field_offset) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field_id.as_str().to_string(),
                    index: group_field_offset,
                });
            };

            Ok(value.clone())
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr_grouped(expr.as_ref(), grouped_row)?;
            operators::eval_unary_expr(*op, operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr_grouped(left.as_ref(), grouped_row)?;
            let right_value = eval_expr_grouped(right.as_ref(), grouped_row)?;

            operators::eval_binary_expr(*op, left_value, right_value)
        }
        Expr::Aggregate(aggregate_expr) => {
            let Some(aggregate_index) =
                super::grouped::resolve_grouped_aggregate_index(grouped_row, aggregate_expr)
            else {
                return Err(ProjectionEvalError::UnknownGroupedAggregateExpression {
                    kind: format!("{:?}", aggregate_expr.kind()),
                    target_field: aggregate_expr.target_field().map(str::to_string),
                    distinct: aggregate_expr.is_distinct(),
                });
            };
            let Some(value) = grouped_row.aggregate_values.get(aggregate_index) else {
                return Err(ProjectionEvalError::MissingGroupedAggregateValue {
                    aggregate_index,
                    aggregate_count: grouped_row.aggregate_values.len(),
                });
            };

            Ok(value.clone())
        }
        Expr::Alias { expr, .. } => eval_expr_grouped(expr.as_ref(), grouped_row),
    }
}
