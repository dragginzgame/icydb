//! Module: executor::projection::eval
//! Responsibility: generic and grouped projection expression evaluation against runtime row contexts.
//! Does not own: scalar-compiled projection execution, expression type inference, or planner semantic validation policy.
//! Boundary: provides executor-side generic evaluation and shared projection error taxonomy.

mod operators;
mod scalar;

use crate::{
    db::{executor::projection::grouped::GroupedRowView, query::plan::expr::Expr},
    error::InternalError,
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};
use thiserror::Error as ThisError;

#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_canonical_scalar_projection_expr;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use scalar::eval_canonical_scalar_projection_expr_with_required_value_reader;
#[cfg(test)]
pub(in crate::db::executor) use scalar::{ScalarProjectionEvalError, eval_scalar_projection_expr};
pub(in crate::db::executor) use scalar::{
    ScalarProjectionExpr, compile_scalar_projection_expr,
    eval_scalar_projection_expr_with_value_reader,
};

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

impl ProjectionEvalError {
    /// Map one projection evaluation failure into the executor invalid-logical-plan boundary.
    pub(in crate::db::executor) fn into_invalid_logical_plan_internal_error(self) -> InternalError {
        InternalError::query_invalid_logical_plan(self.to_string())
    }

    /// Map one grouped projection evaluation failure into the grouped-output
    /// invalid-logical-plan boundary while preserving grouped context.
    pub(in crate::db::executor) fn into_grouped_projection_internal_error(self) -> InternalError {
        InternalError::query_invalid_logical_plan(format!(
            "grouped projection evaluation failed: {self}",
        ))
    }
}

/// Evaluate one projection expression through one runtime slot reader.
pub(in crate::db::executor) fn eval_expr_with_slot_reader(
    expr: &Expr,
    model: &EntityModel,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, ProjectionEvalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(field_index) = resolve_field_slot(model, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                });
            };
            let Some(value) = read_slot(field_index) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field_name.to_string(),
                    index: field_index,
                });
            };

            Ok(value)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr_with_slot_reader(expr.as_ref(), model, read_slot)?;
            operators::eval_unary_expr(*op, operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr_with_slot_reader(left.as_ref(), model, read_slot)?;
            let right_value = eval_expr_with_slot_reader(right.as_ref(), model, read_slot)?;

            operators::eval_binary_expr(*op, left_value, right_value)
        }
        Expr::Aggregate(aggregate) => Err(ProjectionEvalError::AggregateNotEvaluable {
            kind: format!("{:?}", aggregate.kind()),
        }),
        Expr::Alias { expr, .. } => eval_expr_with_slot_reader(expr.as_ref(), model, read_slot),
    }
}

/// Evaluate one projection expression through one required-value reader on the
/// canonical structural row path.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_expr_with_required_value_reader(
    expr: &Expr,
    model: &EntityModel,
    read_slot: &mut dyn FnMut(usize) -> Result<Value, InternalError>,
) -> Result<Value, InternalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(field_index) = resolve_field_slot(model, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                }
                .into_invalid_logical_plan_internal_error());
            };

            read_slot(field_index)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr_with_required_value_reader(expr.as_ref(), model, read_slot)?;
            operators::eval_unary_expr(*op, operand)
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr_with_required_value_reader(left.as_ref(), model, read_slot)?;
            let right_value =
                eval_expr_with_required_value_reader(right.as_ref(), model, read_slot)?;

            operators::eval_binary_expr(*op, left_value, right_value)
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
        }
        Expr::Aggregate(aggregate) => Err(ProjectionEvalError::AggregateNotEvaluable {
            kind: format!("{:?}", aggregate.kind()),
        }
        .into_invalid_logical_plan_internal_error()),
        Expr::Alias { expr, .. } => {
            eval_expr_with_required_value_reader(expr.as_ref(), model, read_slot)
        }
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
