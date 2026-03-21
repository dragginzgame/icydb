//! Module: executor::projection::eval
//! Responsibility: evaluate planned projection expressions against runtime row contexts.
//! Does not own: expression type inference or planner semantic validation policy.
//! Boundary: provides executor-side projection evaluation and typed evaluation errors.

mod operators;

use crate::{
    db::{
        data::SlotReader,
        executor::projection::grouped::GroupedRowView,
        query::plan::expr::{BinaryOp, Expr, UnaryOp},
        scalar_expr::{
            ScalarExprValue, ScalarValueProgram, compile_scalar_field_program,
            compile_scalar_literal_expr_value, eval_scalar_value_program,
            scalar_expr_value_into_value,
        },
    },
    error::InternalError,
    model::entity::{EntityModel, resolve_field_slot},
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

///
/// ScalarProjectionEvalError
///
/// ScalarProjectionEvalError preserves the distinction between projection
/// semantic failures and structural slot-decode failures.
/// Structural projection execution uses this split so corruption and invariant
/// diagnostics are not flattened into logical-plan errors.
///

#[derive(Debug)]
pub(in crate::db::executor) enum ScalarProjectionEvalError {
    Eval(ProjectionEvalError),
    Internal(InternalError),
}

///
/// ScalarProjectionExpr
///
/// ScalarProjectionExpr is the compiled scalar-only projection tree used by
/// structural row materialization.
/// Field slots and scalar literals are resolved once so runtime projection
/// evaluation only falls back to generic `Expr` execution when an expression
/// genuinely leaves the scalar seam.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum ScalarProjectionExpr {
    Field(ScalarProjectionField),
    Literal(ScalarExprValue<'static>),
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// ScalarProjectionField
///
/// ScalarProjectionField is one resolved scalar field reference inside a
/// compiled projection expression.
/// It preserves field-name diagnostics while using the shared scalar value
/// program for slot-reader evaluation.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct ScalarProjectionField {
    field: String,
    slot: usize,
    program: ScalarValueProgram,
}

/// Compile one projection expression onto the scalar seam when it never
/// requires non-scalar field decode or aggregate evaluation.
#[must_use]
pub(in crate::db::executor) fn compile_scalar_projection_expr(
    model: &'static EntityModel,
    expr: &Expr,
) -> Option<ScalarProjectionExpr> {
    match expr {
        Expr::Field(field_id) => {
            let slot = resolve_field_slot(model, field_id.as_str())?;
            let program = compile_scalar_field_program(model, field_id.as_str())?;

            Some(ScalarProjectionExpr::Field(ScalarProjectionField {
                field: field_id.as_str().to_string(),
                slot,
                program,
            }))
        }
        Expr::Literal(value) => {
            compile_scalar_literal_expr_value(value).map(ScalarProjectionExpr::Literal)
        }
        Expr::Unary { op, expr } => {
            compile_scalar_projection_expr(model, expr.as_ref()).map(|expr| {
                ScalarProjectionExpr::Unary {
                    op: *op,
                    expr: Box::new(expr),
                }
            })
        }
        Expr::Binary { op, left, right } => {
            let left = compile_scalar_projection_expr(model, left.as_ref())?;
            let right = compile_scalar_projection_expr(model, right.as_ref())?;

            Some(ScalarProjectionExpr::Binary {
                op: *op,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        Expr::Aggregate(_) => None,
        Expr::Alias { expr, .. } => compile_scalar_projection_expr(model, expr.as_ref()),
    }
}

/// Evaluate one compiled scalar projection expression against one slot reader.
pub(in crate::db::executor) fn eval_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &dyn SlotReader,
) -> Result<Value, ScalarProjectionEvalError> {
    match expr {
        ScalarProjectionExpr::Field(field) => eval_scalar_projection_field(field, slots),
        ScalarProjectionExpr::Literal(value) => Ok(scalar_expr_value_into_value(value.clone())),
        ScalarProjectionExpr::Unary { op, expr } => {
            let operand = eval_scalar_projection_expr(expr, slots)?;
            operators::eval_unary_expr(*op, operand).map_err(ScalarProjectionEvalError::Eval)
        }
        ScalarProjectionExpr::Binary { op, left, right } => {
            let left = eval_scalar_projection_expr(left, slots)?;
            let right = eval_scalar_projection_expr(right, slots)?;

            operators::eval_binary_expr(*op, left, right).map_err(ScalarProjectionEvalError::Eval)
        }
    }
}

fn eval_scalar_projection_field(
    field: &ScalarProjectionField,
    slots: &dyn SlotReader,
) -> Result<Value, ScalarProjectionEvalError> {
    let Some(value) = eval_scalar_value_program(&field.program, slots)
        .map_err(ScalarProjectionEvalError::Internal)?
    else {
        return Err(ScalarProjectionEvalError::Eval(
            ProjectionEvalError::MissingFieldValue {
                field: field.field.clone(),
                index: field.slot,
            },
        ));
    };

    Ok(scalar_expr_value_into_value(value))
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
