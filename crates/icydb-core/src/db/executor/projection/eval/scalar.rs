//! Module: db::executor::projection::eval::scalar
//! Responsibility: compiled scalar-only projection expression evaluation on top of the shared scalar-expression seam.
//! Does not own: grouped projection execution, generic `Expr` evaluation, or planner validation.
//! Boundary: structural projection materialization calls into this file when a projection stays entirely on the scalar seam.

use crate::{
    db::{
        data::SlotReader,
        executor::projection::eval::{ProjectionEvalError, operators},
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
