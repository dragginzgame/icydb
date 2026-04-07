//! Module: db::executor::projection::eval::scalar
//! Responsibility: compiled scalar-only projection expression evaluation on top of the shared scalar-expression seam.
//! Does not own: grouped projection execution, generic `Expr` evaluation, or planner validation.
//! Boundary: structural projection materialization calls into this file when a projection stays entirely on the scalar seam.

#[cfg(test)]
use crate::db::{
    data::CanonicalSlotReader,
    scalar_expr::{
        ScalarValueProgram, compile_scalar_field_program, eval_canonical_scalar_value_program,
    },
};
#[cfg(test)]
use crate::db::{data::SlotReader, scalar_expr::eval_scalar_value_program};
use crate::{
    db::{
        executor::projection::eval::{ProjectionEvalError, operators},
        query::plan::expr::{BinaryOp, Expr, UnaryOp},
        scalar_expr::{
            ScalarExprValue, compile_scalar_literal_expr_value, scalar_expr_value_into_value,
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
#[cfg(test)]
#[allow(dead_code)]
pub(in crate::db::executor) enum ScalarProjectionEvalError {
    Eval(ProjectionEvalError),
    Internal(InternalError),
}

#[cfg(test)]
#[allow(dead_code)]
impl ScalarProjectionEvalError {
    /// Map one scalar projection evaluation failure into the executor
    /// invalid-logical-plan or internal boundary owned by this taxonomy.
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Eval(err) => err.into_invalid_logical_plan_internal_error(),
            Self::Internal(err) => err,
        }
    }
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
    #[cfg(test)]
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
            #[cfg(test)]
            let program = compile_scalar_field_program(model, field_id.as_str())?;

            Some(ScalarProjectionExpr::Field(ScalarProjectionField {
                field: field_id.as_str().to_string(),
                slot,
                #[cfg(test)]
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

#[cfg(test)]
/// Evaluate one compiled scalar projection expression against one slot reader.
pub(in crate::db::executor) fn eval_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &dyn SlotReader,
) -> Result<Value, ScalarProjectionEvalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| eval_scalar_projection_field(field, slots),
        &mut ScalarProjectionEvalError::Eval,
    )
}

/// Evaluate one compiled scalar projection expression against one canonical
/// slot reader where declared slots must already exist.
#[cfg(test)]
pub(in crate::db::executor) fn eval_canonical_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<Value, InternalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| eval_canonical_scalar_projection_field(field, slots),
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
}

/// Evaluate one compiled scalar projection expression through one required
/// value-reader closure on the canonical structural row path.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_canonical_scalar_projection_expr_with_required_value_reader(
    expr: &ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Result<Value, InternalError>,
) -> Result<Value, InternalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| read_slot(field.slot),
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
}

/// Evaluate one compiled scalar projection expression through one pure value
/// reader that resolves slots directly into runtime `Value`s.
pub(in crate::db::executor) fn eval_scalar_projection_expr_with_value_reader(
    expr: &ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, ProjectionEvalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            let Some(value) = read_slot(field.slot) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field.field.clone(),
                    index: field.slot,
                });
            };

            Ok(value)
        },
        &mut |err| err,
    )
}

fn eval_scalar_projection_expr_core<E>(
    expr: &ScalarProjectionExpr,
    eval_field: &mut dyn FnMut(&ScalarProjectionField) -> Result<Value, E>,
    map_projection_error: &mut dyn FnMut(ProjectionEvalError) -> E,
) -> Result<Value, E> {
    match expr {
        ScalarProjectionExpr::Field(field) => eval_field(field),
        ScalarProjectionExpr::Literal(value) => Ok(scalar_expr_value_into_value(value.clone())),
        ScalarProjectionExpr::Unary { op, expr } => {
            let operand = eval_scalar_projection_expr_core(expr, eval_field, map_projection_error)?;
            operators::eval_unary_expr(*op, operand).map_err(map_projection_error)
        }
        ScalarProjectionExpr::Binary { op, left, right } => {
            let left = eval_scalar_projection_expr_core(left, eval_field, map_projection_error)?;
            let right = eval_scalar_projection_expr_core(right, eval_field, map_projection_error)?;

            operators::eval_binary_expr(*op, left, right).map_err(map_projection_error)
        }
    }
}

#[cfg(test)]
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

#[cfg(test)]
fn eval_canonical_scalar_projection_field(
    field: &ScalarProjectionField,
    slots: &dyn CanonicalSlotReader,
) -> Result<Value, InternalError> {
    let value = eval_canonical_scalar_value_program(&field.program, slots)?;

    Ok(scalar_expr_value_into_value(value))
}
