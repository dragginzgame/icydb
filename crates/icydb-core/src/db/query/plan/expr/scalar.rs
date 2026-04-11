//! Module: query::plan::expr::scalar
//! Responsibility: planner-owned scalar projection program lowering.
//! Does not own: runtime projection evaluation or grouped projection lowering.
//! Boundary: freezes slot-resolved scalar projection programs before execution.

#[cfg(test)]
use crate::db::query::plan::expr::{BinaryOp, UnaryOp};
#[cfg(test)]
use crate::db::scalar_expr::{ScalarValueProgram, compile_scalar_field_program};
#[cfg(test)]
use crate::db::scalar_expr::{compile_scalar_literal_expr_value, scalar_expr_value_into_value};
#[cfg(test)]
use crate::value::Value;
use crate::{
    db::query::plan::expr::{Expr, ProjectionField, ProjectionSpec},
    model::entity::{EntityModel, resolve_field_slot},
};

///
/// ScalarProjectionExpr
///
/// ScalarProjectionExpr is the planner-owned compiled scalar projection tree
/// carried into execution for scalar projection materialization.
/// Field slots are resolved once and scalar literals are prebuilt into runtime
/// `Value`s so executor consumers no longer rediscover projection structure or
/// re-materialize literals per row from `EntityModel`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarProjectionExpr {
    Field(ScalarProjectionField),
    #[cfg(test)]
    Literal(Value),
    #[cfg(test)]
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    #[cfg(test)]
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
/// planner-owned compiled projection expression.
/// It preserves field-name diagnostics while turning field access into one
/// direct slot lookup for executor consumers.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarProjectionField {
    field: String,
    slot: usize,
    #[cfg(test)]
    program: Option<ScalarValueProgram>,
}

impl ScalarProjectionField {
    /// Borrow the declared field name for diagnostics.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &str {
        self.field.as_str()
    }

    /// Borrow the resolved slot index used by executor readers.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> usize {
        self.slot
    }

    #[cfg(test)]
    /// Borrow the test-only scalar slot program used by slot-reader tests.
    #[must_use]
    pub(in crate::db) const fn program(&self) -> Option<&ScalarValueProgram> {
        self.program.as_ref()
    }
}

/// Compile one scalar projection expression into a planner-owned slot-resolved
/// program when it stays entirely on the scalar seam.
#[must_use]
pub(in crate::db) fn compile_scalar_projection_expr(
    model: &EntityModel,
    expr: &Expr,
) -> Option<ScalarProjectionExpr> {
    match expr {
        Expr::Field(field_id) => {
            let slot = resolve_field_slot(model, field_id.as_str())?;
            #[cfg(test)]
            let program = compile_scalar_field_program(model, field_id.as_str());

            Some(ScalarProjectionExpr::Field(ScalarProjectionField {
                field: field_id.as_str().to_string(),
                slot,
                #[cfg(test)]
                program,
            }))
        }
        #[cfg(test)]
        Expr::Literal(value) => compile_scalar_literal_expr_value(value)
            .map(scalar_expr_value_into_value)
            .map(ScalarProjectionExpr::Literal),
        #[cfg(test)]
        Expr::Unary { op, expr } => {
            compile_scalar_projection_expr(model, expr.as_ref()).map(|expr| {
                ScalarProjectionExpr::Unary {
                    op: *op,
                    expr: Box::new(expr),
                }
            })
        }
        #[cfg(test)]
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
        #[cfg(test)]
        Expr::Alias { expr, .. } => compile_scalar_projection_expr(model, expr.as_ref()),
    }
}

/// Compile one scalar projection spec into a frozen planner-owned projection
/// program when every projected expression stays on the scalar seam.
#[must_use]
pub(in crate::db) fn compile_scalar_projection_plan(
    model: &EntityModel,
    projection: &ProjectionSpec,
) -> Option<Vec<ScalarProjectionExpr>> {
    let mut compiled_fields = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                compiled_fields.push(compile_scalar_projection_expr(model, expr)?);
            }
        }
    }

    Some(compiled_fields)
}
