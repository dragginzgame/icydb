//! Module: query::plan::expr::scalar
//! Responsibility: planner-owned scalar projection program lowering.
//! Does not own: runtime projection evaluation or grouped projection lowering.
//! Boundary: freezes slot-resolved scalar projection programs before execution.

#[cfg(test)]
use crate::db::query::plan::expr::UnaryOp;
#[cfg(test)]
use crate::db::scalar_expr::{ScalarValueProgram, compile_scalar_field_program};
#[cfg(test)]
use crate::db::scalar_expr::{compile_scalar_literal_expr_value, scalar_expr_value_into_value};
use crate::value::Value;
use crate::{
    db::query::plan::expr::{
        BinaryOp, Expr, ProjectionField, ProjectionSpec, projection_field_expr,
    },
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
    Literal(Value),
    FunctionCall {
        function: crate::db::query::plan::expr::Function,
        args: Vec<Self>,
    },
    #[cfg(test)]
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

/// Extend one slot list with every field slot referenced by one compiled
/// scalar projection expression.
pub(in crate::db) fn extend_scalar_projection_referenced_slots(
    expr: &ScalarProjectionExpr,
    referenced: &mut Vec<usize>,
) {
    match expr {
        ScalarProjectionExpr::Field(field) => {
            if !referenced.contains(&field.slot()) {
                referenced.push(field.slot());
            }
        }
        ScalarProjectionExpr::Literal(_) => {}
        ScalarProjectionExpr::FunctionCall { args, .. } => {
            for arg in args {
                extend_scalar_projection_referenced_slots(arg, referenced);
            }
        }
        #[cfg(test)]
        ScalarProjectionExpr::Unary { expr, .. } => {
            extend_scalar_projection_referenced_slots(expr.as_ref(), referenced);
        }
        ScalarProjectionExpr::Binary { left, right, .. } => {
            extend_scalar_projection_referenced_slots(left.as_ref(), referenced);
            extend_scalar_projection_referenced_slots(right.as_ref(), referenced);
        }
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
        Expr::Field(field_id) => compile_scalar_field_reference(model, field_id.as_str()),
        Expr::Literal(value) => Some(compile_scalar_literal(value)),
        Expr::FunctionCall { function, args } => {
            let args = args
                .iter()
                .map(|arg| compile_scalar_projection_expr(model, arg))
                .collect::<Option<Vec<_>>>()?;

            Some(ScalarProjectionExpr::FunctionCall {
                function: *function,
                args,
            })
        }
        #[cfg(test)]
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
        compiled_fields.push(compile_scalar_projection_field(model, field)?);
    }

    Some(compiled_fields)
}

// Field references are the only scalar projection leaves that need schema slot
// resolution and test-only field-program derivation before recursion continues.
fn compile_scalar_field_reference(
    model: &EntityModel,
    field_name: &str,
) -> Option<ScalarProjectionExpr> {
    let slot = resolve_field_slot(model, field_name)?;
    #[cfg(test)]
    let program = compile_scalar_field_program(model, field_name);

    Some(ScalarProjectionExpr::Field(ScalarProjectionField {
        field: field_name.to_string(),
        slot,
        #[cfg(test)]
        program,
    }))
}

// Literal lowering stays owner-local here so the expression compiler can keep
// the recursive shape match focused on planner expression structure.
fn compile_scalar_literal(value: &Value) -> ScalarProjectionExpr {
    #[cfg(test)]
    {
        if let Some(compiled) = compile_scalar_literal_expr_value(value) {
            return ScalarProjectionExpr::Literal(scalar_expr_value_into_value(compiled));
        }

        // Decimal and other non-shared-scalar test literals still remain valid
        // runtime projection leaves even when the shared scalar test helper does
        // not model them directly.
        ScalarProjectionExpr::Literal(value.clone())
    }

    #[cfg(not(test))]
    {
        ScalarProjectionExpr::Literal(value.clone())
    }
}

// Projection-plan compilation only admits scalar projection fields at this
// boundary, so the field wrapper is lowered through one shared helper.
fn compile_scalar_projection_field(
    model: &EntityModel,
    field: &ProjectionField,
) -> Option<ScalarProjectionExpr> {
    compile_scalar_projection_expr(model, projection_field_expr(field))
}
