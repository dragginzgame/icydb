//! Module: query::plan::expr::scalar
//! Responsibility: planner-owned scalar projection program lowering.
//! Does not own: runtime projection evaluation or grouped projection lowering.
//! Boundary: freezes slot-resolved scalar projection programs before execution.

use crate::db::query::plan::expr::{PathSpec, UnaryOp};
#[cfg(test)]
use crate::db::scalar_expr::{ScalarValueProgram, compile_scalar_field_program};
#[cfg(test)]
use crate::db::scalar_expr::{compile_scalar_literal_expr_value, scalar_expr_value_into_value};
use crate::value::Value;
use crate::{
    db::{
        executor::projection::CompiledPath,
        query::plan::expr::{BinaryOp, Expr, FieldPath, ProjectionField, ProjectionSpec},
    },
    model::entity::EntityModel,
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
    FieldPath(ScalarProjectionFieldPath),
    Literal(Value),
    FunctionCall {
        function: crate::db::query::plan::expr::Function,
        args: Vec<Self>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<ScalarProjectionCaseArm>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

impl ScalarProjectionExpr {
    // Report whether this compiled scalar tree contains any nested field-path
    // projection. FieldPath evaluation needs raw persisted root-field bytes, so
    // value-only validation readers use this to stay on slot-presence checks.
    #[must_use]
    pub(in crate::db) fn contains_field_path(&self) -> bool {
        match self {
            Self::FieldPath(_) => true,
            Self::FunctionCall { args, .. } => args.iter().any(Self::contains_field_path),
            Self::Unary { expr, .. } => expr.contains_field_path(),
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().any(|arm| {
                    arm.condition().contains_field_path() || arm.result().contains_field_path()
                }) || else_expr.contains_field_path()
            }
            Self::Binary { left, right, .. } => {
                left.contains_field_path() || right.contains_field_path()
            }
            Self::Field(_) | Self::Literal(_) => false,
        }
    }

    // Walk the compiled scalar tree and visit every referenced runtime slot on
    // the owner-local traversal contract instead of reopening slot recursion in
    // execution-setup consumers.
    pub(in crate::db) fn for_each_referenced_slot(&self, visit: &mut impl FnMut(usize)) {
        match self {
            Self::Field(field) => visit(field.slot()),
            Self::FieldPath(path) => visit(path.root_slot()),
            Self::Literal(_) => {}
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    arg.for_each_referenced_slot(visit);
                }
            }
            Self::Unary { expr, .. } => expr.for_each_referenced_slot(visit),
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    arm.condition().for_each_referenced_slot(visit);
                    arm.result().for_each_referenced_slot(visit);
                }
                else_expr.for_each_referenced_slot(visit);
            }
            Self::Binary { left, right, .. } => {
                left.for_each_referenced_slot(visit);
                right.for_each_referenced_slot(visit);
            }
        }
    }

    // Extend one slot list with every unique runtime slot referenced by this
    // compiled scalar tree while preserving first-seen traversal order.
    pub(in crate::db) fn extend_referenced_slots(&self, referenced: &mut Vec<usize>) {
        self.for_each_referenced_slot(&mut |slot| {
            if !referenced.contains(&slot) {
                referenced.push(slot);
            }
        });
    }

    // Mark every runtime slot referenced by this compiled scalar tree onto one
    // caller-owned slot-requirement bitset.
    pub(in crate::db) fn mark_referenced_slots(&self, referenced: &mut [bool]) {
        self.for_each_referenced_slot(&mut |slot| {
            if let Some(required) = referenced.get_mut(slot) {
                *required = true;
            }
        });
    }
}

///
/// ScalarProjectionFieldPath
///
/// Compiled nested field-path projection rooted at a resolved top-level slot.
/// The executor uses the slot to borrow the persisted root field bytes, then
/// walks the stored value payload without materializing intermediate maps.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarProjectionFieldPath {
    path: PathSpec,
    compiled_path: CompiledPath,
    root_slot: usize,
}

impl ScalarProjectionFieldPath {
    /// Borrow the top-level field name used as the path root.
    #[must_use]
    pub(in crate::db) const fn root(&self) -> &str {
        self.path.root().as_str()
    }

    /// Borrow the resolved top-level field slot used by execution.
    #[must_use]
    pub(in crate::db) const fn root_slot(&self) -> usize {
        self.root_slot
    }

    /// Borrow the nested map-key path below the root field.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.compiled_path.segments()
    }

    /// Borrow the executor-ready nested path traversal program.
    #[must_use]
    pub(in crate::db) const fn compiled_path(&self) -> &CompiledPath {
        &self.compiled_path
    }
}

///
/// ScalarProjectionCaseArm
///
/// Compiled scalar searched-CASE arm carried into executor evaluation.
/// Conditions and results are independently compiled onto the scalar seam so
/// runtime can evaluate only the selected branch without rediscovering slots.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarProjectionCaseArm {
    condition: ScalarProjectionExpr,
    result: ScalarProjectionExpr,
}

impl ScalarProjectionCaseArm {
    /// Build one compiled scalar CASE arm.
    #[must_use]
    pub(in crate::db) const fn new(
        condition: ScalarProjectionExpr,
        result: ScalarProjectionExpr,
    ) -> Self {
        Self { condition, result }
    }

    /// Borrow the compiled condition expression.
    #[must_use]
    pub(in crate::db) const fn condition(&self) -> &ScalarProjectionExpr {
        &self.condition
    }

    /// Borrow the compiled result expression.
    #[must_use]
    pub(in crate::db) const fn result(&self) -> &ScalarProjectionExpr {
        &self.result
    }
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
        Expr::Field(field_id) => compile_scalar_field_reference(model, field_id.as_str()),
        Expr::FieldPath(path) => compile_scalar_field_path_reference(model, path),
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
        Expr::Unary { op, expr } => {
            compile_scalar_projection_expr(model, expr.as_ref()).map(|expr| {
                ScalarProjectionExpr::Unary {
                    op: *op,
                    expr: Box::new(expr),
                }
            })
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let when_then_arms = when_then_arms
                .iter()
                .map(|arm| {
                    Some(ScalarProjectionCaseArm::new(
                        compile_scalar_projection_expr(model, arm.condition())?,
                        compile_scalar_projection_expr(model, arm.result())?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            let else_expr = compile_scalar_projection_expr(model, else_expr.as_ref())?;

            Some(ScalarProjectionExpr::Case {
                when_then_arms,
                else_expr: Box::new(else_expr),
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

// Field paths still resolve the root through the existing model slot map; only
// the nested tail is deferred to executor value-storage traversal.
fn compile_scalar_field_path_reference(
    model: &EntityModel,
    path: &FieldPath,
) -> Option<ScalarProjectionExpr> {
    debug_assert!(path.path_spec().is_scalar_leaf());
    let path_spec = path.path_spec().clone();
    let compiled_path = CompiledPath::new(path_spec.segments().to_vec());

    Some(ScalarProjectionExpr::FieldPath(ScalarProjectionFieldPath {
        path: path_spec,
        compiled_path,
        root_slot: model.resolve_field_slot(path.root().as_str())?,
    }))
}

// Field references are the only scalar projection leaves that need schema slot
// resolution and test-only field-program derivation before recursion continues.
fn compile_scalar_field_reference(
    model: &EntityModel,
    field_name: &str,
) -> Option<ScalarProjectionExpr> {
    let slot = model.resolve_field_slot(field_name)?;
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
    compile_scalar_projection_expr(model, field.expr())
}
