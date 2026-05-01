//! Module: query::plan::expr::type_inference
//! Responsibility: infer deterministic planner expression type classes from schema and AST.
//! Does not own: runtime projection evaluation or expression execution behavior.
//! Boundary: returns planner-domain type information and typed plan errors
//! without compiling predicates or rewriting canonical expression shape.

mod aggregate;
mod binary;
mod case;
#[cfg(test)]
mod coarse;
mod function;
mod source;
mod unify;

use crate::db::{
    query::plan::{
        PlanError,
        expr::{
            NumericSubtype,
            ast::{Expr, UnaryOp},
        },
        validate::ExprPlanError,
    },
    schema::SchemaInfo,
};

#[cfg(test)]
pub(in crate::db::query::plan::expr) use coarse::{
    dynamic_function_arg_coarse_family, function_arg_coarse_family, function_result_coarse_family,
    infer_case_result_exprs_coarse_family, infer_dynamic_function_result_exprs_coarse_family,
    infer_expr_coarse_family,
};
pub(in crate::db::query::plan::expr) use function::function_is_compare_operand_coarse_family;

///
/// ExprType
///
/// Minimal deterministic expression type classification for planner inference.
/// This intentionally remains coarse in the bootstrap phase.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExprType {
    Blob,
    Bool,
    Numeric(NumericSubtype),
    Text,
    #[cfg(test)]
    Null,
    Collection,
    Structured,
    Opaque,
    Unknown,
}

///
/// ExprCoarseTypeFamily
///
/// Coarse planner-owned expression family projection used by boundaries that
/// intentionally validate against `Bool` / `Numeric` / `Text` contracts
/// without becoming a second independent type lattice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum ExprCoarseTypeFamily {
    #[cfg(test)]
    Bool,
    Numeric,
    Text,
}

///
/// TypedExpr
///
/// Stage artifact for expressions that have crossed the planner type-inference
/// boundary. It carries only the inferred type because the expression tree is
/// already owned by the caller and this stage must not rewrite its shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) struct TypedExpr {
    expr_type: ExprType,
}

impl TypedExpr {
    // Build one typed expression artifact from the inferred planner type.
    const fn new(expr_type: ExprType) -> Self {
        Self { expr_type }
    }

    /// Return the inferred planner type for callers that consume the
    /// type-inference stage as a plain `ExprType`.
    pub(in crate::db::query::plan::expr) const fn into_expr_type(self) -> ExprType {
        self.expr_type
    }
}

impl ExprType {
    // Eligibility answers "can this participate in numeric-only operators?".
    // Subtype answers "which numeric family?" and may remain unresolved.
    const fn is_numeric_eligible(&self) -> bool {
        matches!(self, Self::Numeric(_))
    }

    const fn numeric_subtype(&self) -> Option<NumericSubtype> {
        match self {
            Self::Numeric(subtype) => Some(*subtype),
            _ => None,
        }
    }
}

/// Infer one typed expression artifact deterministically from canonical
/// expression shape without rewriting that shape.
pub(in crate::db::query::plan::expr) fn infer_typed_expr(
    expr: &Expr,
    schema: &SchemaInfo,
) -> Result<TypedExpr, PlanError> {
    infer_expr_type_impl(expr, schema).map(TypedExpr::new)
}

/// Infer expression type deterministically from canonical expression shape.
pub(in crate::db) fn infer_expr_type(
    expr: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    infer_typed_expr(expr, schema).map(TypedExpr::into_expr_type)
}

fn infer_expr_type_impl(expr: &Expr, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    match expr {
        Expr::Field(field) => source::infer_field_expr_type(field, schema),
        Expr::FieldPath(path) => source::infer_field_path_expr_type(path, schema),
        Expr::Literal(value) => Ok(source::infer_literal_type(value)),
        Expr::FunctionCall { function, args } => {
            function::infer_function_expr_type(*function, args.as_slice(), schema)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => case::infer_case_expr_type(when_then_arms.as_slice(), else_expr.as_ref(), schema),
        Expr::Aggregate(aggregate) => aggregate::infer_aggregate_expr_type(aggregate, schema),
        #[cfg(test)]
        Expr::Alias { expr, .. } => infer_expr_type(expr.as_ref(), schema),
        Expr::Unary { op, expr } => {
            let inner = infer_expr_type(expr.as_ref(), schema)?;

            match op {
                UnaryOp::Not => {
                    if !matches!(inner, ExprType::Bool) {
                        return Err(PlanError::from(ExprPlanError::invalid_unary_operand(
                            "not",
                            format!("{inner:?}"),
                        )));
                    }

                    Ok(ExprType::Bool)
                }
            }
        }
        Expr::Binary { op, left, right } => {
            binary::infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema)
        }
    }
}

/// Project one inferred planner expression type into one coarse boundary-local
/// family without reinterpreting the underlying typing semantics.
#[must_use]
#[cfg(test)]
pub(in crate::db::query::plan::expr) const fn coarse_family_for_expr_type(
    expr_type: &ExprType,
) -> Option<ExprCoarseTypeFamily> {
    match expr_type {
        ExprType::Bool => Some(ExprCoarseTypeFamily::Bool),
        ExprType::Numeric(_) => Some(ExprCoarseTypeFamily::Numeric),
        ExprType::Text => Some(ExprCoarseTypeFamily::Text),
        #[cfg(test)]
        ExprType::Null => None,
        ExprType::Blob
        | ExprType::Collection
        | ExprType::Structured
        | ExprType::Opaque
        | ExprType::Unknown => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
