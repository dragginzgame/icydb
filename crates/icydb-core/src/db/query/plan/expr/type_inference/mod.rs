//! Module: query::plan::expr::type_inference
//! Responsibility: infer deterministic planner expression type classes from schema and AST.
//! Does not own: runtime projection evaluation or expression execution behavior.
//! Boundary: returns planner-domain type information and typed plan errors
//! without compiling predicates or rewriting canonical expression shape.

mod aggregate;
mod binary;
mod case;
mod function;
mod source;
mod unify;

#[cfg(test)]
mod admission_tests;

use crate::db::{
    QueryError,
    query::plan::{
        PlanError,
        expr::{
            NumericSubtype,
            ast::{Expr, UnaryOp},
        },
        validate::ExprPlanError,
    },
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

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
    // Known SQL NULL is distinct from unresolved type information in every build.
    Null,
    Collection,
    Structured,
    Opaque,
    U256,
    Unknown,
}

///
/// FunctionArgumentFamily
///
/// Closed scalar-function argument family used by planner type validation.
/// This projects only the numeric/text distinctions required by fixed
/// signatures and does not form a second expression type lattice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FunctionArgumentFamily {
    Numeric,
    Text,
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

/// Infer expression type under current preparation authority without rewriting
/// syntax. Admit each visited node before inspecting it; child order is semantic.
pub(in crate::db) fn infer_expr_type(
    expr: &Expr,
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<ExprType, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match expr {
        Expr::Field(field) => source::infer_field_expr_type(field, schema, work),
        Expr::FieldPath(path) => source::infer_field_path_expr_type(path, schema, work),
        Expr::Literal(value) => Ok(source::infer_literal_type(value)),
        Expr::FunctionCall { function, args } => {
            function::infer_function_expr_type(*function, args.as_slice(), schema, work)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            case::infer_case_expr_type(when_then_arms.as_slice(), else_expr.as_ref(), schema, work)
        }
        Expr::Aggregate(aggregate) => aggregate::infer_aggregate_expr_type(aggregate, schema, work),
        Expr::Unary { op, expr } => {
            let inner = infer_expr_type(expr.as_ref(), schema, work)?;

            match op {
                UnaryOp::Not => {
                    if !matches!(inner, ExprType::Bool | ExprType::Null) {
                        return Err(PlanError::from(ExprPlanError::invalid_unary_operand(
                            *op, &inner,
                        ))
                        .into());
                    }

                    Ok(ExprType::Bool)
                }
            }
        }
        Expr::Binary { op, left, right } => {
            binary::infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema, work)
        }
    }
}

#[cfg(test)]
mod tests;
