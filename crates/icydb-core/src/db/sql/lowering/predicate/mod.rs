mod normalize;
#[cfg(test)]
mod tests;
mod validate;

use crate::{
    db::{
        predicate::{
            CoercionId, CompareOp, MembershipCompareLeaf, Predicate,
            collapse_membership_compare_leaves,
        },
        query::plan::expr::{
            Expr, derive_normalized_bool_expr_predicate_subset, is_normalized_bool_expr,
        },
        sql::{
            lowering::{
                SqlLoweringError,
                expr::{SqlExprPhase, lower_sql_expr},
            },
            parser::{SqlExpr, SqlScalarFunction},
        },
    },
    value::Value,
};

// Lower one parser-owned SQL `WHERE` expression onto the runtime predicate
// authority through the shared SQL-expression seam.
pub(in crate::db::sql::lowering) fn lower_sql_where_expr(
    expr: &SqlExpr,
) -> Result<Predicate, SqlLoweringError> {
    let lowered_expr = lower_sql_where_bool_expr(expr)?;

    derive_sql_where_expr_predicate_subset(expr, &lowered_expr)
        .ok_or_else(SqlLoweringError::unsupported_where_expression)
}

// Derive the predicate subset for one already-lowered SQL WHERE expression.
// Top-level membership can stay compact instead of expanding through an
// OR/AND expression chain and collapsing back to `IN`/`NOT IN` later.
pub(in crate::db::sql::lowering) fn derive_sql_where_expr_predicate_subset(
    sql_expr: &SqlExpr,
    lowered_expr: &Expr,
) -> Option<Predicate> {
    derive_top_level_sql_membership_predicate_subset(sql_expr)
        .or_else(|| derive_normalized_bool_expr_predicate_subset(lowered_expr))
}

// Derive a fully-owned predicate for one parser-level SQL WHERE shape without
// first lowering a visible expression. This is intentionally narrow: callers
// may skip the visible filter only when parser context proves the predicate is
// the complete semantic filter.
pub(in crate::db::sql::lowering) fn derive_sql_where_expr_predicate_only_subset(
    sql_expr: &SqlExpr,
) -> Option<Predicate> {
    derive_top_level_sql_membership_predicate_subset(sql_expr)
}

// Lower one parser-owned SQL boolean expression onto the shared planner-owned
// WHERE boolean seam without compiling it into the runtime predicate layer.
pub(in crate::db::sql::lowering) fn lower_sql_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_bool_expr_internal(expr, false, SqlExprPhase::Where)
}

// Lower one SQL boolean expression that uses WHERE admission rules but does
// not own the top-level WHERE-only text-predicate casefold compatibility path.
pub(in crate::db::sql::lowering) fn lower_sql_pre_aggregate_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_bool_expr_internal(expr, false, SqlExprPhase::PreAggregate)
}

// Lower one parser-owned SQL scalar-row boolean expression through the
// bounded scalar searched-`CASE` canonicalization seam without changing the
// grouped or aggregate filter-expression surfaces.
pub(in crate::db::sql::lowering) fn lower_sql_scalar_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_bool_expr_internal(expr, true, SqlExprPhase::Where)
}

fn lower_sql_bool_expr_internal(
    expr: &SqlExpr,
    scalar_case_canonicalization: bool,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    let expr = lower_sql_expr(expr, phase)?;
    validate::validate_where_bool_expr(&expr)?;
    let expr = if scalar_case_canonicalization {
        normalize::normalize_scalar_where_bool_expr(expr)
    } else {
        normalize::normalize_where_bool_expr(expr)
    };

    debug_assert!(
        validate::validate_where_bool_expr(&expr).is_ok(),
        "WHERE normalization must not widen or narrow clause admissibility",
    );

    debug_assert!(is_normalized_bool_expr(&expr));

    Ok(expr)
}

fn derive_top_level_sql_membership_predicate_subset(expr: &SqlExpr) -> Option<Predicate> {
    let SqlExpr::Membership {
        expr,
        values,
        negated,
    } = expr
    else {
        return None;
    };

    let target_op = if *negated {
        CompareOp::NotIn
    } else {
        CompareOp::In
    };
    let (field, fixed_coercion) = sql_membership_target(expr.as_ref())?;

    let leaves = values
        .iter()
        .map(|value| {
            let coercion = sql_membership_value_coercion(value, fixed_coercion)?;

            Some(MembershipCompareLeaf::new(field, value.clone(), coercion))
        })
        .collect::<Option<Vec<_>>>()?;

    collapse_membership_compare_leaves(leaves, target_op).map(Predicate::Compare)
}

fn sql_membership_target(expr: &SqlExpr) -> Option<(&str, Option<CoercionId>)> {
    match expr {
        SqlExpr::Field(field) => Some((field.as_str(), None)),
        SqlExpr::FunctionCall {
            function: SqlScalarFunction::Lower,
            args,
        } => match args.as_slice() {
            [SqlExpr::Field(field)] => Some((field.as_str(), Some(CoercionId::TextCasefold))),
            _ => None,
        },
        _ => None,
    }
}

const fn sql_membership_value_coercion(
    value: &Value,
    fixed: Option<CoercionId>,
) -> Option<CoercionId> {
    match fixed {
        Some(CoercionId::TextCasefold) if matches!(value, Value::Text(_)) => {
            Some(CoercionId::TextCasefold)
        }
        Some(_) => None,
        None if matches!(value, Value::List(_) | Value::Map(_)) => None,
        None if value.supports_numeric_coercion() => Some(CoercionId::NumericWiden),
        None => Some(CoercionId::Strict),
    }
}
