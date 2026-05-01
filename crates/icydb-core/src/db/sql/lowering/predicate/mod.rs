mod normalize;
#[cfg(test)]
mod tests;
mod validate;

use crate::db::{
    predicate::Predicate,
    query::plan::expr::{
        Expr, derive_normalized_bool_expr_predicate_subset, is_normalized_bool_expr,
    },
    sql::{
        lowering::{
            SqlLoweringError,
            expr::{SqlExprPhase, lower_sql_expr},
        },
        parser::SqlExpr,
    },
};

// Lower one parser-owned SQL `WHERE` expression onto the runtime predicate
// authority through the shared SQL-expression seam.
pub(in crate::db) fn lower_sql_where_expr(expr: &SqlExpr) -> Result<Predicate, SqlLoweringError> {
    let expr = lower_sql_where_bool_expr(expr)?;

    derive_normalized_bool_expr_predicate_subset(&expr)
        .ok_or_else(SqlLoweringError::unsupported_where_expression)
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
