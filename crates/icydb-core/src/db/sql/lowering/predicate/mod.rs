mod compile;
mod normalize;
mod validate;

use crate::db::{
    predicate::Predicate,
    query::plan::expr::Expr,
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

    Ok(compile::compile_where_bool_expr_to_predicate(&expr))
}

// Lower one parser-owned SQL boolean expression onto the shared planner-owned
// WHERE boolean seam without compiling it into the runtime predicate layer.
pub(in crate::db::sql::lowering) fn lower_sql_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    let expr = lower_sql_expr(expr, SqlExprPhase::PreAggregate)?;
    let expr = normalize::normalize_where_bool_expr(expr);
    validate::validate_where_bool_expr(&expr)?;

    debug_assert!(normalize::is_normalized_where_bool_expr(&expr));

    Ok(expr)
}
