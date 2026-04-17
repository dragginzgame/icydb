mod compile;
mod normalize;
mod validate;

use crate::db::{
    predicate::Predicate,
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
    let expr = lower_sql_expr(expr, SqlExprPhase::PreAggregate)?;
    validate::validate_where_bool_expr(&expr)?;
    let expr = normalize::normalize_where_bool_expr(expr);

    debug_assert!(normalize::is_normalized_where_bool_expr(&expr));

    Ok(compile::compile_where_bool_expr_to_predicate(&expr))
}
