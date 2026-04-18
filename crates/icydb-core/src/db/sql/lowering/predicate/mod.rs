mod compile;
mod normalize;
mod validate;

use crate::db::{
    predicate::{CoercionId, ComparePredicate, Predicate},
    query::plan::expr::Expr,
    sql::{
        lowering::{
            SqlLoweringError,
            expr::{SqlExprPhase, lower_sql_expr},
        },
        parser::{SqlExpr, SqlTextFunction, SqlTextFunctionCall},
    },
};
use crate::value::Value;

// Lower one parser-owned SQL `WHERE` expression onto the runtime predicate
// authority through the shared SQL-expression seam.
pub(in crate::db) fn lower_sql_where_expr(expr: &SqlExpr) -> Result<Predicate, SqlLoweringError> {
    if let Some(predicate) = lower_sql_membership_where_expr(expr) {
        return Ok(predicate);
    }

    let expr = lower_sql_where_bool_expr(expr)?;

    Ok(compile::compile_where_bool_expr_to_predicate(&expr))
}

// Lower one parser-owned SQL boolean expression onto the shared planner-owned
// WHERE boolean seam without compiling it into the runtime predicate layer.
pub(in crate::db::sql::lowering) fn lower_sql_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    let expr = lower_sql_expr(expr, SqlExprPhase::PreAggregate)?;
    validate::validate_where_bool_expr(&expr)?;
    let expr = normalize::normalize_where_bool_expr(expr);

    debug_assert!(normalize::is_normalized_where_bool_expr(&expr));

    Ok(expr)
}

// Keep plain top-level SQL membership predicates on the narrower runtime
// compare surface so they do not reopen the generic boolean-expression path.
fn lower_sql_membership_where_expr(expr: &SqlExpr) -> Option<Predicate> {
    let SqlExpr::Membership {
        expr,
        values,
        negated,
    } = expr
    else {
        return None;
    };

    compile_sql_membership_where_expr(expr.as_ref(), values.as_slice(), *negated)
}

// Bind one parsed membership predicate directly when the left-hand side stays
// on the already-admitted field or LOWER/UPPER(field) wrapper family.
fn compile_sql_membership_where_expr(
    expr: &SqlExpr,
    values: &[Value],
    negated: bool,
) -> Option<Predicate> {
    let (field, coercion) = match expr {
        SqlExpr::Field(field) => (field.clone(), CoercionId::Strict),
        SqlExpr::TextFunction(SqlTextFunctionCall {
            function: SqlTextFunction::Lower | SqlTextFunction::Upper,
            field,
            literal: None,
            literal2: None,
            literal3: None,
        }) => (field.clone(), CoercionId::TextCasefold),
        _ => return None,
    };
    if values
        .iter()
        .any(|value| matches!(value, Value::List(_) | Value::Map(_)))
    {
        return None;
    }
    if coercion == CoercionId::TextCasefold
        && !values.iter().all(|value| matches!(value, Value::Text(_)))
    {
        return None;
    }

    Some(Predicate::Compare(ComparePredicate::with_coercion(
        field,
        if negated {
            crate::db::predicate::CompareOp::NotIn
        } else {
            crate::db::predicate::CompareOp::In
        },
        Value::List(values.to_vec()),
        coercion,
    )))
}
