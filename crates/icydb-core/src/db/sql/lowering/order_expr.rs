//! Module: db::sql::lowering::order_expr
//! Responsibility: lower parser-owned ORDER BY expression ASTs into canonical planner expressions.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: keeps SQL ORDER expression parsing in parser while preserving lowering-owned semantic adaptation.

use crate::db::{
    query::plan::expr::Expr,
    sql::{
        lowering::expr::{SqlExprPhase, lower_sql_expr},
        parser::{parse_grouped_post_aggregate_order_expr_ast, parse_supported_order_expr_ast},
    },
};

/// Lower one supported SQL `ORDER BY` expression term into the canonical
/// expression tree after parser-owned token parsing.
#[must_use]
pub(in crate::db) fn lower_supported_order_expr_text(term: &str) -> Option<Expr> {
    let ast = parse_supported_order_expr_ast(term)?;

    lower_sql_expr(&ast, SqlExprPhase::Scalar).ok()
}

/// Lower one grouped post-aggregate SQL `ORDER BY` expression term into the
/// canonical expression tree after parser-owned token parsing.
#[must_use]
pub(in crate::db) fn lower_grouped_post_aggregate_order_expr_text(term: &str) -> Option<Expr> {
    let ast = parse_grouped_post_aggregate_order_expr_ast(term)?;

    lower_sql_expr(&ast, SqlExprPhase::PostAggregate).ok()
}
