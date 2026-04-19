use crate::db::{
    predicate::{is_normalized_bool_expr, normalize_bool_expr},
    query::plan::expr::Expr,
};

pub(super) fn normalize_where_bool_expr(expr: Expr) -> Expr {
    normalize_bool_expr(expr)
}

pub(super) fn is_normalized_where_bool_expr(expr: &Expr) -> bool {
    is_normalized_bool_expr(expr)
}
