use crate::db::query::plan::expr::{
    Expr, canonicalize_scalar_where_bool_expr, eval_literal_only_expr_value, normalize_bool_expr,
    rewrite_affine_numeric_compare_expr, simplify_bool_expr_constants,
};
use crate::db::query::preparation::PreparationWork;

pub(super) fn normalize_where_bool_expr(
    expr: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, crate::db::QueryError> {
    let expr = rewrite_affine_numeric_compare_expr(expr);
    let expr = fold_literal_only_where_expr(expr);
    let expr = simplify_bool_expr_constants(expr);

    normalize_bool_expr(expr, work)
}

pub(super) fn normalize_scalar_where_bool_expr(
    expr: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, crate::db::QueryError> {
    let expr = fold_literal_only_where_expr(expr);
    let expr = simplify_bool_expr_constants(expr);

    canonicalize_scalar_where_bool_expr(expr, work)
}

// Fold literal-only scalar subtrees inside WHERE before normalization so the
// conservative predicate compiler can still reuse its existing field-vs-literal
// fast paths when the right-hand side is just a wrapped constant expression.
fn fold_literal_only_where_expr(mut expr: Expr) -> Expr {
    expr.map_scalar_children(fold_literal_only_where_expr);
    if matches!(
        expr,
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) | Expr::Aggregate(_)
    ) {
        return expr;
    }

    fold_literal_only_where_leaf(expr)
}

fn fold_literal_only_where_leaf(expr: Expr) -> Expr {
    if !where_expr_is_literal_only(&expr) {
        return expr;
    }

    eval_literal_only_expr_value(&expr)
        .map(Expr::Literal)
        .unwrap_or(expr)
}

fn where_expr_is_literal_only(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Aggregate(_) => false,
        Expr::FunctionCall { args, .. } => args.iter().all(where_expr_is_literal_only),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                where_expr_is_literal_only(arm.condition())
                    && where_expr_is_literal_only(arm.result())
            }) && where_expr_is_literal_only(else_expr.as_ref())
        }
        Expr::Binary { left, right, .. } => {
            where_expr_is_literal_only(left.as_ref()) && where_expr_is_literal_only(right.as_ref())
        }
        Expr::Unary { expr, .. } => where_expr_is_literal_only(expr.as_ref()),
        #[cfg(test)]
        Expr::Alias { expr, .. } => where_expr_is_literal_only(expr.as_ref()),
    }
}
