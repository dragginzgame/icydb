use crate::db::query::plan::expr::{
    CaseWhenArm, Expr, canonicalize_scalar_where_bool_expr, eval_literal_only_expr_value,
    normalize_bool_expr, rewrite_affine_numeric_compare_expr, simplify_bool_expr_constants,
};

pub(super) fn normalize_where_bool_expr(expr: Expr) -> Expr {
    let expr = rewrite_affine_numeric_compare_expr(expr);
    let expr = fold_literal_only_where_expr(expr);
    let expr = simplify_bool_expr_constants(expr);

    normalize_bool_expr(expr)
}

pub(super) fn normalize_scalar_where_bool_expr(expr: Expr) -> Expr {
    let expr = rewrite_affine_numeric_compare_expr(expr);
    let expr = fold_literal_only_where_expr(expr);
    let expr = simplify_bool_expr_constants(expr);

    canonicalize_scalar_where_bool_expr(expr)
}

// Fold literal-only scalar subtrees inside WHERE before normalization so the
// conservative predicate compiler can still reuse its existing field-vs-literal
// fast paths when the right-hand side is just a wrapped constant expression.
fn fold_literal_only_where_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) | Expr::Aggregate(_) => expr,
        Expr::FunctionCall { function, args } => {
            let args = args
                .into_iter()
                .map(fold_literal_only_where_expr)
                .collect::<Vec<_>>();

            fold_literal_only_where_leaf(Expr::FunctionCall { function, args })
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let when_then_arms = when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        fold_literal_only_where_expr(arm.condition().clone()),
                        fold_literal_only_where_expr(arm.result().clone()),
                    )
                })
                .collect();
            let else_expr = Box::new(fold_literal_only_where_expr(*else_expr));

            fold_literal_only_where_leaf(Expr::Case {
                when_then_arms,
                else_expr,
            })
        }
        Expr::Binary { op, left, right } => {
            let left = Box::new(fold_literal_only_where_expr(*left));
            let right = Box::new(fold_literal_only_where_expr(*right));

            fold_literal_only_where_leaf(Expr::Binary { op, left, right })
        }
        Expr::Unary { op, expr } => {
            let expr = Box::new(fold_literal_only_where_expr(*expr));

            fold_literal_only_where_leaf(Expr::Unary { op, expr })
        }
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(fold_literal_only_where_expr(*expr)),
            name,
        },
    }
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
