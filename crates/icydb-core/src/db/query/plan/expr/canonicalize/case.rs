use crate::{
    db::query::plan::expr::{
        BinaryOp, CaseWhenArm, Expr, Function, UnaryOp,
        canonicalize::{
            normalize_bool_expr,
            truth_admission::{TruthAdmission, TruthWrapperScope},
        },
    },
    value::Value,
};

const MAX_BOOL_CASE_CANONICALIZATION_ARMS: usize = 8;

// Canonicalize one planner-owned boolean searched `CASE` onto the bounded
// first-match boolean expansion when the resulting expression size stays within
// the shipped `0.107` threshold. Otherwise preserve the normalized `CASE`
// shape so canonicalization remains explicit and fail-closed.
pub(super) fn normalize_bool_case_expr(
    when_then_arms: Vec<CaseWhenArm>,
    else_expr: Expr,
    top_level_where_null_collapse: bool,
) -> Expr {
    lower_searched_case_to_boolean(
        when_then_arms.as_slice(),
        &else_expr,
        top_level_where_null_collapse,
    )
    .unwrap_or_else(|| Expr::Case {
        when_then_arms,
        else_expr: Box::new(else_expr),
    })
}

// Recurse across boolean-context planner nodes only so searched `CASE`
// canonicalization stays scoped to scalar filter semantics instead of
// rewriting generic value-expression surfaces like grouped WHERE, HAVING, or
// arbitrary compare operands.
pub(super) fn canonicalize_normalized_bool_case_in_bool_context(
    expr: Expr,
    top_level_where_null_collapse: bool,
    truth_wrapper_scope: Option<TruthWrapperScope>,
) -> Expr {
    match expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(canonicalize_normalized_bool_case_in_bool_context(
                *expr,
                false,
                truth_wrapper_scope,
            )),
        },
        Expr::Binary {
            op: logical @ (BinaryOp::And | BinaryOp::Or),
            left,
            right,
        } => Expr::Binary {
            op: logical,
            left: Box::new(canonicalize_normalized_bool_case_in_bool_context(
                *left,
                top_level_where_null_collapse,
                truth_wrapper_scope,
            )),
            right: Box::new(canonicalize_normalized_bool_case_in_bool_context(
                *right,
                top_level_where_null_collapse,
                truth_wrapper_scope,
            )),
        },
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let when_then_arms = when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        canonicalize_normalized_bool_case_in_bool_context(
                            arm.condition().clone(),
                            true,
                            truth_wrapper_scope,
                        ),
                        canonicalize_normalized_bool_case_in_bool_context(
                            arm.result().clone(),
                            top_level_where_null_collapse,
                            truth_wrapper_scope,
                        ),
                    )
                })
                .collect::<Vec<_>>();
            let else_expr = canonicalize_normalized_bool_case_in_bool_context(
                *else_expr,
                top_level_where_null_collapse,
                truth_wrapper_scope,
            );

            normalize_bool_case_expr(when_then_arms, else_expr, top_level_where_null_collapse)
        }
        other => maybe_collapse_truth_wrapper_in_bool_context(other, truth_wrapper_scope),
    }
}

// Collapse the admitted `= TRUE` / `= FALSE` wrapper family through one
// planner-owned truth-condition authority instead of keeping separate local
// wrapper semantics in grouped-only or predicate-adjacent paths.
fn maybe_collapse_truth_wrapper_in_bool_context(
    expr: Expr,
    scope: Option<TruthWrapperScope>,
) -> Expr {
    let Some(scope) = scope else {
        return expr;
    };

    match expr {
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } if matches!(right.as_ref(), Expr::Literal(Value::Bool(true)))
            && truth_wrapper_candidate(left.as_ref(), scope) =>
        {
            *left
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } if matches!(left.as_ref(), Expr::Literal(Value::Bool(true)))
            && truth_wrapper_candidate(right.as_ref(), scope) =>
        {
            *right
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } if matches!(right.as_ref(), Expr::Literal(Value::Bool(false)))
            && truth_wrapper_candidate(left.as_ref(), scope) =>
        {
            Expr::Unary {
                op: UnaryOp::Not,
                expr: left,
            }
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } if matches!(left.as_ref(), Expr::Literal(Value::Bool(false)))
            && truth_wrapper_candidate(right.as_ref(), scope) =>
        {
            Expr::Unary {
                op: UnaryOp::Not,
                expr: right,
            }
        }
        other => other,
    }
}

// Recognize the admitted truth-condition family where outer bool equality
// wrappers are semantically redundant in boolean filter contexts.
fn truth_wrapper_candidate(expr: &Expr, scope: TruthWrapperScope) -> bool {
    match scope {
        TruthWrapperScope::ScalarWhere => TruthAdmission::is_scalar_condition(expr),
        TruthWrapperScope::GroupedHaving => TruthAdmission::is_grouped_condition(expr),
    }
}

/// Lower one already-normalized searched `CASE` expression into an equivalent
/// boolean expression tree when the number of arms stays within the
/// `MAX_BOOL_CASE_CANONICALIZATION_ARMS` bound.
///
/// Searched SQL `CASE` selects a branch only when the condition evaluates to
/// `TRUE`; both `FALSE` and `NULL` fall through to the next arm. The lowered
/// guard therefore wraps each condition as `COALESCE(condition, false)`, which
/// preserves that first-match contract while converting the guard into an
/// ordinary two-valued boolean condition for the `AND` / `OR` expansion.
///
/// `NULL` branch results are not rewritten inside nested subexpressions. Only
/// the final scalar-WHERE result may collapse `ELSE NULL` to `FALSE`, because
/// scalar row filtering rejects both outcomes. Grouped `HAVING` passes
/// `top_level_where_null_collapse=false`, so it retains its distinct grouped
/// result semantics.
fn lower_searched_case_to_boolean(
    arms: &[CaseWhenArm],
    else_expr: &Expr,
    top_level_where_null_collapse: bool,
) -> Option<Expr> {
    if arms.is_empty() || arms.len() > MAX_BOOL_CASE_CANONICALIZATION_ARMS {
        return None;
    }

    let mut canonical = match (top_level_where_null_collapse, else_expr) {
        (true, Expr::Literal(Value::Null)) => Expr::Literal(Value::Bool(false)),
        (_, other) => other.clone(),
    };
    for arm in arms.iter().rev() {
        canonical = normalize_bool_expr(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(guarded_bool_case_branch(
                searched_case_match_guard(arm.condition().clone()),
                arm.result().clone(),
            )),
            right: Box::new(guarded_bool_case_branch(
                Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(searched_case_match_guard(arm.condition().clone())),
                },
                canonical,
            )),
        });
    }

    Some(canonical)
}

// Build one guarded boolean branch while preserving the small three-valued
// identities that keep searched `CASE` canonicalization from emitting obvious
// `guard AND TRUE` / `guard AND FALSE` shells.
fn guarded_bool_case_branch(guard: Expr, result: Expr) -> Expr {
    match result {
        Expr::Literal(Value::Bool(true)) => guard,
        Expr::Literal(Value::Bool(false)) => Expr::Literal(Value::Bool(false)),
        other => Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(guard),
            right: Box::new(other),
        },
    }
}

// Lower one searched-`CASE` branch condition onto the planner-owned boolean
// match contract where only `TRUE` selects the branch and both `FALSE` and
// `NULL` fall through to the next arm.
fn searched_case_match_guard(condition: Expr) -> Expr {
    Expr::FunctionCall {
        function: Function::Coalesce,
        args: vec![condition, Expr::Literal(Value::Bool(false))],
    }
}
