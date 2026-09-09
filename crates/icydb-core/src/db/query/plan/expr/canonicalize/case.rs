mod budget;
#[cfg(test)]
mod expansion_tests;
#[cfg(test)]
mod tests;

use crate::{
    db::query::plan::expr::{
        BinaryOp, CaseWhenArm, Expr, Function, UnaryOp,
        canonicalize::{
            normalize_bool_expr,
            truth_admission::{TruthAdmission, TruthWrapperScope},
        },
    },
    db::{QueryError, query::preparation::PreparationWork},
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

// Canonicalize one planner-owned boolean searched `CASE` onto the bounded
// first-match boolean expansion when the resulting expression size stays within
// the current threshold. Otherwise preserve the normalized `CASE`
// shape so canonicalization remains explicit and fail-closed.
pub(super) fn normalize_bool_case_expr(
    mut expr: Expr,
    top_level_where_null_collapse: bool,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    let Expr::Case {
        when_then_arms,
        else_expr,
    } = &mut expr
    else {
        return Err(QueryError::invariant());
    };
    let Some(admitted) = budget::admit_expansion(when_then_arms, else_expr, work)? else {
        // Preserve the original arms and ELSE box when content stays compact.
        return Ok(expr);
    };
    lower_searched_case_to_boolean(
        std::mem::take(when_then_arms),
        else_expr.take(),
        admitted,
        top_level_where_null_collapse,
        work,
    )
}

// Recurse across boolean-context planner nodes only so searched `CASE`
// canonicalization stays scoped to scalar filter semantics instead of
// rewriting generic value-expression surfaces like grouped WHERE, HAVING, or
// arbitrary compare operands.
pub(super) fn canonicalize_normalized_bool_case_in_bool_context(
    mut expr: Expr,
    top_level_where_null_collapse: bool,
    truth_wrapper_scope: Option<TruthWrapperScope>,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    work.charge(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        1,
    )?;
    match &mut expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr: child,
        } => {
            **child = canonicalize_normalized_bool_case_in_bool_context(
                child.take(),
                false,
                truth_wrapper_scope,
                work,
            )?;
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => {
            **left = canonicalize_normalized_bool_case_in_bool_context(
                left.take(),
                top_level_where_null_collapse,
                truth_wrapper_scope,
                work,
            )?;
            **right = canonicalize_normalized_bool_case_in_bool_context(
                right.take(),
                top_level_where_null_collapse,
                truth_wrapper_scope,
                work,
            )?;
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms.iter_mut() {
                let [condition, result] = arm.children_mut();
                *condition = canonicalize_normalized_bool_case_in_bool_context(
                    condition.take(),
                    true,
                    truth_wrapper_scope,
                    work,
                )?;
                *result = canonicalize_normalized_bool_case_in_bool_context(
                    result.take(),
                    top_level_where_null_collapse,
                    truth_wrapper_scope,
                    work,
                )?;
            }
            **else_expr = canonicalize_normalized_bool_case_in_bool_context(
                else_expr.take(),
                top_level_where_null_collapse,
                truth_wrapper_scope,
                work,
            )?;
            return normalize_bool_case_expr(expr, top_level_where_null_collapse, work);
        }
        _ => {
            return maybe_collapse_truth_wrapper_in_bool_context(expr, truth_wrapper_scope, work);
        }
    }

    Ok(expr)
}

// Collapse only admitted boolean equality wrappers. Transfer the chosen child
// out of the original node so destruction cannot revisit a recursive subtree.
fn maybe_collapse_truth_wrapper_in_bool_context(
    mut expr: Expr,
    scope: Option<TruthWrapperScope>,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    let Some(scope) = scope else {
        return Ok(expr);
    };
    if let Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    } = &mut expr
    {
        // Keep positive-wrapper precedence even when both sides are literals:
        // intermediate shape also feeds the deterministic CASE rewrite budget.
        let chosen = match (left.as_ref(), right.as_ref()) {
            (_, Expr::Literal(Value::Bool(true))) if truth_wrapper_candidate(left, scope) => {
                Some((left.take(), true))
            }
            (Expr::Literal(Value::Bool(true)), _) if truth_wrapper_candidate(right, scope) => {
                Some((right.take(), true))
            }
            (_, Expr::Literal(Value::Bool(false))) if truth_wrapper_candidate(left, scope) => {
                Some((left.take(), false))
            }
            (Expr::Literal(Value::Bool(false)), _) if truth_wrapper_candidate(right, scope) => {
                Some((right.take(), false))
            }
            _ => None,
        };
        if let Some((child, positive)) = chosen {
            if positive {
                return Ok(child);
            }
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            work.charge(Resource::TemporaryBytes, size_of::<Expr>() as u64)?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(child),
            });
        }
    }

    Ok(expr)
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
/// boolean expression tree after content admission. Move branch results and
/// ELSE, charging the single necessary condition copy and introduced wrappers
/// before construction. Failure discards the private owned intermediate.
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
    arms: Vec<CaseWhenArm>,
    else_expr: Expr,
    admitted: budget::AdmittedExpansion,
    top_level_where_null_collapse: bool,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    let mut canonical = match (top_level_where_null_collapse, else_expr) {
        (true, Expr::Literal(Value::Null)) => {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            Expr::Literal(Value::Bool(false))
        }
        (_, other) => other,
    };
    for (mut arm, copy) in arms.into_iter().zip(admitted.condition_copies).rev() {
        let [condition, result] = arm.children_mut();
        copy.charge(work)?;
        let duplicate = condition.clone();
        let positive = guarded_bool_case_branch(
            searched_case_match_guard(condition.take(), work)?,
            result.take(),
            work,
        )?;
        let negative_guard = searched_case_match_guard(duplicate, work)?;
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        work.charge(Resource::TemporaryBytes, size_of::<Expr>() as u64)?;
        let negative = guarded_bool_case_branch(
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(negative_guard),
            },
            canonical,
            work,
        )?;
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        work.charge(Resource::TemporaryBytes, (2 * size_of::<Expr>()) as u64)?;
        canonical = normalize_bool_expr(
            Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(positive),
                right: Box::new(negative),
            },
            work,
        )?;
    }

    Ok(canonical)
}

// Build one guarded boolean branch while preserving the small three-valued
// identities that keep searched `CASE` canonicalization from emitting obvious
// `guard AND TRUE` / `guard AND FALSE` shells.
fn guarded_bool_case_branch(
    guard: Expr,
    result: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    match result {
        Expr::Literal(Value::Bool(true)) => Ok(guard),
        Expr::Literal(Value::Bool(false)) => Ok(result),
        other => {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            work.charge(Resource::TemporaryBytes, (2 * size_of::<Expr>()) as u64)?;
            Ok(Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(guard),
                right: Box::new(other),
            })
        }
    }
}

// Lower one searched-`CASE` branch condition onto the planner-owned boolean
// match contract where only `TRUE` selects the branch and both `FALSE` and
// `NULL` fall through to the next arm.
fn searched_case_match_guard(
    condition: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 2)?;
    work.charge(Resource::TemporaryBytes, (2 * size_of::<Expr>()) as u64)?;
    Ok(Expr::FunctionCall {
        function: Function::Coalesce,
        args: vec![condition, Expr::Literal(Value::Bool(false))],
    })
}
