//! Module: query::explain::predicate
//! Responsibility: explain-only predicate projection from query expression DTOs.
//! Does not own: runtime predicate execution or executor descriptor assembly.
//! Boundary: query-owned expression shape -> explain predicate DTO.
//! Cross-layer rule: executor callers must request a richer explain artifact
//! here instead of importing `Expr` and reinterpreting query expression trees.

use crate::{
    db::query::{
        explain::ExplainPredicate,
        plan::expr::{
            CaseWhenArm, Expr, Function, derive_normalized_bool_expr_predicate_subset,
            normalize_bool_expr,
        },
    },
    value::Value,
};

/// Builds the query-owned explain predicate artifact for executor diagnostics.
///
/// This is the boundary used by executor explain descriptors when a prepared
/// runtime shape still carries a residual expression but EXPLAIN needs the
/// stable predicate DTO. Add new display distinctions here rather than in
/// executor code so expression interpretation stays owned by `db::query`.
///
pub(in crate::db) fn explain_predicate_from_expr(expr: &Expr) -> Option<ExplainPredicate> {
    let normalized = normalize_bool_expr(strip_explain_bool_false_guards(expr.clone()));

    derive_normalized_bool_expr_predicate_subset(&normalized)
        .map(|predicate| ExplainPredicate::from_predicate(&predicate))
}

// Strip planner-owned `COALESCE(bool_expr, FALSE)` guards before EXPLAIN asks
// the predicate subset compiler for one canonical boolean projection. This is
// display-only normalization: runtime execution still uses the effective
// filter program carried by the prepared plan.
fn strip_explain_bool_false_guards(expr: Expr) -> Expr {
    match expr {
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(strip_explain_bool_false_guards(*expr)),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(strip_explain_bool_false_guards(*left)),
            right: Box::new(strip_explain_bool_false_guards(*right)),
        },
        Expr::FunctionCall {
            function: Function::Coalesce,
            args,
        } => match args.as_slice() {
            [inner, Expr::Literal(Value::Bool(false))] => {
                strip_explain_bool_false_guards(inner.clone())
            }
            _ => Expr::FunctionCall {
                function: Function::Coalesce,
                args: args
                    .into_iter()
                    .map(strip_explain_bool_false_guards)
                    .collect(),
            },
        },
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(strip_explain_bool_false_guards)
                .collect(),
        },
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        strip_explain_bool_false_guards(arm.condition().clone()),
                        strip_explain_bool_false_guards(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(strip_explain_bool_false_guards(*else_expr)),
        },
        other => other,
    }
}
