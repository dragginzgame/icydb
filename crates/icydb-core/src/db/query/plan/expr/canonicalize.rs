use crate::{
    db::{
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        predicate::CompareOp,
        query::plan::{
            expr::{
                BinaryOp, BooleanFunctionShape, CaseWhenArm, Expr, Function, UnaryOp,
                function_is_compare_operand_coarse_family,
            },
            render_scalar_filter_expr_plan_label,
        },
    },
    value::Value,
};

const MAX_BOOL_CASE_CANONICALIZATION_ARMS: usize = 8;

///
/// TruthWrapperScope
///
/// Bounded truth-wrapper collapse scope for planner-owned boolean
/// canonicalization.
///
/// Scalar `WHERE` and grouped `HAVING` share the same admitted `= TRUE` /
/// `= FALSE` wrapper family, but grouped `HAVING` still needs a slightly wider
/// candidate set because aggregate and `COALESCE(...)` shapes can already act
/// as grouped truth conditions there.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TruthWrapperScope {
    ScalarWhere,
    GroupedHaving,
}

/// Resolve one planner truth-condition compare operator onto the binary
/// expression family used by normalized expression trees.
#[must_use]
pub(in crate::db) const fn truth_condition_compare_binary_op(op: CompareOp) -> Option<BinaryOp> {
    match op {
        CompareOp::Eq => Some(BinaryOp::Eq),
        CompareOp::Ne => Some(BinaryOp::Ne),
        CompareOp::Lt => Some(BinaryOp::Lt),
        CompareOp::Lte => Some(BinaryOp::Lte),
        CompareOp::Gt => Some(BinaryOp::Gt),
        CompareOp::Gte => Some(BinaryOp::Gte),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => None,
    }
}

/// Resolve one planner binary compare operator back onto the admitted
/// truth-condition compare family.
#[must_use]
pub(in crate::db) const fn truth_condition_binary_compare_op(op: BinaryOp) -> Option<CompareOp> {
    match op {
        BinaryOp::Eq => Some(CompareOp::Eq),
        BinaryOp::Ne => Some(CompareOp::Ne),
        BinaryOp::Lt => Some(CompareOp::Lt),
        BinaryOp::Lte => Some(CompareOp::Lte),
        BinaryOp::Gt => Some(CompareOp::Gt),
        BinaryOp::Gte => Some(CompareOp::Gte),
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => None,
    }
}

/// Normalize one planner-owned boolean expression without changing
/// three-valued semantics inside subexpressions.
#[must_use]
pub(in crate::db) fn normalize_bool_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => match normalize_bool_expr(*expr) {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => *expr,
            Expr::Literal(Value::Bool(value)) => Expr::Literal(Value::Bool(!value)),
            Expr::Literal(Value::Null) => Expr::Literal(Value::Null),
            expr => Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            },
        },
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => normalize_bool_associative_expr(BinaryOp::And, *left, *right),
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => normalize_bool_associative_expr(BinaryOp::Or, *left, *right),
        Expr::Binary { op, left, right } => normalize_bool_compare_expr(
            op,
            normalize_bool_compare_operand(*left),
            normalize_bool_compare_operand(*right),
        ),
        Expr::FunctionCall { function, args } => normalize_bool_function_call(function, args),
        other => other,
    }
}

/// Canonicalize one scalar-WHERE boolean expression onto the shipped `0.107`
/// searched-`CASE` boolean seam after the shared structural normalization pass
/// has already settled the planner-owned tree shape.
#[must_use]
pub(in crate::db) fn canonicalize_scalar_where_bool_expr(expr: Expr) -> Expr {
    let expr = normalize_bool_expr(expr);
    let expr = canonicalize_normalized_bool_case_in_bool_context(
        expr,
        true,
        Some(TruthWrapperScope::ScalarWhere),
    );
    let expr = normalize_bool_expr(expr);

    debug_assert!(is_normalized_bool_expr(&expr));

    expr
}

/// Canonicalize one grouped-HAVING boolean expression onto the bounded
/// searched-`CASE` boolean seam after the shared structural normalization pass
/// has already settled the planner-owned grouped tree shape.
///
/// Unlike scalar `WHERE`, grouped `HAVING` does not collapse a final
/// `ELSE NULL` arm to `FALSE`. Grouped canonicalization therefore preserves
/// the explicit grouped boolean result tree unless the shipped searched-`CASE`
/// expansion is already semantically identical without null-arm collapse.
#[must_use]
pub(in crate::db) fn canonicalize_grouped_having_bool_expr(expr: Expr) -> Expr {
    let expr = normalize_bool_expr(expr);
    let expr = canonicalize_normalized_bool_case_in_bool_context(
        expr,
        false,
        Some(TruthWrapperScope::GroupedHaving),
    );

    normalize_bool_expr(expr)
}

/// Report whether one boolean expression is already in the canonical
/// normalized shape required by predicate compilation.
#[must_use]
pub(in crate::db) fn is_normalized_bool_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) => true,
        Expr::Literal(Value::Bool(_) | Value::Null) => true,
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            !matches!(
                expr.as_ref(),
                Expr::Unary {
                    op: UnaryOp::Not,
                    ..
                }
            ) && is_normalized_bool_expr(expr.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            ..
        } => is_normalized_bool_associative_expr(expr),
        Expr::Binary { op, left, right } => is_normalized_bool_compare_expr(*op, left, right),
        Expr::FunctionCall { function, args } => {
            is_normalized_bool_function_call(*function, args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                is_normalized_bool_expr(arm.condition()) && is_normalized_bool_expr(arm.result())
            }) && is_normalized_bool_expr(else_expr.as_ref())
        }
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

/// Collapse one evaluated boolean-context value through the shared TRUE-only
/// admission boundary used by WHERE-style row filtering, grouped HAVING, and
/// aggregate FILTER semantics.
pub(in crate::db) fn collapse_true_only_boolean_admission<E>(
    value: Value,
    invalid: impl FnOnce(Box<Value>) -> E,
) -> Result<bool, E> {
    match value {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(invalid(Box::new(other))),
    }
}

/// Rewrite the planner-owned affine numeric compare family that can already
/// reduce onto the existing field-vs-literal predicate lane.
#[must_use]
pub(in crate::db) fn rewrite_affine_numeric_compare_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(rewrite_affine_numeric_compare_expr(*expr)),
        },
        Expr::Binary {
            op: logical @ (BinaryOp::And | BinaryOp::Or),
            left,
            right,
        } => Expr::Binary {
            op: logical,
            left: Box::new(rewrite_affine_numeric_compare_expr(*left)),
            right: Box::new(rewrite_affine_numeric_compare_expr(*right)),
        },
        Expr::Binary { op, left, right } => {
            let left = rewrite_affine_numeric_compare_expr(*left);
            let right = rewrite_affine_numeric_compare_expr(*right);

            rewrite_affine_compare_binary(op, left, right)
        }
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(rewrite_affine_numeric_compare_expr)
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
                        rewrite_affine_numeric_compare_expr(arm.condition().clone()),
                        rewrite_affine_numeric_compare_expr(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(rewrite_affine_numeric_compare_expr(*else_expr)),
        },
        other => other,
    }
}

/// Simplify mixed boolean trees after constant folding so downstream predicate
/// extraction can keep reusing one derived lane when one side has collapsed.
#[must_use]
pub(in crate::db) fn simplify_bool_expr_constants(expr: Expr) -> Expr {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => simplify_boolean_and(
            simplify_bool_expr_constants(*left),
            simplify_bool_expr_constants(*right),
        ),
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => simplify_boolean_or(
            simplify_bool_expr_constants(*left),
            simplify_bool_expr_constants(*right),
        ),
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(simplify_bool_expr_constants(*expr)),
        },
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args.into_iter().map(simplify_bool_expr_constants).collect(),
        },
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        simplify_bool_expr_constants(arm.condition().clone()),
                        simplify_bool_expr_constants(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(simplify_bool_expr_constants(*else_expr)),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(simplify_bool_expr_constants(*left)),
            right: Box::new(simplify_bool_expr_constants(*right)),
        },
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(simplify_bool_expr_constants(*expr)),
            name,
        },
        other => other,
    }
}

// Normalize one associative boolean chain onto one flattened, deterministically
// ordered left-associated shape so equivalent `AND` / `OR` spellings feed the
// same extracted predicate and residual contracts downstream.
fn normalize_bool_associative_expr(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    let mut children = Vec::new();
    collect_normalized_bool_associative_children(op, normalize_bool_expr(left), &mut children);
    collect_normalized_bool_associative_children(op, normalize_bool_expr(right), &mut children);
    children.sort_by(bool_expr_normalized_order);
    children.dedup();

    rebuild_normalized_bool_associative_chain(op, children)
}

// Canonicalize one planner-owned boolean searched `CASE` onto the bounded
// first-match boolean expansion when the resulting expression size stays within
// the shipped `0.107` threshold. Otherwise preserve the normalized `CASE`
// shape so canonicalization remains explicit and fail-closed.
fn normalize_bool_case_expr(
    when_then_arms: Vec<CaseWhenArm>,
    else_expr: Expr,
    top_level_where_null_collapse: bool,
) -> Expr {
    canonicalize_normalized_bool_case_expr(
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
fn canonicalize_normalized_bool_case_in_bool_context(
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
        TruthWrapperScope::ScalarWhere => scalar_where_truth_condition_is_admitted(expr),
        TruthWrapperScope::GroupedHaving => grouped_truth_wrapper_candidate(expr),
    }
}

/// Report whether one planner expression belongs to the admitted scalar-WHERE
/// truth-condition family.
///
/// This is the single planner-owned admission rule for scalar filter truth
/// meaning. Lowering may still decide clause ownership, but it should not keep
/// a parallel compare/null-test truth-family ladder.
pub(in crate::db) fn scalar_where_truth_condition_is_admitted(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(Value::Bool(_) | Value::Null) => true,
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => scalar_where_truth_condition_is_admitted(expr.as_ref()),
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } if matches!(right.as_ref(), Expr::Literal(Value::Bool(true | false))) => {
            scalar_where_truth_condition_is_admitted(left.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } if matches!(left.as_ref(), Expr::Literal(Value::Bool(true | false))) => {
            scalar_where_truth_condition_is_admitted(right.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => {
            scalar_where_truth_condition_is_admitted(left.as_ref())
                && scalar_where_truth_condition_is_admitted(right.as_ref())
        }
        Expr::Binary { op, left, right } if truth_condition_binary_compare_op(*op).is_some() => {
            scalar_where_truth_compare_operand_is_admitted(left.as_ref())
                && scalar_where_truth_compare_operand_is_admitted(right.as_ref())
        }
        Expr::Binary { .. } => false,
        Expr::FunctionCall { function, args } => {
            scalar_where_truth_function_call_is_admitted(*function, args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                scalar_where_truth_condition_is_admitted(arm.condition())
                    && scalar_where_truth_condition_is_admitted(arm.result())
            }) && scalar_where_truth_condition_is_admitted(else_expr.as_ref())
        }
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { expr, .. } => scalar_where_truth_condition_is_admitted(expr.as_ref()),
    }
}

// Keep scalar compare admission aligned with the shipped scalar residual family
// so compare/null-test truth shaping and wrapper collapse read from one owner.
fn scalar_where_truth_compare_operand_is_admitted(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::FunctionCall { function, args }
            if function_is_compare_operand_coarse_family(*function) =>
        {
            args.iter()
                .all(scalar_where_truth_compare_operand_is_admitted)
        }
        Expr::Binary { op, left, right } if op.is_numeric_arithmetic() => {
            scalar_where_truth_compare_operand_is_admitted(left.as_ref())
                && scalar_where_truth_compare_operand_is_admitted(right.as_ref())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                scalar_where_truth_condition_is_admitted(arm.condition())
                    && scalar_where_truth_compare_operand_is_admitted(arm.result())
            }) && scalar_where_truth_compare_operand_is_admitted(else_expr.as_ref())
        }
        Expr::Aggregate(_)
        | Expr::Unary { .. }
        | Expr::FunctionCall { .. }
        | Expr::Binary { .. } => false,
        #[cfg(test)]
        Expr::Alias { expr, .. } => scalar_where_truth_compare_operand_is_admitted(expr.as_ref()),
    }
}

// Keep scalar truth-condition admission aligned with the bounded boolean
// function family already consumed by scalar WHERE lowering and predicate
// compilation.
fn scalar_where_truth_function_call_is_admitted(function: Function, args: &[Expr]) -> bool {
    bool_function_args_match(
        function,
        args,
        scalar_where_truth_condition_is_admitted,
        scalar_where_truth_compare_operand_is_admitted,
        false,
    )
}

// Recognize the bounded grouped boolean-expression family where an outer bool
// equality wrapper is already redundant in grouped truth semantics.
fn grouped_truth_wrapper_candidate(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(Value::Bool(_) | Value::Null) => true,
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => grouped_truth_wrapper_candidate(expr.as_ref()),
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => {
            grouped_truth_wrapper_candidate(left.as_ref())
                || grouped_truth_wrapper_candidate(right.as_ref())
        }
        Expr::Binary { op, .. } if truth_condition_binary_compare_op(*op).is_some() => true,
        Expr::Binary { .. } => false,
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                grouped_truth_wrapper_candidate(arm.condition())
                    && grouped_truth_wrapper_candidate(arm.result())
            }) && grouped_truth_wrapper_candidate(else_expr.as_ref())
        }
        Expr::FunctionCall { function, args } => match function.boolean_function_shape() {
            Some(BooleanFunctionShape::TruthCoalesce) => {
                args.iter().all(grouped_truth_wrapper_candidate)
            }
            Some(
                BooleanFunctionShape::NullTest
                | BooleanFunctionShape::FieldPredicate
                | BooleanFunctionShape::TextPredicate,
            ) => true,
            Some(BooleanFunctionShape::CollectionContains) | None => false,
        },
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { expr, .. } => grouped_truth_wrapper_candidate(expr.as_ref()),
    }
}

// Expand one already-normalized scalar-WHERE searched `CASE` onto the shipped
// first-match boolean form. In scalar filter semantics, a final `ELSE NULL`
// arm is equivalent to `ELSE FALSE` because both outcomes reject the row.
fn canonicalize_normalized_bool_case_expr(
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

// Collect one associative boolean subtree onto one flat child list after each
// child has already been normalized independently.
fn collect_normalized_bool_associative_children(op: BinaryOp, expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Binary {
            op: child_op,
            left,
            right,
        } if child_op == op => {
            collect_normalized_bool_associative_children(op, *left, out);
            collect_normalized_bool_associative_children(op, *right, out);
        }
        other => out.push(other),
    }
}

// Rebuild one normalized associative child list onto one stable left-associated
// binary tree because the current planner and predicate compiler still operate
// on binary boolean expression nodes.
fn rebuild_normalized_bool_associative_chain(op: BinaryOp, children: Vec<Expr>) -> Expr {
    let mut children = children.into_iter();
    let Some(first) = children.next() else {
        return Expr::Literal(Value::Bool(matches!(op, BinaryOp::And)));
    };

    children.fold(first, |left, right| Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

// Order one normalized boolean child by its rendered planner-owned label first
// and its debug shape second so equivalent associative trees settle onto one
// deterministic extraction order without inventing a new expression hash.
fn bool_expr_normalized_order(left: &Expr, right: &Expr) -> std::cmp::Ordering {
    let left_rendered = render_scalar_filter_expr_plan_label(left);
    let right_rendered = render_scalar_filter_expr_plan_label(right);

    left_rendered
        .cmp(&right_rendered)
        .then_with(|| format!("{left:?}").cmp(&format!("{right:?}")))
}

// Report whether one associative boolean chain is already flattened onto one
// deterministically ordered child sequence.
fn is_normalized_bool_associative_expr(expr: &Expr) -> bool {
    let Expr::Binary { op, .. } = expr else {
        return false;
    };
    if !matches!(op, BinaryOp::And | BinaryOp::Or) {
        return false;
    }

    let mut children = Vec::new();
    collect_bool_associative_chain_refs(expr, *op, &mut children);

    children.iter().all(|child| is_normalized_bool_expr(child))
        && children
            .windows(2)
            .all(|window| bool_expr_normalized_order(window[0], window[1]).is_le())
}

// Traverse one associative boolean chain as shared references so the
// normalized-shape checker can validate ordering without rebuilding the tree.
fn collect_bool_associative_chain_refs<'a>(expr: &'a Expr, op: BinaryOp, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary {
            op: child_op,
            left,
            right,
        } if *child_op == op => {
            collect_bool_associative_chain_refs(left.as_ref(), op, out);
            collect_bool_associative_chain_refs(right.as_ref(), op, out);
        }
        other => out.push(other),
    }
}

fn normalize_bool_compare_expr(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    match (&left, &right) {
        (Expr::Literal(_), right_expr)
            if !matches!(right_expr, Expr::Literal(_))
                && is_normalized_bool_compare_operand(right_expr) =>
        {
            Expr::Binary {
                op: flip_bool_compare_op(op),
                left: Box::new(right),
                right: Box::new(left),
            }
        }
        (Expr::Field(left_field), Expr::Field(right_field))
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne) && left_field < right_field =>
        {
            Expr::Binary {
                op,
                left: Box::new(right),
                right: Box::new(left),
            }
        }
        _ => Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

fn normalize_bool_compare_operand(expr: Expr) -> Expr {
    match expr {
        Expr::FunctionCall {
            function: Function::Upper | Function::Lower,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Expr::FunctionCall {
                function: Function::Lower,
                args: vec![Expr::Field(field.clone())],
            },
            _ => Expr::FunctionCall {
                function: Function::Lower,
                args: args
                    .into_iter()
                    .map(normalize_bool_compare_operand)
                    .collect(),
            },
        },
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(normalize_bool_compare_operand)
                .collect(),
        },
        Expr::Binary { op, left, right } if op.is_numeric_arithmetic() => Expr::Binary {
            op,
            left: Box::new(normalize_bool_compare_operand(*left)),
            right: Box::new(normalize_bool_compare_operand(*right)),
        },
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        normalize_bool_expr(arm.condition().clone()),
                        normalize_bool_compare_operand(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(normalize_bool_compare_operand(*else_expr)),
        },
        expr => expr,
    }
}

fn normalize_bool_function_call(function: Function, args: Vec<Expr>) -> Expr {
    match function.boolean_function_shape() {
        Some(BooleanFunctionShape::TruthCoalesce) => Expr::FunctionCall {
            function,
            args: args.into_iter().map(normalize_bool_expr).collect(),
        },
        Some(BooleanFunctionShape::TextPredicate) => {
            let [left, right] = <[Expr; 2]>::try_from(args)
                .expect("validated boolean text predicate should keep two arguments");

            Expr::FunctionCall {
                function,
                args: vec![
                    normalize_bool_compare_operand(left),
                    normalize_bool_compare_operand(right),
                ],
            }
        }
        Some(
            BooleanFunctionShape::NullTest
            | BooleanFunctionShape::FieldPredicate
            | BooleanFunctionShape::CollectionContains,
        )
        | None => Expr::FunctionCall { function, args },
    }
}

fn is_normalized_bool_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
    match (left, right) {
        (Expr::Literal(_), right_expr)
            if !matches!(right_expr, Expr::Literal(_))
                && is_normalized_bool_compare_operand(right_expr) =>
        {
            false
        }
        (Expr::Field(left_field), Expr::Field(right_field))
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne) && left_field < right_field =>
        {
            false
        }
        _ => is_normalized_bool_compare_operand(left) && is_normalized_bool_compare_operand(right),
    }
}

fn is_normalized_bool_compare_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::FunctionCall { function, args }
            if function_is_compare_operand_coarse_family(*function) =>
        {
            args.iter().all(is_normalized_bool_compare_operand)
        }
        Expr::Binary { op, left, right } if op.is_numeric_arithmetic() => {
            is_normalized_bool_compare_operand(left.as_ref())
                && is_normalized_bool_compare_operand(right.as_ref())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                is_normalized_bool_expr(arm.condition())
                    && is_normalized_bool_compare_operand(arm.result())
            }) && is_normalized_bool_compare_operand(else_expr.as_ref())
        }
        Expr::Aggregate(_)
        | Expr::Unary { .. }
        | Expr::Binary { .. }
        | Expr::FunctionCall { .. } => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

fn is_normalized_bool_function_call(function: Function, args: &[Expr]) -> bool {
    bool_function_args_match(
        function,
        args,
        is_normalized_bool_expr,
        is_normalized_bool_compare_operand,
        true,
    )
}

// Validate the shared boolean-function argument skeleton while letting callers
// supply their own truth-context and compare-operand admission predicates.
fn bool_function_args_match(
    function: Function,
    args: &[Expr],
    truth_arg: impl Fn(&Expr) -> bool,
    compare_arg: impl Fn(&Expr) -> bool,
    truth_coalesce_requires_args: bool,
) -> bool {
    match function.boolean_function_shape() {
        Some(BooleanFunctionShape::TruthCoalesce) => {
            (!truth_coalesce_requires_args || !args.is_empty()) && args.iter().all(truth_arg)
        }
        Some(BooleanFunctionShape::NullTest) => {
            matches!(args, [arg] if compare_arg(arg))
        }
        Some(BooleanFunctionShape::TextPredicate) => {
            matches!(args, [left, right] if compare_arg(left) && compare_arg(right))
        }
        Some(BooleanFunctionShape::FieldPredicate) => {
            matches!(args, [Expr::Field(_)])
        }
        Some(BooleanFunctionShape::CollectionContains) => {
            matches!(args, [Expr::Field(_), Expr::Literal(_)])
        }
        None => false,
    }
}

// Keep the affine binary rewrite intentionally narrow:
// - only boolean compare operators participate
// - only one direct field plus/minus one numeric literal is rewritten
// - everything else stays fail-closed for the existing validator
fn rewrite_affine_compare_binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    let Some(compare_op) = affine_compare_op(op) else {
        return Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        };
    };

    if let Some((field, value)) = rewrite_affine_field_compare(&left, &right) {
        return Expr::Binary {
            op,
            left: Box::new(field),
            right: Box::new(Expr::Literal(value)),
        };
    }

    if let Some((field, value)) = rewrite_affine_field_compare(&right, &left) {
        return Expr::Binary {
            op: flip_compare_binary_op(compare_op),
            left: Box::new(field),
            right: Box::new(Expr::Literal(value)),
        };
    }

    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

// Recognize one affine compare side of the form:
// - field + literal
// - literal + field
// - field - literal
// and move the offset onto the literal side.
fn rewrite_affine_field_compare(affine_side: &Expr, literal_side: &Expr) -> Option<(Expr, Value)> {
    let Expr::Literal(target) = literal_side else {
        return None;
    };

    let (field, offset, arithmetic_op) = affine_field_offset(affine_side)?;
    let rewritten = match arithmetic_op {
        NumericArithmeticOp::Add => {
            apply_numeric_arithmetic(NumericArithmeticOp::Sub, target, offset)?
        }
        NumericArithmeticOp::Sub => {
            apply_numeric_arithmetic(NumericArithmeticOp::Add, target, offset)?
        }
        NumericArithmeticOp::Mul | NumericArithmeticOp::Div | NumericArithmeticOp::Rem => {
            return None;
        }
    };

    Some((field.clone(), Value::Decimal(rewritten)))
}

// Extract the direct field plus/minus literal offset pattern admitted by this
// first affine boolean compare rewrite slice.
fn affine_field_offset(expr: &Expr) -> Option<(&Expr, &Value, NumericArithmeticOp)> {
    let Expr::Binary { op, left, right } = expr else {
        return None;
    };

    match (op, left.as_ref(), right.as_ref()) {
        (BinaryOp::Add, Expr::Field(_), Expr::Literal(offset))
            if offset.supports_numeric_coercion() =>
        {
            Some((left.as_ref(), offset, NumericArithmeticOp::Add))
        }
        (BinaryOp::Add, Expr::Literal(offset), Expr::Field(_))
            if offset.supports_numeric_coercion() =>
        {
            Some((right.as_ref(), offset, NumericArithmeticOp::Add))
        }
        (BinaryOp::Sub, Expr::Field(_), Expr::Literal(offset))
            if offset.supports_numeric_coercion() =>
        {
            Some((left.as_ref(), offset, NumericArithmeticOp::Sub))
        }
        _ => None,
    }
}

fn simplify_boolean_and(left: Expr, right: Expr) -> Expr {
    match (left, right) {
        (Expr::Literal(Value::Bool(false)), _) | (_, Expr::Literal(Value::Bool(false))) => {
            Expr::Literal(Value::Bool(false))
        }
        (Expr::Literal(Value::Bool(true)), expr) | (expr, Expr::Literal(Value::Bool(true))) => expr,
        (left, right) => Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

fn simplify_boolean_or(left: Expr, right: Expr) -> Expr {
    match (left, right) {
        (Expr::Literal(Value::Bool(true)), _) | (_, Expr::Literal(Value::Bool(true))) => {
            Expr::Literal(Value::Bool(true))
        }
        (Expr::Literal(Value::Bool(false)), expr) | (expr, Expr::Literal(Value::Bool(false))) => {
            expr
        }
        (left, right) => Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

const fn affine_compare_op(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => Some(op),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => None,
    }
}

fn flip_compare_binary_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::Ne => BinaryOp::Ne,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Lte => BinaryOp::Gte,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Gte => BinaryOp::Lte,
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => {
            unreachable!("only compare operators can be flipped")
        }
    }
}

const fn flip_bool_compare_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::Ne => BinaryOp::Ne,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Lte => BinaryOp::Gte,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Gte => BinaryOp::Lte,
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => op,
    }
}
