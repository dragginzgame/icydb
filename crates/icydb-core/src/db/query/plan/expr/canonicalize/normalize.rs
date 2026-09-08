#[cfg(test)]
mod tests;

use crate::{
    db::QueryError,
    db::query::{
        builder::scalar_projection::render_scalar_projection_expr_plan_label,
        plan::expr::{
            BinaryOp, BooleanFunctionShape, Expr, Function, UnaryOp,
            canonicalize::truth_admission::bool_function_args_match,
            function_is_compare_operand_coarse_family,
        },
        preparation::PreparationWork,
    },
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Normalize one planner-owned boolean expression without changing
/// three-valued semantics inside subexpressions.
pub(in crate::db::query::plan::expr::canonicalize) fn normalize_bool_expr_impl(
    mut expr: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match &mut expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr: child,
        } => {
            **child = normalize_bool_expr_impl(child.take(), work)?;
            match child.as_mut() {
                Expr::Unary {
                    op: UnaryOp::Not,
                    expr: inner,
                } => return Ok(inner.take()),
                Expr::Literal(Value::Bool(value)) => {
                    return Ok(Expr::Literal(Value::Bool(!*value)));
                }
                Expr::Literal(Value::Null) => return Ok(Expr::Literal(Value::Null)),
                _ => {}
            }
        }
        Expr::Binary {
            op: op @ (BinaryOp::And | BinaryOp::Or),
            left,
            right,
        } => return normalize_bool_associative_expr(*op, left.take(), right.take(), work),
        Expr::Binary { op, left, right } => {
            **left = normalize_bool_compare_operand(left.take(), work)?;
            **right = normalize_bool_compare_operand(right.take(), work)?;
            normalize_bool_compare_expr(op, left, right);
        }
        Expr::FunctionCall { function, args } => {
            normalize_bool_function_args(*function, args, work)?;
        }
        _ => {}
    }

    Ok(expr)
}

/// Report whether one boolean expression is already in the canonical
/// normalized shape required by predicate compilation.
#[must_use]
pub(in crate::db) fn is_normalized_bool_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::FieldPath(_) => true,
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

/// Simplify mixed boolean trees after constant folding so downstream predicate
/// extraction can keep reusing one derived lane when one side has collapsed.
#[must_use]
pub(in crate::db) fn simplify_bool_expr_constants(mut expr: Expr) -> Expr {
    expr.map_scalar_children(simplify_bool_expr_constants);
    match &mut expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => simplify_boolean_and(left.take(), right.take()),
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => simplify_boolean_or(left.take(), right.take()),
        _ => expr,
    }
}

// Normalize one associative boolean chain onto one flattened, deterministically
// ordered left-associated shape so equivalent `AND` / `OR` spellings feed the
// same extracted predicate and residual contracts downstream.
fn normalize_bool_associative_expr(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    let mut children = Vec::new();
    normalize_bool_associative_children(op, left, &mut children, work)?;
    normalize_bool_associative_children(op, right, &mut children, work)?;
    crate::db::query::plan::expr::canonicalize::ordering::sort_bool_children(&mut children, work)?;

    // All admitted expression forms are deterministic, so boolean idempotence
    // permits equivalent children to collapse onto one canonical term.
    children.dedup();

    work.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;
    work.charge(
        Resource::TemporaryBytes,
        (children.len().saturating_sub(1) as u64).saturating_mul(2 * size_of::<Expr>() as u64),
    )?;
    Ok(rebuild_normalized_bool_associative_chain(op, children))
}

// Visit the whole same-operator input group before sorting or rebuilding it.
// Normalizing each binary subtree first repeats that work for every prefix of
// a left-associated chain. Only the group's non-associative terms need their
// own normalization boundary.
fn normalize_bool_associative_children(
    op: BinaryOp,
    mut expr: Expr,
    out: &mut Vec<Expr>,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match &mut expr {
        Expr::Binary {
            op: child_op,
            left,
            right,
        } if *child_op == op => {
            normalize_bool_associative_children(op, left.take(), out, work)?;
            normalize_bool_associative_children(op, right.take(), out, work)?;
        }
        _ => {
            // Removing a double NOT can expose an already-normalized group.
            // Flatten it without normalizing its terms a second time.
            collect_normalized_bool_associative_children(
                op,
                normalize_bool_expr_impl(expr, work)?,
                out,
                work,
            )?;
        }
    }
    Ok(())
}

// Collect one associative boolean subtree onto one flat child list after each
// child has already been normalized independently.
fn collect_normalized_bool_associative_children(
    op: BinaryOp,
    mut expr: Expr,
    out: &mut Vec<Expr>,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match &mut expr {
        Expr::Binary {
            op: child_op,
            left,
            right,
        } if *child_op == op => {
            collect_normalized_bool_associative_children(op, left.take(), out, work)?;
            collect_normalized_bool_associative_children(op, right.take(), out, work)?;
        }
        _ => {
            work.reserve_vec(out, 1)?;
            out.push(expr);
        }
    }
    Ok(())
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
// This is the read-only normal-form classifier, not the preparation sorter.
// Rendering never re-enters normalization.
fn bool_expr_normalized_order(left: &Expr, right: &Expr) -> std::cmp::Ordering {
    let left_rendered = render_scalar_projection_expr_plan_label(left);
    let right_rendered = render_scalar_projection_expr_plan_label(right);

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

fn normalize_bool_compare_expr(op: &mut BinaryOp, left: &mut Expr, right: &mut Expr) {
    // Reorientation reuses the input's boxed children; it does not reconstruct
    // two allocations merely to exchange their operands.
    match (&*left, &*right) {
        (Expr::Literal(Value::Bool(_)), right_expr)
            if matches!(*op, BinaryOp::Eq | BinaryOp::Ne)
                && !matches!(right_expr, Expr::Literal(_))
                && is_normalized_bool_expr(right_expr) =>
        {
            std::mem::swap(left, right);
        }
        (Expr::Literal(_), right_expr)
            if !matches!(right_expr, Expr::Literal(_))
                && is_normalized_bool_compare_operand(right_expr) =>
        {
            *op = flip_bool_compare_op(*op);
            std::mem::swap(left, right);
        }
        (Expr::Field(left_field), Expr::Field(right_field))
            if matches!(*op, BinaryOp::Eq | BinaryOp::Ne) && left_field < right_field =>
        {
            std::mem::swap(left, right);
        }
        _ => {}
    }
}

fn normalize_bool_compare_operand(
    mut expr: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match &mut expr {
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                *arg = normalize_bool_compare_operand(arg.take(), work)?;
            }
        }
        Expr::Binary { op, left, right } if op.is_numeric_arithmetic() => {
            **left = normalize_bool_compare_operand(left.take(), work)?;
            **right = normalize_bool_compare_operand(right.take(), work)?;
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                let [condition, result] = arm.children_mut();
                *condition = normalize_bool_expr_impl(condition.take(), work)?;
                *result = normalize_bool_compare_operand(result.take(), work)?;
            }
            **else_expr = normalize_bool_compare_operand(else_expr.take(), work)?;
        }
        _ => {}
    }

    Ok(expr)
}

fn normalize_bool_function_args(
    function: Function,
    args: &mut [Expr],
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    match function.boolean_function_shape() {
        Some(BooleanFunctionShape::TruthCoalesce) => {
            for arg in args {
                *arg = normalize_bool_expr_impl(arg.take(), work)?;
            }
        }
        Some(BooleanFunctionShape::TextPredicate) => {
            if let [left, right] = args {
                *left = normalize_bool_compare_operand(left.take(), work)?;
                *right = normalize_bool_compare_operand(right.take(), work)?;
            }
        }
        Some(
            BooleanFunctionShape::NullTest
            | BooleanFunctionShape::FieldPredicate
            | BooleanFunctionShape::CollectionContains,
        )
        | None => {}
        Some(BooleanFunctionShape::Membership) => {
            if let [target, _] = args {
                *target = normalize_bool_compare_operand(target.take(), work)?;
            }
        }
    }
    Ok(())
}

fn is_normalized_bool_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
    match (left, right) {
        (Expr::Literal(Value::Bool(_)), right_expr)
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne)
                && !matches!(right_expr, Expr::Literal(_))
                && is_normalized_bool_expr(right_expr) =>
        {
            false
        }
        (left_expr, Expr::Literal(Value::Bool(_)))
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne) && is_normalized_bool_expr(left_expr) =>
        {
            true
        }
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
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) | Expr::Aggregate(_) => true,
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
        Expr::Unary { .. } | Expr::Binary { .. } | Expr::FunctionCall { .. } => false,
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
