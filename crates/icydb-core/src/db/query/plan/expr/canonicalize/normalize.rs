use crate::{
    db::query::plan::{
        expr::{
            BinaryOp, BooleanFunctionShape, CaseWhenArm, Expr, Function, FunctionDeterminism,
            UnaryOp, canonicalize::truth_admission::bool_function_args_match,
            function_is_compare_operand_coarse_family,
        },
        render_scalar_filter_expr_plan_label,
    },
    value::Value,
};

/// Normalize one planner-owned boolean expression without changing
/// three-valued semantics inside subexpressions.
#[must_use]
pub(in crate::db::query::plan::expr::canonicalize) fn normalize_bool_expr_impl(expr: Expr) -> Expr {
    match expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => match normalize_bool_expr_impl(*expr) {
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
    collect_normalized_bool_associative_children(op, normalize_bool_expr_impl(left), &mut children);
    collect_normalized_bool_associative_children(
        op,
        normalize_bool_expr_impl(right),
        &mut children,
    );
    children.sort_by(bool_expr_normalized_order);

    // Deduplicating `A AND A` / `A OR A` is only semantics-preserving because
    // planner expressions are deterministic. Bind that engine invariant to
    // the function registry so adding a non-deterministic function makes this
    // boundary fail loudly instead of quietly changing SQL truth semantics.
    assert!(
        children.iter().all(expr_is_deterministic),
        "associative boolean dedup requires deterministic child expressions",
    );
    children.dedup();

    rebuild_normalized_bool_associative_chain(op, children)
}

// Report whether one planner expression is deterministic for purposes of
// associative boolean deduplication. Scalar functions are checked through the
// shared function registry; aggregate leaves are deterministic when their
// input and filter expression trees are deterministic for a fixed group.
fn expr_is_deterministic(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) => true,
        Expr::Unary { expr, .. } => expr_is_deterministic(expr),
        Expr::Binary { left, right, .. } => {
            expr_is_deterministic(left) && expr_is_deterministic(right)
        }
        Expr::FunctionCall { function, args } => {
            let function_is_deterministic = match function.spec().determinism {
                FunctionDeterminism::Deterministic => true,
            };

            function_is_deterministic && args.iter().all(expr_is_deterministic)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                expr_is_deterministic(arm.condition()) && expr_is_deterministic(arm.result())
            }) && expr_is_deterministic(else_expr)
        }
        Expr::Aggregate(aggregate) => {
            aggregate.input_expr().is_none_or(expr_is_deterministic)
                && aggregate.filter_expr().is_none_or(expr_is_deterministic)
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => expr_is_deterministic(expr),
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
        (Expr::Literal(Value::Bool(_)), right_expr)
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne)
                && !matches!(right_expr, Expr::Literal(_))
                && is_normalized_bool_expr(right_expr) =>
        {
            Expr::Binary {
                op,
                left: Box::new(right),
                right: Box::new(left),
            }
        }
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
                        normalize_bool_expr_impl(arm.condition().clone()),
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
            args: args.into_iter().map(normalize_bool_expr_impl).collect(),
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
