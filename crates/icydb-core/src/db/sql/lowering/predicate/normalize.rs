use crate::{
    db::query::plan::expr::{BinaryOp, CaseWhenArm, Expr, Function, UnaryOp},
    value::Value,
};

// Normalize one validated planner-owned WHERE expression without changing
// three-valued semantics inside subexpressions. This owns shaping only.
pub(super) fn normalize_where_bool_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => match normalize_where_bool_expr(*expr) {
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
        } => Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(normalize_where_bool_expr(*left)),
            right: Box::new(normalize_where_bool_expr(*right)),
        },
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(normalize_where_bool_expr(*left)),
            right: Box::new(normalize_where_bool_expr(*right)),
        },
        Expr::Binary { op, left, right } => normalize_where_compare_expr(
            op,
            normalize_where_compare_operand(*left),
            normalize_where_compare_operand(*right),
        ),
        Expr::FunctionCall { function, args } => normalize_where_bool_function_call(function, args),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        normalize_where_bool_expr(arm.condition().clone()),
                        normalize_where_bool_expr(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(normalize_where_bool_expr(*else_expr)),
        },
        other => other,
    }
}

// Report whether one WHERE expression is already in the canonical normalized
// shape required by predicate compilation.
pub(super) fn is_normalized_where_bool_expr(expr: &Expr) -> bool {
    match expr {
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
            ) && is_normalized_where_bool_expr(expr.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => {
            is_normalized_where_bool_expr(left.as_ref())
                && is_normalized_where_bool_expr(right.as_ref())
        }
        Expr::Binary { op, left, right } => is_normalized_where_compare_expr(*op, left, right),
        Expr::FunctionCall { function, args } => {
            is_normalized_where_bool_function_call(*function, args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                is_normalized_where_bool_expr(arm.condition())
                    && is_normalized_where_bool_expr(arm.result())
            }) && is_normalized_where_bool_expr(else_expr.as_ref())
        }
        Expr::Field(_) | Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

// Canonicalize one compare shell so field-first and symmetric equality forms
// stay stable before predicate compilation.
fn normalize_where_compare_expr(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    match (&left, &right) {
        (
            Expr::Literal(_),
            Expr::Field(_)
            | Expr::FunctionCall {
                function: Function::Lower,
                ..
            },
        ) => Expr::Binary {
            op: flip_where_compare_op(op),
            left: Box::new(right),
            right: Box::new(left),
        },
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

// Canonicalize one compare operand so casefold wrappers always use the shared
// LOWER(field) normalized shape.
fn normalize_where_compare_operand(expr: Expr) -> Expr {
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
                args,
            },
        },
        expr => expr,
    }
}

// Normalize direct text boolean function targets through the same compare
// operand canonicalization used by compare shells.
fn normalize_where_bool_function_call(function: Function, args: Vec<Expr>) -> Expr {
    match function {
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            let [left, right] = <[Expr; 2]>::try_from(args)
                .expect("validated WHERE text predicate should keep two arguments");

            Expr::FunctionCall {
                function,
                args: vec![normalize_where_compare_operand(left), right],
            }
        }
        _ => Expr::FunctionCall { function, args },
    }
}

// Report whether one compare shell already satisfies the canonical field-first
// and casefold-wrapper invariants required by compilation.
fn is_normalized_where_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
    match (left, right) {
        (
            Expr::Literal(_),
            Expr::Field(_)
            | Expr::FunctionCall {
                function: Function::Lower,
                ..
            },
        ) => false,
        (Expr::Field(left_field), Expr::Field(right_field))
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne) && left_field < right_field =>
        {
            false
        }
        _ => {
            is_normalized_where_compare_operand(left) && is_normalized_where_compare_operand(right)
        }
    }
}

// Report whether one compare operand is already in the canonical field/literal
// or LOWER(field) wrapper shape.
fn is_normalized_where_compare_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => matches!(args.as_slice(), [Expr::Field(_)]),
        Expr::FunctionCall {
            function: Function::Upper,
            ..
        } => false,
        Expr::Aggregate(_)
        | Expr::Unary { .. }
        | Expr::Binary { .. }
        | Expr::Case { .. }
        | Expr::FunctionCall { .. } => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

// Report whether one boolean function call already uses canonical normalized
// argument shapes.
fn is_normalized_where_bool_function_call(function: Function, args: &[Expr]) -> bool {
    match function {
        Function::IsNull | Function::IsNotNull => {
            matches!(args, [Expr::Field(_) | Expr::Literal(_)])
        }
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            matches!(args, [left, Expr::Literal(Value::Text(_))] if is_normalized_where_compare_operand(left))
        }
        _ => false,
    }
}

// Flip one compare operator when normalization rewrites a literal-first shell
// into the canonical field-first form.
const fn flip_where_compare_op(op: BinaryOp) -> BinaryOp {
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
