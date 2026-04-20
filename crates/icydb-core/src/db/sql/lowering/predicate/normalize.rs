use crate::{
    db::{
        executor::projection::eval_builder_expr_for_value_preview,
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        predicate::{is_normalized_bool_expr, normalize_bool_expr},
        query::plan::expr::{BinaryOp, Expr},
    },
    value::Value,
};

pub(super) fn normalize_where_bool_expr(expr: Expr) -> Expr {
    let expr = rewrite_affine_numeric_compare_expr(expr);
    let expr = fold_literal_only_where_expr(expr);
    let expr = simplify_where_boolean_constants(expr);

    normalize_bool_expr(expr)
}

pub(super) fn is_normalized_where_bool_expr(expr: &Expr) -> bool {
    is_normalized_bool_expr(expr)
}

// Rewrite the narrow affine numeric compare family that can be reduced onto
// the existing field-vs-literal predicate lane without introducing a new
// scalar residual-expression execution model.
fn rewrite_affine_numeric_compare_expr(expr: Expr) -> Expr {
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
                    crate::db::query::plan::expr::CaseWhenArm::new(
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

// Keep the top-level binary rewrite intentionally narrow:
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
        NumericArithmeticOp::Mul | NumericArithmeticOp::Div => return None,
    };

    Some((field.clone(), Value::Decimal(rewritten)))
}

// Extract the direct field plus/minus literal offset pattern admitted by this
// first affine WHERE rewrite slice.
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

// Fold literal-only scalar subtrees inside WHERE before normalization so the
// conservative predicate compiler can still reuse its existing field-vs-literal
// fast paths when the right-hand side is just a wrapped constant expression.
fn fold_literal_only_where_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Field(_) | Expr::Literal(_) | Expr::Aggregate(_) => expr,
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
                    crate::db::query::plan::expr::CaseWhenArm::new(
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

    literal_only_where_expr_value(&expr)
        .map(Expr::Literal)
        .unwrap_or(expr)
}

fn where_expr_is_literal_only(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Field(_) | Expr::Aggregate(_) => false,
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

fn literal_only_where_expr_value(expr: &Expr) -> Option<Value> {
    eval_builder_expr_for_value_preview(expr, "__where_const__", &Value::Null).ok()
}

// Simplify mixed boolean trees after literal-only folding so the conservative
// predicate compiler can still reuse one derived predicate lane when the other
// side of an AND/OR collapses to a constant boolean.
fn simplify_where_boolean_constants(expr: Expr) -> Expr {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => simplify_where_boolean_and(
            simplify_where_boolean_constants(*left),
            simplify_where_boolean_constants(*right),
        ),
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => simplify_where_boolean_or(
            simplify_where_boolean_constants(*left),
            simplify_where_boolean_constants(*right),
        ),
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(simplify_where_boolean_constants(*expr)),
        },
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(simplify_where_boolean_constants)
                .collect(),
        },
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    crate::db::query::plan::expr::CaseWhenArm::new(
                        simplify_where_boolean_constants(arm.condition().clone()),
                        simplify_where_boolean_constants(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(simplify_where_boolean_constants(*else_expr)),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(simplify_where_boolean_constants(*left)),
            right: Box::new(simplify_where_boolean_constants(*right)),
        },
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(simplify_where_boolean_constants(*expr)),
            name,
        },
        other => other,
    }
}

fn simplify_where_boolean_and(left: Expr, right: Expr) -> Expr {
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

fn simplify_where_boolean_or(left: Expr, right: Expr) -> Expr {
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
