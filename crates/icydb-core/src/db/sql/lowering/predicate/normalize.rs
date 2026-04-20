use crate::{
    db::{
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        predicate::{is_normalized_bool_expr, normalize_bool_expr},
        query::plan::expr::{BinaryOp, Expr},
    },
    value::Value,
};

pub(super) fn normalize_where_bool_expr(expr: Expr) -> Expr {
    let expr = rewrite_affine_numeric_compare_expr(expr);

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
