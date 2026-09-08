use crate::{
    db::{
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic_checked},
        query::plan::expr::{BinaryOp, Expr},
    },
    value::Value,
};

/// Rewrite the planner-owned affine numeric compare family that can already
/// reduce onto the existing field-vs-literal predicate lane.
#[must_use]
pub(in crate::db) fn rewrite_affine_numeric_compare_expr(mut expr: Expr) -> Expr {
    #[cfg(test)]
    if matches!(expr, Expr::Alias { .. }) {
        return expr;
    }
    expr.map_scalar_children(rewrite_affine_numeric_compare_expr);
    if let Expr::Binary { op, left, right } = &mut expr
        && !matches!(op, BinaryOp::And | BinaryOp::Or)
    {
        return rewrite_affine_compare_binary(*op, left.take(), right.take());
    }

    expr
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

    if let Some((field, value)) = rewrite_affine_field_compare(&right, &left)
        && let Some(flipped_op) = flip_compare_binary_op(compare_op)
    {
        return Expr::Binary {
            op: flipped_op,
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
            apply_numeric_arithmetic_checked(NumericArithmeticOp::Sub, target, offset)
                .ok()
                .flatten()?
        }
        NumericArithmeticOp::Sub => {
            apply_numeric_arithmetic_checked(NumericArithmeticOp::Add, target, offset)
                .ok()
                .flatten()?
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

const fn flip_compare_binary_op(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Eq => Some(BinaryOp::Eq),
        BinaryOp::Ne => Some(BinaryOp::Ne),
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::Lte => Some(BinaryOp::Gte),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::Gte => Some(BinaryOp::Lte),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => None,
    }
}
