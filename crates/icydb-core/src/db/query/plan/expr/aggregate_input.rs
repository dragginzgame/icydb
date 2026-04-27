//! Module: query::plan::expr::aggregate_input
//! Responsibility: aggregate-input canonicalization shared by planner builders and frontend lowering.
//! Does not own: aggregate validation policy, grouped execution wiring, or parser frontends.
//! Boundary: one planner-owned normalization seam for constant folding and numeric literal shaping.

use crate::{
    db::{
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic, coerce_numeric_decimal},
        query::plan::{
            AggregateKind,
            expr::{AggregateInputConstantFoldShape, BinaryOp, Expr, Function},
        },
    },
    value::Value,
};

// Keep aggregate input identity canonical anywhere planner-owned aggregate
// expressions are constructed so grouped/global paths do not drift on
// semantically equivalent constant subexpressions.
pub(in crate::db) fn canonicalize_aggregate_input_expr(kind: AggregateKind, expr: Expr) -> Expr {
    let folded =
        normalize_aggregate_input_numeric_literals(fold_aggregate_input_constant_expr(expr));

    match kind {
        AggregateKind::Sum | AggregateKind::Avg => match folded {
            Expr::Literal(value) => coerce_numeric_decimal(&value)
                .map_or(Expr::Literal(value), |decimal| {
                    Expr::Literal(Value::Decimal(decimal.normalize()))
                }),
            other => other,
        },
        AggregateKind::Count
        | AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::Exists
        | AggregateKind::First
        | AggregateKind::Last => folded,
    }
}

// Fold literal-only aggregate-input subexpressions so semantic aggregate
// matching can treat `AVG(age + 1 * 2)` and `AVG(age + 2)` as the same input.
fn fold_aggregate_input_constant_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Field(_) | Expr::Literal(_) | Expr::Aggregate(_) => expr,
        Expr::FunctionCall { function, args } => {
            let args = args
                .into_iter()
                .map(fold_aggregate_input_constant_expr)
                .collect::<Vec<_>>();

            fold_aggregate_input_constant_function(function, args.as_slice())
                .unwrap_or(Expr::FunctionCall { function, args })
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    crate::db::query::plan::expr::CaseWhenArm::new(
                        fold_aggregate_input_constant_expr(arm.condition().clone()),
                        fold_aggregate_input_constant_expr(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(fold_aggregate_input_constant_expr(*else_expr)),
        },
        Expr::Binary { op, left, right } => {
            let left = fold_aggregate_input_constant_expr(*left);
            let right = fold_aggregate_input_constant_expr(*right);

            fold_aggregate_input_constant_binary(op, &left, &right).unwrap_or_else(|| {
                Expr::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            })
        }
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(fold_aggregate_input_constant_expr(*expr)),
            name,
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(fold_aggregate_input_constant_expr(*expr)),
        },
    }
}

// Fold one literal-only binary aggregate-input fragment onto one decimal
// literal so aggregate identity stays stable across equivalent frontend spellings.
fn fold_aggregate_input_constant_binary(op: BinaryOp, left: &Expr, right: &Expr) -> Option<Expr> {
    let (Expr::Literal(left), Expr::Literal(right)) = (left, right) else {
        return None;
    };
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let arithmetic_op = match op {
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => return None,
        BinaryOp::Add => NumericArithmeticOp::Add,
        BinaryOp::Sub => NumericArithmeticOp::Sub,
        BinaryOp::Mul => NumericArithmeticOp::Mul,
        BinaryOp::Div => NumericArithmeticOp::Div,
    };
    let result = apply_numeric_arithmetic(arithmetic_op, left, right)?;

    Some(Expr::Literal(Value::Decimal(result)))
}

// Fold one admitted literal-only aggregate-input function call when the
// reduced aggregate-input family has one deterministic literal result.
fn fold_aggregate_input_constant_function(function: Function, args: &[Expr]) -> Option<Expr> {
    match function.aggregate_input_constant_fold_shape() {
        Some(AggregateInputConstantFoldShape::Round) => {
            fold_aggregate_input_constant_round(function, args)
        }
        Some(AggregateInputConstantFoldShape::DynamicCoalesce) => {
            fold_aggregate_input_constant_coalesce(args)
        }
        Some(AggregateInputConstantFoldShape::DynamicNullIf) => {
            fold_aggregate_input_constant_nullif(args)
        }
        Some(AggregateInputConstantFoldShape::BinaryNumeric) => {
            fold_aggregate_input_constant_binary_numeric(function, args)
        }
        Some(AggregateInputConstantFoldShape::UnaryNumeric) => {
            fold_aggregate_input_constant_unary_numeric(function, args)
        }
        None => None,
    }
}

// Fold one admitted unary numeric aggregate-input wrapper through the shared
// planner numeric contract so literal-only numeric calls keep one canonical
// aggregate identity.
fn fold_aggregate_input_constant_unary_numeric(function: Function, args: &[Expr]) -> Option<Expr> {
    let [Expr::Literal(input)] = args else {
        return None;
    };
    if matches!(input, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let decimal = coerce_numeric_decimal(input)?;
    let result = function
        .unary_numeric_function_kind()?
        .eval_decimal(decimal)?;

    Some(Expr::Literal(result))
}

// Fold one admitted binary numeric aggregate-input wrapper through the shared
// planner numeric contract so literal-only numeric calls keep one canonical
// aggregate identity.
fn fold_aggregate_input_constant_binary_numeric(function: Function, args: &[Expr]) -> Option<Expr> {
    let [Expr::Literal(left), Expr::Literal(right)] = args else {
        return None;
    };
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let left = coerce_numeric_decimal(left)?;
    let right = coerce_numeric_decimal(right)?;
    let result = function
        .binary_numeric_function_kind()?
        .eval_decimal(left, right)?;

    Some(Expr::Literal(result))
}

// Fold one literal-only ROUND(...) aggregate-input fragment so parenthesized
// constant arithmetic keeps the same aggregate identity as its literal result.
fn fold_aggregate_input_constant_round(function: Function, args: &[Expr]) -> Option<Expr> {
    let [Expr::Literal(input), Expr::Literal(scale)] = args else {
        return None;
    };
    if matches!(input, Value::Null) || matches!(scale, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let scale = match scale {
        Value::Int(value) => u32::try_from(*value).ok()?,
        Value::Uint(value) => u32::try_from(*value).ok()?,
        _ => return None,
    };

    Some(Expr::Literal(function.eval_numeric_scale(input, scale)?))
}

// Fold one literal-only COALESCE aggregate-input subtree so all-null versus
// first-non-null behavior stays stable before aggregate dedupe compares inputs.
fn fold_aggregate_input_constant_coalesce(args: &[Expr]) -> Option<Expr> {
    let mut literal_values = Vec::with_capacity(args.len());
    for arg in args {
        let Expr::Literal(value) = arg else {
            return None;
        };
        literal_values.push(value.clone());
    }

    Some(Expr::Literal(
        Function::Coalesce.eval_coalesce_values(literal_values.as_slice()),
    ))
}

// Fold one literal-only NULLIF aggregate-input subtree so equivalent frontend
// spellings collapse to the same planner literal before aggregate matching.
fn fold_aggregate_input_constant_nullif(args: &[Expr]) -> Option<Expr> {
    let [Expr::Literal(left), Expr::Literal(right)] = args else {
        return None;
    };

    Some(Expr::Literal(Function::NullIf.eval_nullif_values(
        left,
        right,
        left == right,
    )))
}

// Normalize numeric literal leaves recursively so semantically equivalent
// aggregate inputs like `age + 2` and `age + 1 * 2` share one canonical
// planner identity after literal-only subtree folding.
fn normalize_aggregate_input_numeric_literals(expr: Expr) -> Expr {
    match expr {
        Expr::Literal(value) => coerce_numeric_decimal(&value)
            .map_or(Expr::Literal(value), |decimal| {
                Expr::Literal(Value::Decimal(decimal.normalize()))
            }),
        Expr::Field(_) | Expr::Aggregate(_) => expr,
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(normalize_aggregate_input_numeric_literals)
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
                        normalize_aggregate_input_numeric_literals(arm.condition().clone()),
                        normalize_aggregate_input_numeric_literals(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(normalize_aggregate_input_numeric_literals(*else_expr)),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(normalize_aggregate_input_numeric_literals(*left)),
            right: Box::new(normalize_aggregate_input_numeric_literals(*right)),
        },
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(normalize_aggregate_input_numeric_literals(*expr)),
            name,
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(normalize_aggregate_input_numeric_literals(*expr)),
        },
    }
}
