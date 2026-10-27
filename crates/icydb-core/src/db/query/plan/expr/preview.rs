#[cfg(test)]
mod tests;

use crate::{
    db::{
        numeric::{
            apply_value_arithmetic_checked, coerce_numeric_decimal, compare_numeric_eq,
            compare_numeric_or_strict_order,
        },
        query::plan::expr::{
            BinaryOp, CaseWhenArm, Expr, Function, ScalarEvalFunctionShape, UnaryOp,
            collapse_true_only_boolean_admission, projection_eval::admit_replace_text,
        },
        resource::construction::ConstructionBudget,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::cmp::Ordering;

///
/// NullableTextArg
///
/// Bounded decode result for one literal-owned text argument used by planner
/// preview evaluation.
///
/// This keeps `NULL` distinct from unsupported non-text values without
/// relying on nested `Option<Option<_>>` plumbing.
///

enum NullableTextArg<'a> {
    Null,
    Text(&'a str),
}

///
/// NullableIntegerArg
///
/// Bounded decode result for one literal-owned integer argument used by
/// planner preview evaluation.
///
/// This keeps `NULL` distinct from unsupported non-integer values while
/// preserving the existing `Nat` saturation rule for helper functions.
///

enum NullableIntegerArg {
    Null,
    Integer(i64),
}

/// Evaluate one planner-owned expression only when every reachable subtree is
/// literal-owned and can therefore be folded without depending on runtime
/// field reads or executor projection machinery.
///
/// Lowering uses this bounded preview to collapse wrapped constant helper
/// expressions inside scalar `WHERE` before the later planner normalization
/// passes try to recover predicate-shaped fast paths.
pub(in crate::db) fn eval_literal_only_expr_value(
    expr: &Expr,
    work: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    Ok(match expr {
        Expr::Literal(value) => Some(work.copy_value(value)?),
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Aggregate(_) => None,
        Expr::FunctionCall { function, args } => {
            return eval_literal_only_function_call(*function, args, work);
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            return eval_literal_only_case_expr(when_then_arms, else_expr.as_ref(), work);
        }
        Expr::Binary { op, left, right } => {
            let Some(left) = eval_literal_only_expr_value(left.as_ref(), work)? else {
                return Ok(None);
            };
            let Some(right) = eval_literal_only_expr_value(right.as_ref(), work)? else {
                return Ok(None);
            };
            eval_literal_only_binary_expr(*op, &left, &right)
        }
        Expr::Unary { op, expr } => {
            let Some(value) = eval_literal_only_expr_value(expr.as_ref(), work)? else {
                return Ok(None);
            };
            eval_literal_only_unary_expr(*op, &value)
        }
    })
}

// Evaluate one literal-only CASE through the same TRUE-only admission owner,
// retaining short circuiting and typed construction failures.
fn eval_literal_only_case_expr(
    when_then_arms: &[CaseWhenArm],
    else_expr: &Expr,
    work: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    for arm in when_then_arms {
        let Some(condition) = eval_literal_only_expr_value(arm.condition(), work)? else {
            return Ok(None);
        };
        let Some(selected) = collapse_true_only_boolean_admission(condition, |_| ()).ok() else {
            return Ok(None);
        };
        if selected {
            return eval_literal_only_expr_value(arm.result(), work);
        }
    }
    eval_literal_only_expr_value(else_expr, work)
}

// Evaluate one literal-only function call through the bounded planner helper
// surface currently exercised by scalar WHERE constant-folding.
fn eval_literal_only_function_call(
    function: Function,
    args: &[Expr],
    work: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    let mut evaluated_args = work.vec_with_capacity(args.len())?;
    for arg in args {
        let Some(value) = eval_literal_only_expr_value(arg, work)? else {
            return Ok(None);
        };
        evaluated_args.push(value);
    }

    Ok(match function.scalar_eval_shape() {
        ScalarEvalFunctionShape::NullTest => {
            eval_null_test_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::NonExecutableProjection | ScalarEvalFunctionShape::Membership => {
            None
        }
        ScalarEvalFunctionShape::UnaryText => {
            return eval_unary_text_function_call(function, &evaluated_args, work);
        }
        ScalarEvalFunctionShape::DynamicCoalesce => {
            eval_coalesce_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::DynamicNullIf => {
            eval_nullif_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::UnaryNumeric => {
            eval_unary_numeric_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::BinaryNumeric => {
            eval_binary_numeric_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::LeftRightText => {
            return eval_left_right_text_function_call(function, &evaluated_args, work);
        }
        ScalarEvalFunctionShape::TextPredicate => {
            eval_text_predicate_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::PositionText => {
            eval_position_text_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::ReplaceText => {
            return eval_replace_text_function_call(function, &evaluated_args, work);
        }
        ScalarEvalFunctionShape::SubstringText => {
            return eval_substring_text_function_call(function, &evaluated_args, work);
        }
        ScalarEvalFunctionShape::NumericScale => {
            eval_numeric_scale_function_call(function, &evaluated_args)
        }
        ScalarEvalFunctionShape::OctetLength => eval_octet_length_function_call(&evaluated_args),
    })
}

// Evaluate one literal-only unary expression without touching executor-owned
// projection error taxonomy or runtime readers.
fn eval_literal_only_unary_expr(op: UnaryOp, value: &Value) -> Option<Value> {
    if matches!(value, Value::Null) {
        return Some(Value::Null);
    }

    match op {
        UnaryOp::Not => match value {
            Value::Bool(inner) => Some(Value::Bool(!inner)),
            _ => None,
        },
    }
}

// Evaluate one literal-only binary expression through planner-owned numeric and
// value comparison helpers.
fn eval_literal_only_binary_expr(op: BinaryOp, left: &Value, right: &Value) -> Option<Value> {
    match op {
        BinaryOp::Or | BinaryOp::And => eval_boolean_binary_expr(op, left, right),
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => eval_compare_binary_expr(op, left, right),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Some(Value::Null);
            }

            let arithmetic_op = op.numeric_arithmetic_op()?;

            apply_value_arithmetic_checked(arithmetic_op, left, right)
                .ok()
                .flatten()
        }
    }
}

// Evaluate one literal-only boolean AND/OR using the shared three-valued
// truth table used by scalar projection evaluation.
const fn eval_boolean_binary_expr(op: BinaryOp, left: &Value, right: &Value) -> Option<Value> {
    match op {
        BinaryOp::And => match (left, right) {
            (Value::Bool(false), _) | (_, Value::Bool(false)) => Some(Value::Bool(false)),
            (Value::Bool(true), Value::Bool(true)) => Some(Value::Bool(true)),
            (Value::Bool(true) | Value::Null, Value::Null) | (Value::Null, Value::Bool(true)) => {
                Some(Value::Null)
            }
            _ => None,
        },
        BinaryOp::Or => match (left, right) {
            (Value::Bool(true), _) | (_, Value::Bool(true)) => Some(Value::Bool(true)),
            (Value::Bool(false), Value::Bool(false)) => Some(Value::Bool(false)),
            (Value::Bool(false) | Value::Null, Value::Null) | (Value::Null, Value::Bool(false)) => {
                Some(Value::Null)
            }
            _ => None,
        },
        _ => None,
    }
}

// Evaluate one literal-only compare using the same numeric-widen versus strict
// fallback rule already shared elsewhere in planner/runtime comparison helpers.
fn eval_compare_binary_expr(op: BinaryOp, left: &Value, right: &Value) -> Option<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Some(Value::Null);
    }

    let numeric_widen_enabled =
        left.supports_numeric_coercion() || right.supports_numeric_coercion();
    let result = match op {
        BinaryOp::Eq => {
            if let Some(equal) = compare_numeric_eq(left, right) {
                equal
            } else if !numeric_widen_enabled {
                left == right
            } else {
                return None;
            }
        }
        BinaryOp::Ne => {
            if let Some(equal) = compare_numeric_eq(left, right) {
                !equal
            } else if !numeric_widen_enabled {
                left != right
            } else {
                return None;
            }
        }
        BinaryOp::Lt => compare_numeric_or_strict_order(left, right).map(Ordering::is_lt)?,
        BinaryOp::Lte => compare_numeric_or_strict_order(left, right).map(Ordering::is_le)?,
        BinaryOp::Gt => compare_numeric_or_strict_order(left, right).map(Ordering::is_gt)?,
        BinaryOp::Gte => compare_numeric_or_strict_order(left, right).map(Ordering::is_ge)?,
        _ => return None,
    };

    Some(Value::Bool(result))
}

// Evaluate one NULL-test function when its only input is already literal-owned.
fn eval_null_test_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [value] = args else {
        return None;
    };

    Some(function.boolean_null_test_kind()?.eval_value(value))
}

// Evaluate one text wrapper over a literal-owned text input.
fn eval_unary_text_function_call(
    function: Function,
    args: &[Value],
    work: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    let [input] = args else {
        return Ok(None);
    };

    Ok(match input {
        Value::Null => Some(Value::Null),
        Value::Text(text) => {
            let Some(kind) = function.unary_text_function_kind() else {
                return Ok(None);
            };
            Some(kind.eval_text(text, |resource, amount| work.charge(resource, amount))?)
        }
        _ => None,
    })
}

// Evaluate one numeric wrapper over a literal-owned numeric input.
fn eval_unary_numeric_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [input] = args else {
        return None;
    };

    match input {
        Value::Null => Some(Value::Null),
        value => {
            let decimal = coerce_numeric_decimal(value)?;

            function
                .unary_numeric_function_kind()?
                .eval_decimal(decimal)
                .ok()
        }
    }
}

// Evaluate one byte-length wrapper over a literal-owned text or blob input.
fn eval_octet_length_function_call(args: &[Value]) -> Option<Value> {
    let [input] = args else {
        return None;
    };

    match input {
        Value::Null => Some(Value::Null),
        Value::Text(text) => Some(Value::Nat64(u64::try_from(text.len()).unwrap_or(u64::MAX))),
        Value::Blob(bytes) => Some(Value::Nat64(u64::try_from(bytes.len()).unwrap_or(u64::MAX))),
        _ => None,
    }
}

// Evaluate one binary numeric wrapper over literal-owned numeric inputs.
fn eval_binary_numeric_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [left, right] = args else {
        return None;
    };

    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Some(Value::Null),
        (left, right) => function
            .binary_numeric_function_kind()?
            .eval_values(left, right)
            .ok()
            .flatten(),
    }
}

// Evaluate one literal-only COALESCE helper.
fn eval_coalesce_function_call(function: Function, args: &[Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }

    Some(function.eval_coalesce_values(args))
}

// Evaluate one literal-only NULLIF helper through the same compare semantics as
// the preview binary compare path.
fn eval_nullif_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [left, right] = args else {
        return None;
    };

    match eval_compare_binary_expr(BinaryOp::Eq, left, right)? {
        Value::Bool(true) => Some(function.eval_nullif_values(left, right, true)),
        Value::Bool(false) => Some(function.eval_nullif_values(left, right, false)),
        _ => None,
    }
}

// Evaluate one literal-only LEFT/RIGHT helper with the same integer coercion
// boundary already used by runtime projection evaluation.
fn eval_left_right_text_function_call(
    function: Function,
    args: &[Value],
    work: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    let [input, length] = args else {
        return Ok(None);
    };
    let Some(length) = integer_value(length) else {
        return Ok(None);
    };

    Ok(match (input, length) {
        (Value::Null, _) | (_, NullableIntegerArg::Null) => Some(Value::Null),
        (Value::Text(text), NullableIntegerArg::Integer(length)) => {
            let Some(kind) = function.left_right_text_function_kind() else {
                return Ok(None);
            };
            Some(kind.eval_text(text, length, |resource, amount| {
                work.charge(resource, amount)
            })?)
        }
        _ => None,
    })
}

// Evaluate one literal-only text predicate helper.
fn eval_text_predicate_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [input, literal] = args else {
        return None;
    };
    let literal = text_value(literal)?;

    match (input, literal) {
        (Value::Null, _) | (_, NullableTextArg::Null) => Some(Value::Null),
        (Value::Text(text), NullableTextArg::Text(needle)) => Some(
            function
                .boolean_text_predicate_kind()?
                .eval_text(text, needle),
        ),
        _ => None,
    }
}

// Evaluate one literal-only POSITION helper.
fn eval_position_text_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [needle, input] = args else {
        return None;
    };
    let needle = text_value(needle)?;

    match (needle, input) {
        (_, Value::Null) | (NullableTextArg::Null, _) => Some(Value::Null),
        (NullableTextArg::Text(needle), Value::Text(text)) => {
            Some(function.eval_position_text(text, needle))
        }
        _ => None,
    }
}

// Evaluate one literal-only REPLACE helper.
fn eval_replace_text_function_call(
    function: Function,
    args: &[Value],
    work: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    let [input, from, to] = args else {
        return Ok(None);
    };
    let (Some(from), Some(to)) = (text_value(from), text_value(to)) else {
        return Ok(None);
    };
    Ok(match (input, from, to) {
        (Value::Null, _, _) | (_, NullableTextArg::Null, _) | (_, _, NullableTextArg::Null) => {
            Some(Value::Null)
        }
        (Value::Text(text), NullableTextArg::Text(from), NullableTextArg::Text(to)) => {
            let size = admit_replace_text(text, from, to, |resource, amount| {
                work.charge(resource, amount)
            })?;
            Some(function.eval_replace_text(text, from, to, size))
        }
        _ => None,
    })
}

// Evaluate one literal-only SUBSTRING helper.
fn eval_substring_text_function_call(
    function: Function,
    args: &[Value],
    work: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    let [input, start, rest @ ..] = args else {
        return Ok(None);
    };
    let Some(start) = integer_value(start) else {
        return Ok(None);
    };
    let length = match rest {
        [] => Some(None),
        [length] => integer_value(length).map(|length| match length {
            NullableIntegerArg::Null => None,
            NullableIntegerArg::Integer(value) => Some(value),
        }),
        _ => None,
    };
    let Some(length) = length else {
        return Ok(None);
    };

    Ok(match (input, start) {
        (Value::Null, _) | (_, NullableIntegerArg::Null) => Some(Value::Null),
        (Value::Text(text), NullableIntegerArg::Integer(start)) => Some(
            function.eval_substring_text(text, start, length, |resource, amount| {
                work.charge(resource, amount)
            })?,
        ),
        _ => None,
    })
}

// Evaluate one literal-only ROUND helper.
fn eval_numeric_scale_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [input, scale] = args else {
        return None;
    };
    let scale = integer_value(scale)?;

    match (input, scale) {
        (Value::Null, _) | (_, NullableIntegerArg::Null) => Some(Value::Null),
        (value, NullableIntegerArg::Integer(scale)) => {
            let scale = u32::try_from(scale).ok()?;

            function.eval_numeric_scale(value, scale)
        }
    }
}

// Decode one literal text argument, preserving NULL as its own boundary.
const fn text_value(value: &Value) -> Option<NullableTextArg<'_>> {
    match value {
        Value::Null => Some(NullableTextArg::Null),
        Value::Text(text) => Some(NullableTextArg::Text(text.as_str())),
        _ => None,
    }
}

// Decode one literal integer argument, preserving NULL as its own
// boundary while still accepting `Nat`.
fn integer_value(value: &Value) -> Option<NullableIntegerArg> {
    match value {
        Value::Null => Some(NullableIntegerArg::Null),
        Value::Int64(inner) => Some(NullableIntegerArg::Integer(*inner)),
        Value::Nat64(inner) => Some(NullableIntegerArg::Integer(
            i64::try_from(*inner).unwrap_or(i64::MAX),
        )),
        _ => None,
    }
}
