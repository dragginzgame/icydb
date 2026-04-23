use crate::{
    db::{
        numeric::{
            NumericArithmeticOp, apply_numeric_arithmetic, compare_numeric_eq,
            compare_numeric_or_strict_order,
        },
        query::plan::expr::{
            BinaryOp, CaseWhenArm, Expr, Function, UnaryOp, collapse_true_only_boolean_admission,
        },
    },
    value::Value,
};
use std::cmp::Ordering;

///
/// NullableTextArg
///
/// Bounded decode result for one literal-owned text argument used by planner
/// preview evaluation.
///
/// This keeps SQL `NULL` distinct from unsupported non-text values without
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
/// This keeps SQL `NULL` distinct from unsupported non-integer values while
/// preserving the existing `Uint` saturation rule for helper functions.
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
pub(in crate::db) fn eval_literal_only_expr_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Field(_) | Expr::Aggregate(_) => None,
        Expr::FunctionCall { function, args } => eval_literal_only_function_call(*function, args),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => eval_literal_only_case_expr(when_then_arms, else_expr.as_ref()),
        Expr::Binary { op, left, right } => {
            let left = eval_literal_only_expr_value(left.as_ref())?;
            let right = eval_literal_only_expr_value(right.as_ref())?;

            eval_literal_only_binary_expr(*op, &left, &right)
        }
        Expr::Unary { op, expr } => {
            let value = eval_literal_only_expr_value(expr.as_ref())?;

            eval_literal_only_unary_expr(*op, &value)
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => eval_literal_only_expr_value(expr.as_ref()),
    }
}

// Evaluate one literal-only CASE expression through the shared TRUE-only
// boolean admission boundary used elsewhere in planner-owned boolean semantics.
fn eval_literal_only_case_expr(when_then_arms: &[CaseWhenArm], else_expr: &Expr) -> Option<Value> {
    for arm in when_then_arms {
        let condition = eval_literal_only_expr_value(arm.condition())?;
        if collapse_true_only_boolean_admission(condition, |_| ()).ok()? {
            return eval_literal_only_expr_value(arm.result());
        }
    }

    eval_literal_only_expr_value(else_expr)
}

// Evaluate one literal-only function call through the bounded planner helper
// surface currently exercised by scalar WHERE constant-folding.
fn eval_literal_only_function_call(function: Function, args: &[Expr]) -> Option<Value> {
    let evaluated_args = args
        .iter()
        .map(eval_literal_only_expr_value)
        .collect::<Option<Vec<_>>>()?;

    match function {
        Function::IsNull | Function::IsNotNull => {
            eval_null_test_function_call(function, &evaluated_args)
        }
        Function::IsMissing
        | Function::IsEmpty
        | Function::IsNotEmpty
        | Function::CollectionContains => None,
        Function::Trim
        | Function::Ltrim
        | Function::Rtrim
        | Function::Lower
        | Function::Upper
        | Function::Length => eval_unary_text_function_call(function, &evaluated_args),
        Function::Coalesce => eval_coalesce_function_call(&evaluated_args),
        Function::NullIf => eval_nullif_function_call(&evaluated_args),
        Function::Abs | Function::Ceiling | Function::Floor => {
            eval_unary_numeric_function_call(function, &evaluated_args)
        }
        Function::Left | Function::Right => {
            eval_left_right_text_function_call(function, &evaluated_args)
        }
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            eval_text_predicate_function_call(function, &evaluated_args)
        }
        Function::Position => eval_position_text_function_call(&evaluated_args),
        Function::Replace => eval_replace_text_function_call(&evaluated_args),
        Function::Substring => eval_substring_text_function_call(&evaluated_args),
        Function::Round => eval_round_function_call(&evaluated_args),
    }
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

            let arithmetic_op = match op {
                BinaryOp::Add => NumericArithmeticOp::Add,
                BinaryOp::Sub => NumericArithmeticOp::Sub,
                BinaryOp::Mul => NumericArithmeticOp::Mul,
                BinaryOp::Div => NumericArithmeticOp::Div,
                _ => unreachable!("arithmetic dispatch drifted"),
            };

            apply_numeric_arithmetic(arithmetic_op, left, right).map(Value::Decimal)
        }
    }
}

// Evaluate one literal-only boolean AND/OR using the shared three-valued
// truth table used by scalar projection evaluation.
fn eval_boolean_binary_expr(op: BinaryOp, left: &Value, right: &Value) -> Option<Value> {
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
        _ => unreachable!("boolean binary dispatch drifted"),
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
        _ => unreachable!("compare dispatch drifted"),
    };

    Some(Value::Bool(result))
}

// Evaluate one NULL-test function when its only input is already literal-owned.
fn eval_null_test_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [value] = args else {
        return None;
    };

    Some(Value::Bool(match function {
        Function::IsNull => matches!(value, Value::Null),
        Function::IsNotNull => !matches!(value, Value::Null),
        _ => unreachable!("null-test dispatch drifted"),
    }))
}

// Evaluate one text wrapper over a literal-owned text input.
fn eval_unary_text_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [input] = args else {
        return None;
    };

    match input {
        Value::Null => Some(Value::Null),
        Value::Text(text) => Some(match function {
            Function::Trim => Value::Text(text.trim().to_string()),
            Function::Ltrim => Value::Text(text.trim_start().to_string()),
            Function::Rtrim => Value::Text(text.trim_end().to_string()),
            Function::Lower => Value::Text(text.to_lowercase()),
            Function::Upper => Value::Text(text.to_uppercase()),
            Function::Length => {
                Value::Uint(u64::try_from(text.chars().count()).unwrap_or(u64::MAX))
            }
            _ => unreachable!("text wrapper dispatch drifted"),
        }),
        _ => None,
    }
}

// Evaluate one numeric wrapper over a literal-owned numeric input.
fn eval_unary_numeric_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [input] = args else {
        return None;
    };

    match input {
        Value::Null => Some(Value::Null),
        value => {
            let decimal = value.to_numeric_decimal()?;

            Some(Value::Decimal(match function {
                Function::Abs => decimal.abs(),
                Function::Ceiling => decimal.ceil_dp0(),
                Function::Floor => decimal.floor_dp0(),
                _ => unreachable!("numeric wrapper dispatch drifted"),
            }))
        }
    }
}

// Evaluate one literal-only COALESCE helper.
fn eval_coalesce_function_call(args: &[Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }

    Some(
        args.iter()
            .find(|value| !matches!(value, Value::Null))
            .cloned()
            .unwrap_or(Value::Null),
    )
}

// Evaluate one literal-only NULLIF helper through the same compare semantics as
// the preview binary compare path.
fn eval_nullif_function_call(args: &[Value]) -> Option<Value> {
    let [left, right] = args else {
        return None;
    };

    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Some(left.clone());
    }

    match eval_compare_binary_expr(BinaryOp::Eq, left, right)? {
        Value::Bool(true) => Some(Value::Null),
        Value::Bool(false) => Some(left.clone()),
        _ => None,
    }
}

// Evaluate one literal-only LEFT/RIGHT helper with the same integer coercion
// boundary already used by runtime projection evaluation.
fn eval_left_right_text_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [input, length] = args else {
        return None;
    };
    let length = integer_value(length)?;

    match (input, length) {
        (Value::Null, _) | (_, NullableIntegerArg::Null) => Some(Value::Null),
        (Value::Text(text), NullableIntegerArg::Integer(length)) => {
            Some(Value::Text(match function {
                Function::Left => left_chars(text.as_str(), length),
                Function::Right => right_chars(text.as_str(), length),
                _ => unreachable!("left/right dispatch drifted"),
            }))
        }
        _ => None,
    }
}

// Evaluate one literal-only text predicate helper.
fn eval_text_predicate_function_call(function: Function, args: &[Value]) -> Option<Value> {
    let [input, literal] = args else {
        return None;
    };
    let literal = text_value(literal)?;

    match (input, literal) {
        (Value::Null, _) | (_, NullableTextArg::Null) => Some(Value::Null),
        (Value::Text(text), NullableTextArg::Text(needle)) => Some(Value::Bool(match function {
            Function::StartsWith => text.starts_with(needle),
            Function::EndsWith => text.ends_with(needle),
            Function::Contains => text.contains(needle),
            _ => unreachable!("text predicate dispatch drifted"),
        })),
        _ => None,
    }
}

// Evaluate one literal-only POSITION helper.
fn eval_position_text_function_call(args: &[Value]) -> Option<Value> {
    let [needle, input] = args else {
        return None;
    };
    let needle = text_value(needle)?;

    match (needle, input) {
        (_, Value::Null) | (NullableTextArg::Null, _) => Some(Value::Null),
        (NullableTextArg::Text(needle), Value::Text(text)) => {
            Some(Value::Uint(text_position_1_based(text, needle)))
        }
        _ => None,
    }
}

// Evaluate one literal-only REPLACE helper.
fn eval_replace_text_function_call(args: &[Value]) -> Option<Value> {
    let [input, from, to] = args else {
        return None;
    };
    let from = text_value(from)?;
    let to = text_value(to)?;

    match (input, from, to) {
        (Value::Null, _, _) | (_, NullableTextArg::Null, _) | (_, _, NullableTextArg::Null) => {
            Some(Value::Null)
        }
        (Value::Text(text), NullableTextArg::Text(from), NullableTextArg::Text(to)) => {
            Some(Value::Text(text.replace(from, to)))
        }
        _ => None,
    }
}

// Evaluate one literal-only SUBSTRING helper.
fn eval_substring_text_function_call(args: &[Value]) -> Option<Value> {
    let [input, start, rest @ ..] = args else {
        return None;
    };
    let start = integer_value(start)?;
    let length = match rest {
        [] => Some(None),
        [length] => Some(match integer_value(length)? {
            NullableIntegerArg::Null => None,
            NullableIntegerArg::Integer(value) => Some(value),
        }),
        _ => None,
    }?;

    match (input, start) {
        (Value::Null, _) | (_, NullableIntegerArg::Null) => Some(Value::Null),
        (Value::Text(text), NullableIntegerArg::Integer(start)) => {
            Some(Value::Text(substring_1_based(text, start, length)))
        }
        _ => None,
    }
}

// Evaluate one literal-only ROUND helper.
fn eval_round_function_call(args: &[Value]) -> Option<Value> {
    let [input, scale] = args else {
        return None;
    };
    let scale = integer_value(scale)?;

    match (input, scale) {
        (Value::Null, _) | (_, NullableIntegerArg::Null) => Some(Value::Null),
        (value, NullableIntegerArg::Integer(scale)) => {
            let scale = u32::try_from(scale).ok()?;
            let decimal = value.to_numeric_decimal()?;

            Some(Value::Decimal(decimal.round_dp(scale)))
        }
    }
}

// Decode one literal text argument, preserving SQL NULL as its own boundary.
const fn text_value(value: &Value) -> Option<NullableTextArg<'_>> {
    match value {
        Value::Null => Some(NullableTextArg::Null),
        Value::Text(text) => Some(NullableTextArg::Text(text.as_str())),
        _ => None,
    }
}

// Decode one literal integer argument, preserving SQL NULL as its own
// boundary while still accepting `Uint`.
fn integer_value(value: &Value) -> Option<NullableIntegerArg> {
    match value {
        Value::Null => Some(NullableIntegerArg::Null),
        Value::Int(inner) => Some(NullableIntegerArg::Integer(*inner)),
        Value::Uint(inner) => Some(NullableIntegerArg::Integer(
            i64::try_from(*inner).unwrap_or(i64::MAX),
        )),
        _ => None,
    }
}

// Convert one found substring byte offset into the stable 1-based SQL char
// position used by POSITION(...).
fn text_position_1_based(haystack: &str, needle: &str) -> u64 {
    let Some(byte_index) = haystack.find(needle) else {
        return 0;
    };
    let char_offset = haystack[..byte_index].chars().count();

    u64::try_from(char_offset)
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

// Return the first N chars from one text input while keeping negative/zero
// lengths on the empty-string SQL boundary.
fn left_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    text.chars()
        .take(usize::try_from(count).unwrap_or(usize::MAX))
        .collect()
}

// Return the last N chars from one text input while keeping negative/zero
// lengths on the empty-string SQL boundary.
fn right_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    let count = usize::try_from(count).unwrap_or(usize::MAX);
    let total = text.chars().count();
    let skip = total.saturating_sub(count);

    text.chars().skip(skip).collect()
}

// Slice one text input using SQL-style 1-based substring coordinates.
fn substring_1_based(text: &str, start: i64, length: Option<i64>) -> String {
    if start <= 0 {
        return String::new();
    }
    if matches!(length, Some(inner) if inner <= 0) {
        return String::new();
    }

    let start_index = usize::try_from(start.saturating_sub(1)).unwrap_or(usize::MAX);
    let chars = text.chars().skip(start_index);

    match length {
        Some(length) => chars
            .take(usize::try_from(length).unwrap_or(usize::MAX))
            .collect(),
        None => chars.collect(),
    }
}
