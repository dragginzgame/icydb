//! Module: db::executor::projection::eval::scalar_function
//! Responsibility: bounded scalar-function evaluation for scalar projection
//! execution.
//! Does not own: SQL parsing, planner validation, or grouped-lowering policy.
//! Boundary: executor-owned runtime semantics for canonical `Expr::FunctionCall`
//! values admitted on the narrowed scalar projection slice.

use crate::{
    db::{
        QueryError,
        query::plan::expr::{BinaryOp, Expr, Function},
    },
    value::Value,
};

pub(in crate::db) const fn projection_function_name(function: Function) -> &'static str {
    match function {
        Function::IsNull => "is_null",
        Function::IsNotNull => "is_not_null",
        Function::IsMissing => "is_missing",
        Function::IsEmpty => "is_empty",
        Function::IsNotEmpty => "is_not_empty",
        Function::Trim => "trim",
        Function::Ltrim => "ltrim",
        Function::Rtrim => "rtrim",
        Function::Coalesce => "coalesce",
        Function::NullIf => "nullif",
        Function::Abs => "abs",
        Function::Ceil => "ceil",
        Function::Ceiling => "ceiling",
        Function::Floor => "floor",
        Function::Lower => "lower",
        Function::Upper => "upper",
        Function::Length => "length",
        Function::Left => "left",
        Function::Right => "right",
        Function::StartsWith => "starts_with",
        Function::EndsWith => "ends_with",
        Function::Contains => "contains",
        Function::CollectionContains => "collection_contains",
        Function::Position => "position",
        Function::Replace => "replace",
        Function::Substring => "substring",
        Function::Round => "round",
    }
}

/// Evaluate one bounded projection-function call over already-evaluated
/// argument values.
pub(in crate::db) fn eval_projection_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    match function {
        Function::IsNull | Function::IsNotNull => eval_null_test_function_call(function, args),
        Function::IsMissing
        | Function::IsEmpty
        | Function::IsNotEmpty
        | Function::CollectionContains => Err(QueryError::invariant(format!(
            "projection function '{}' is not executable in scalar projection evaluation",
            projection_function_name(function),
        ))),
        Function::Trim
        | Function::Ltrim
        | Function::Rtrim
        | Function::Lower
        | Function::Upper
        | Function::Length => eval_unary_text_function_call(function, args),
        Function::Coalesce => eval_coalesce_function_call(function, args),
        Function::NullIf => eval_nullif_function_call(function, args),
        Function::Abs | Function::Ceil | Function::Ceiling | Function::Floor => {
            eval_unary_numeric_function_call(function, args)
        }
        Function::Left | Function::Right => eval_left_right_text_function_call(function, args),
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            eval_text_predicate_function_call(function, args)
        }
        Function::Position => eval_position_text_function_call(function, args),
        Function::Replace => eval_replace_text_function_call(function, args),
        Function::Substring => eval_substring_text_function_call(function, args),
        Function::Round => eval_round_function_call(function, args),
    }
}

fn eval_null_test_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    let value = required_function_arg(function, args, 0, "value")?;

    if args.len() != 1 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected 1 argument but received {}",
            projection_function_name(function),
            args.len(),
        )));
    }

    Ok(Value::Bool(match function {
        Function::IsNull => matches!(value, Value::Null),
        Function::IsNotNull => !matches!(value, Value::Null),
        _ => unreachable!("null-test evaluator called with non-null-test function"),
    }))
}

/// Evaluate one builder-owned preview expression against one already-loaded
/// source field value.
///
/// NOTE: this is a builder-side utility for local preview/application helpers.
/// It is not used by production execution paths, which stay on compiled
/// projection forms.
pub(in crate::db) fn eval_builder_expr_for_value_preview(
    expr: &Expr,
    field_name: &str,
    value: &Value,
) -> Result<Value, QueryError> {
    match expr {
        Expr::Field(field) => {
            if field.as_str() != field_name {
                return Err(QueryError::invariant(format!(
                    "value projection expected field '{field_name}' but found '{}'",
                    field.as_str()
                )));
            }

            Ok(value.clone())
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::FunctionCall { function, args } => {
            let evaluated_args = args
                .iter()
                .map(|arg| eval_builder_expr_for_value_preview(arg, field_name, value))
                .collect::<Result<Vec<_>, _>>()?;

            eval_projection_function_call(*function, evaluated_args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                let condition =
                    eval_builder_expr_for_value_preview(arm.condition(), field_name, value)?;
                if crate::db::executor::projection::eval::collapse_true_only_boolean_admission(
                    condition,
                    |found| {
                        QueryError::unsupported_query(format!(
                            "CASE condition did not evaluate to bool: {found:?}",
                        ))
                    },
                )? {
                    return eval_builder_expr_for_value_preview(arm.result(), field_name, value);
                }
            }

            eval_builder_expr_for_value_preview(else_expr.as_ref(), field_name, value)
        }
        Expr::Aggregate(_) => Err(QueryError::invariant(
            "value projection expressions cannot evaluate aggregate leaves",
        )),
        Expr::Binary { op, left, right } => {
            let left = eval_builder_expr_for_value_preview(left.as_ref(), field_name, value)?;
            let right = eval_builder_expr_for_value_preview(right.as_ref(), field_name, value)?;

            crate::db::executor::projection::eval::eval_binary_expr(*op, &left, &right)
                .map_err(|err| QueryError::unsupported_query(err.to_string()))
        }
        Expr::Unary { op, expr } => {
            let value = eval_builder_expr_for_value_preview(expr.as_ref(), field_name, value)?;

            crate::db::executor::projection::eval::eval_unary_expr(*op, &value)
                .map_err(|err| QueryError::unsupported_query(err.to_string()))
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            eval_builder_expr_for_value_preview(expr.as_ref(), field_name, value)
        }
    }
}

fn required_function_arg<'a>(
    function: Function,
    args: &'a [Value],
    index: usize,
    label: &str,
) -> Result<&'a Value, QueryError> {
    args.get(index).ok_or_else(|| {
        QueryError::invariant(format!(
            "{} projection item was missing its {label} argument",
            projection_function_name(function),
        ))
    })
}

fn eval_unary_text_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;

    match input {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => match function {
            Function::Trim => Ok(Value::Text(text.trim().to_string())),
            Function::Ltrim => Ok(Value::Text(text.trim_start().to_string())),
            Function::Rtrim => Ok(Value::Text(text.trim_end().to_string())),
            Function::Lower => Ok(Value::Text(text.to_lowercase())),
            Function::Upper => Ok(Value::Text(text.to_uppercase())),
            Function::Length => Ok(Value::Uint(
                u64::try_from(text.chars().count()).unwrap_or(u64::MAX),
            )),
            _ => unreachable!("unary text-function dispatch drifted"),
        },
        other => Err(text_input_error(function, other)),
    }
}

fn eval_unary_numeric_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;

    match input {
        Value::Null => Ok(Value::Null),
        value => {
            let Some(decimal) = value.to_numeric_decimal() else {
                return Err(QueryError::unsupported_query(format!(
                    "{}(...) requires numeric input, found {value:?}",
                    projection_function_name(function),
                )));
            };

            Ok(Value::Decimal(match function {
                Function::Abs => decimal.abs(),
                Function::Ceil | Function::Ceiling => decimal.ceil_dp0(),
                Function::Floor => decimal.floor_dp0(),
                _ => unreachable!("unary numeric-function dispatch drifted"),
            }))
        }
    }
}

fn eval_coalesce_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    if args.len() < 2 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected at least 2 arguments but received {}",
            projection_function_name(function),
            args.len(),
        )));
    }

    Ok(args
        .iter()
        .find(|value| !matches!(value, Value::Null))
        .cloned()
        .unwrap_or(Value::Null))
}

fn eval_nullif_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    let left = required_function_arg(function, args, 0, "left")?;
    let right = required_function_arg(function, args, 1, "right")?;

    if args.len() != 2 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected 2 arguments but received {}",
            projection_function_name(function),
            args.len(),
        )));
    }

    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(left.clone());
    }

    let equals = crate::db::executor::projection::eval::eval_binary_expr(BinaryOp::Eq, left, right)
        .map_err(|err| QueryError::unsupported_query(err.to_string()))?;

    Ok(if matches!(equals, Value::Bool(true)) {
        Value::Null
    } else {
        left.clone()
    })
}

fn eval_left_right_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let length = integer_literal_arg(function, args, 1, "length")?;

    match (input, length) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(length)) => Ok(Value::Text(match function {
            Function::Left => left_chars(text.as_str(), length),
            Function::Right => right_chars(text.as_str(), length),
            _ => unreachable!("left/right dispatch drifted"),
        })),
        (other, _) => Err(text_input_error(function, other)),
    }
}

fn eval_text_predicate_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let literal = text_literal_arg(function, args, 1, "literal")?;

    match (input, literal) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(needle)) => Ok(Value::Bool(match function {
            Function::StartsWith => text.starts_with(needle),
            Function::EndsWith => text.ends_with(needle),
            Function::Contains => text.contains(needle),
            _ => unreachable!("text predicate dispatch drifted"),
        })),
        (other, _) => Err(text_input_error(function, other)),
    }
}

fn eval_position_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    let needle = text_literal_arg(function, args, 0, "literal")?;
    let input = required_function_arg(function, args, 1, "input")?;

    match (needle, input) {
        (_, Value::Null) | (None, _) => Ok(Value::Null),
        (Some(needle), Value::Text(text)) => {
            Ok(Value::Uint(text_position_1_based(text.as_str(), needle)))
        }
        (_, other) => Err(text_input_error(function, other)),
    }
}

fn eval_replace_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let from = text_literal_arg(function, args, 1, "from")?;
    let to = text_literal_arg(function, args, 2, "to")?;

    match (input, from, to) {
        (Value::Null, _, _) | (_, None, _) | (_, _, None) => Ok(Value::Null),
        (Value::Text(text), Some(from), Some(to)) => Ok(Value::Text(text.replace(from, to))),
        (other, _, _) => Err(text_input_error(function, other)),
    }
}

fn eval_substring_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let start = integer_literal_arg(function, args, 1, "start")?;
    let length = optional_integer_literal_arg(function, args, 2, "length")?;

    match (input, start) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(start)) => {
            Ok(Value::Text(substring_1_based(text.as_str(), start, length)))
        }
        (other, _) => Err(text_input_error(function, other)),
    }
}

fn eval_round_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let scale = integer_literal_arg(function, args, 1, "scale")?;

    match (input, scale) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (value, Some(scale)) => {
            let Some(scale) = u32::try_from(scale).ok() else {
                return Err(QueryError::unsupported_query(format!(
                    "ROUND(...) requires non-negative integer scale, found {scale}",
                )));
            };
            let Some(decimal) = value.to_numeric_decimal() else {
                return Err(QueryError::unsupported_query(format!(
                    "ROUND(...) requires numeric input, found {value:?}",
                )));
            };

            Ok(Value::Decimal(decimal.round_dp(scale)))
        }
    }
}

fn text_input_error(function: Function, other: &Value) -> QueryError {
    QueryError::unsupported_query(format!(
        "{}(...) requires text input, found {other:?}",
        projection_function_name(function),
    ))
}

fn text_literal_arg<'a>(
    function: Function,
    args: &'a [Value],
    index: usize,
    label: &str,
) -> Result<Option<&'a str>, QueryError> {
    match required_function_arg(function, args, index, label)? {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.as_str())),
        other => Err(QueryError::unsupported_query(format!(
            "{}(...) requires text or NULL {label}, found {other:?}",
            projection_function_name(function),
        ))),
    }
}

fn integer_literal_arg(
    function: Function,
    args: &[Value],
    index: usize,
    label: &str,
) -> Result<Option<i64>, QueryError> {
    match required_function_arg(function, args, index, label)? {
        Value::Null => Ok(None),
        Value::Int(value) => Ok(Some(*value)),
        Value::Uint(value) => Ok(Some(i64::try_from(*value).unwrap_or(i64::MAX))),
        other => Err(QueryError::unsupported_query(format!(
            "{}(...) requires integer or NULL {label}, found {other:?}",
            projection_function_name(function),
        ))),
    }
}

fn optional_integer_literal_arg(
    function: Function,
    args: &[Value],
    index: usize,
    label: &str,
) -> Result<Option<i64>, QueryError> {
    if index >= args.len() {
        return Ok(None);
    }

    integer_literal_arg(function, args, index, label)
}

fn text_position_1_based(haystack: &str, needle: &str) -> u64 {
    let Some(byte_index) = haystack.find(needle) else {
        return 0;
    };
    let char_offset = haystack[..byte_index].chars().count();

    u64::try_from(char_offset)
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

fn left_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    text.chars()
        .take(usize::try_from(count).unwrap_or(usize::MAX))
        .collect()
}

fn right_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    let count = usize::try_from(count).unwrap_or(usize::MAX);
    let total = text.chars().count();
    let skip = total.saturating_sub(count);

    text.chars().skip(skip).collect()
}

fn substring_1_based(text: &str, start: i64, len: Option<i64>) -> String {
    if start <= 0 {
        return String::new();
    }
    if matches!(len, Some(length) if length <= 0) {
        return String::new();
    }

    let start_index = usize::try_from(start.saturating_sub(1)).unwrap_or(usize::MAX);
    let chars = text.chars().skip(start_index);

    match len {
        Some(length) => chars
            .take(usize::try_from(length).unwrap_or(usize::MAX))
            .collect(),
        None => chars.collect(),
    }
}
