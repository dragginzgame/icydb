//! Module: db::executor::projection::eval::text_function
//! Responsibility: bounded text-function evaluation for projection execution.
//! Does not own: SQL parsing, planner validation, or grouped-lowering policy.
//! Boundary: executor-owned runtime semantics for canonical `Expr::FunctionCall`
//! values admitted on the narrowed text-function slice.

use crate::{
    db::{
        QueryError,
        query::plan::expr::{Expr, Function},
    },
    value::Value,
};

pub(in crate::db) const fn projection_function_name(function: Function) -> &'static str {
    match function {
        Function::Trim => "trim",
        Function::Ltrim => "ltrim",
        Function::Rtrim => "rtrim",
        Function::Lower => "lower",
        Function::Upper => "upper",
        Function::Length => "length",
        Function::Left => "left",
        Function::Right => "right",
        Function::StartsWith => "starts_with",
        Function::EndsWith => "ends_with",
        Function::Contains => "contains",
        Function::Position => "position",
        Function::Replace => "replace",
        Function::Substring => "substring",
    }
}

/// Evaluate one bounded text-function call over already-evaluated argument values.
pub(in crate::db) fn eval_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    match function {
        Function::Trim
        | Function::Ltrim
        | Function::Rtrim
        | Function::Lower
        | Function::Upper
        | Function::Length => {
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
        Function::Left | Function::Right => {
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
        Function::StartsWith | Function::EndsWith | Function::Contains => {
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
        Function::Position => {
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
        Function::Replace => {
            let input = required_function_arg(function, args, 0, "input")?;
            let from = text_literal_arg(function, args, 1, "from")?;
            let to = text_literal_arg(function, args, 2, "to")?;

            match (input, from, to) {
                (Value::Null, _, _) | (_, None, _) | (_, _, None) => Ok(Value::Null),
                (Value::Text(text), Some(from), Some(to)) => {
                    Ok(Value::Text(text.replace(from, to)))
                }
                (other, _, _) => Err(text_input_error(function, other)),
            }
        }
        Function::Substring => {
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
    }
}

/// Evaluate one bounded text projection expression against one already-loaded
/// source field value.
pub(in crate::db) fn eval_text_projection_expr_with_value(
    expr: &Expr,
    field_name: &str,
    value: &Value,
) -> Result<Value, QueryError> {
    match expr {
        Expr::Field(field) => {
            if field.as_str() != field_name {
                return Err(QueryError::invariant(format!(
                    "text projection expected field '{field_name}' but found '{}'",
                    field.as_str()
                )));
            }

            Ok(value.clone())
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::FunctionCall { function, args } => {
            let evaluated_args = args
                .iter()
                .map(|arg| eval_text_projection_expr_with_value(arg, field_name, value))
                .collect::<Result<Vec<_>, _>>()?;

            eval_text_function_call(*function, evaluated_args.as_slice())
        }
        Expr::Aggregate(_) => Err(QueryError::invariant(
            "text projection expressions cannot evaluate aggregate leaves",
        )),
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            eval_text_projection_expr_with_value(expr.as_ref(), field_name, value)
        }
        #[cfg(test)]
        Expr::Unary { .. } | Expr::Binary { .. } => Err(QueryError::invariant(
            "text projection expressions cannot evaluate generic test-only operators",
        )),
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
