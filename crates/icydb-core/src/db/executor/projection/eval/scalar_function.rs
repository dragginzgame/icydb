//! Module: db::executor::projection::eval::scalar_function
//! Responsibility: bounded scalar-function evaluation for scalar projection
//! execution.
//! Does not own: SQL parsing, planner validation, or grouped-lowering policy.
//! Boundary: executor-owned runtime semantics for canonical `Expr::FunctionCall`
//! values admitted on the narrowed scalar projection slice.

use crate::{
    db::{
        QueryError,
        query::plan::expr::{
            BinaryOp, Expr, Function, ScalarEvalFunctionShape, collapse_true_only_boolean_admission,
        },
    },
    value::Value,
};

/// Evaluate one bounded projection-function call over already-evaluated
/// argument values.
pub(in crate::db) fn eval_projection_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    match function.scalar_eval_shape() {
        ScalarEvalFunctionShape::NullTest => eval_null_test_function_call(function, args),
        ScalarEvalFunctionShape::NonExecutableProjection => Err(QueryError::invariant(format!(
            "projection function '{}' is not executable in scalar projection evaluation",
            function.projection_eval_name(),
        ))),
        ScalarEvalFunctionShape::UnaryText => eval_unary_text_function_call(function, args),
        ScalarEvalFunctionShape::DynamicCoalesce => eval_coalesce_function_call(function, args),
        ScalarEvalFunctionShape::DynamicNullIf => eval_nullif_function_call(function, args),
        ScalarEvalFunctionShape::UnaryNumeric => eval_unary_numeric_function_call(function, args),
        ScalarEvalFunctionShape::LeftRightText => {
            eval_left_right_text_function_call(function, args)
        }
        ScalarEvalFunctionShape::TextPredicate => eval_text_predicate_function_call(function, args),
        ScalarEvalFunctionShape::PositionText => eval_position_text_function_call(function, args),
        ScalarEvalFunctionShape::ReplaceText => eval_replace_text_function_call(function, args),
        ScalarEvalFunctionShape::SubstringText => eval_substring_text_function_call(function, args),
        ScalarEvalFunctionShape::Round => eval_round_function_call(function, args),
    }
}

fn eval_null_test_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    let value = required_function_arg(function, args, 0, "value")?;

    if args.len() != 1 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected 1 argument but received {}",
            function.projection_eval_name(),
            args.len(),
        )));
    }

    Ok(function
        .boolean_null_test_kind()
        .expect("null-test runtime dispatch must keep one null-test kind")
        .eval_value(value))
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
                if collapse_true_only_boolean_admission(condition, |found| {
                    QueryError::unsupported_query(format!(
                        "CASE condition did not evaluate to bool: {found:?}",
                    ))
                })? {
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
            function.projection_eval_name(),
        ))
    })
}

fn eval_unary_text_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;

    match input {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => Ok(function
            .unary_text_function_kind()
            .expect("unary-text runtime dispatch must keep one unary-text kind")
            .eval_text(text.as_str())),
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
                    function.projection_eval_name(),
                )));
            };

            Ok(function
                .unary_numeric_function_kind()
                .expect("unary-numeric runtime dispatch must keep one unary-numeric kind")
                .eval_decimal(decimal))
        }
    }
}

fn eval_coalesce_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    if args.len() < 2 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected at least 2 arguments but received {}",
            function.projection_eval_name(),
            args.len(),
        )));
    }

    Ok(function.eval_coalesce_values(args))
}

fn eval_nullif_function_call(function: Function, args: &[Value]) -> Result<Value, QueryError> {
    let left = required_function_arg(function, args, 0, "left")?;
    let right = required_function_arg(function, args, 1, "right")?;

    if args.len() != 2 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected 2 arguments but received {}",
            function.projection_eval_name(),
            args.len(),
        )));
    }

    let equals = crate::db::executor::projection::eval::eval_binary_expr(BinaryOp::Eq, left, right)
        .map_err(|err| QueryError::unsupported_query(err.to_string()))?;

    Ok(function.eval_nullif_values(left, right, matches!(equals, Value::Bool(true))))
}

fn eval_left_right_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let length = integer_literal_arg(function, args, 1, "length")?;

    match (input, length) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(length)) => Ok(function
            .left_right_text_function_kind()
            .expect("left/right runtime dispatch must keep one left/right kind")
            .eval_text(text.as_str(), length)),
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
        (Value::Text(text), Some(needle)) => Ok(function
            .boolean_text_predicate_kind()
            .expect("text-predicate runtime dispatch must keep one text-predicate kind")
            .eval_text(text, needle)),
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
        (Some(needle), Value::Text(text)) => Ok(function.eval_position_text(text.as_str(), needle)),
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
        (Value::Text(text), Some(from), Some(to)) => {
            Ok(function.eval_replace_text(text.as_str(), from, to))
        }
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
            Ok(function.eval_substring_text(text.as_str(), start, length))
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
            let Some(value) = function.eval_round_numeric(value, scale) else {
                return Err(QueryError::unsupported_query(format!(
                    "ROUND(...) requires numeric input, found {value:?}",
                )));
            };

            Ok(value)
        }
    }
}

fn text_input_error(function: Function, other: &Value) -> QueryError {
    QueryError::unsupported_query(format!(
        "{}(...) requires text input, found {other:?}",
        function.projection_eval_name(),
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
            function.projection_eval_name(),
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
            function.projection_eval_name(),
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
