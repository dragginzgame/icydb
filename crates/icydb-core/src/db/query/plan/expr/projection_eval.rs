//! Module: query::plan::expr::projection_eval
//! Responsibility: neutral scalar projection expression evaluation helpers
//! shared by query builders and executor projection runtime.
//! Does not own: row materialization, grouped aggregate folds, or executor route
//! selection.
//! Boundary: evaluates already-bound scalar expression arguments and builder
//! preview expressions without importing executor modules.

use crate::{
    db::{
        QueryError,
        numeric::{
            NumericArithmeticOp, NumericEvalError, apply_numeric_arithmetic_checked,
            coerce_numeric_decimal,
        },
        predicate::{CoercionId, CoercionSpec, compare_eq, compare_order},
        query::plan::expr::{
            BinaryOp, Expr, Function, ScalarEvalFunctionShape, UnaryOp,
            collapse_true_only_boolean_admission,
        },
    },
    value::Value,
};
use std::cmp::Ordering;

///
/// ProjectionFunctionEvalError
///
/// ProjectionFunctionEvalError keeps checked numeric failures distinct from
/// ordinary function-shape failures so executor projection paths can preserve
/// numeric overflow as a query execution error instead of reclassifying it as
/// an invalid logical plan.
///

pub(in crate::db) enum ProjectionFunctionEvalError {
    Query(QueryError),
    Numeric(NumericEvalError),
}

impl ProjectionFunctionEvalError {
    /// Convert this function-evaluation failure into the query-facing error
    /// taxonomy used by builder preview paths.
    pub(in crate::db) fn into_query_error(self) -> QueryError {
        match self {
            Self::Query(err) => err,
            Self::Numeric(err) => QueryError::from_numeric_eval_error(err),
        }
    }
}

impl From<QueryError> for ProjectionFunctionEvalError {
    fn from(err: QueryError) -> Self {
        Self::Query(err)
    }
}

impl From<NumericEvalError> for ProjectionFunctionEvalError {
    fn from(err: NumericEvalError) -> Self {
        Self::Numeric(err)
    }
}

/// Evaluate one bounded projection-function call over already-evaluated
/// argument values.
pub(in crate::db) fn eval_projection_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    eval_projection_function_call_checked(function, args)
        .map_err(ProjectionFunctionEvalError::into_query_error)
}

/// Evaluate one bounded projection-function call while preserving checked
/// numeric failures for executor projection paths.
pub(in crate::db) fn eval_projection_function_call_checked(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    match function.scalar_eval_shape() {
        ScalarEvalFunctionShape::NullTest => eval_null_test_function_call(function, args),
        ScalarEvalFunctionShape::NonExecutableProjection => Err(QueryError::invariant(format!(
            "projection function '{}' is not executable in scalar projection evaluation",
            function.projection_eval_name(),
        ))
        .into()),
        ScalarEvalFunctionShape::UnaryText => eval_unary_text_function_call(function, args),
        ScalarEvalFunctionShape::DynamicCoalesce => eval_coalesce_function_call(function, args),
        ScalarEvalFunctionShape::DynamicNullIf => eval_nullif_function_call(function, args),
        ScalarEvalFunctionShape::UnaryNumeric => eval_unary_numeric_function_call(function, args),
        ScalarEvalFunctionShape::BinaryNumeric => eval_binary_numeric_function_call(function, args),
        ScalarEvalFunctionShape::LeftRightText => {
            eval_left_right_text_function_call(function, args)
        }
        ScalarEvalFunctionShape::TextPredicate => eval_text_predicate_function_call(function, args),
        ScalarEvalFunctionShape::PositionText => eval_position_text_function_call(function, args),
        ScalarEvalFunctionShape::ReplaceText => eval_replace_text_function_call(function, args),
        ScalarEvalFunctionShape::SubstringText => eval_substring_text_function_call(function, args),
        ScalarEvalFunctionShape::NumericScale => eval_numeric_scale_function_call(function, args),
    }
}

/// Evaluate one builder-owned preview expression against one already-loaded
/// source field value.
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
        Expr::FieldPath(_) => Err(QueryError::unsupported_query(
            "nested field-path projection preview is not supported yet",
        )),
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

            eval_preview_binary_expr(*op, &left, &right)
        }
        Expr::Unary { op, expr } => {
            let value = eval_builder_expr_for_value_preview(expr.as_ref(), field_name, value)?;

            eval_preview_unary_expr(*op, &value)
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
) -> Result<&'a Value, ProjectionFunctionEvalError> {
    args.get(index).ok_or_else(|| {
        QueryError::invariant(format!(
            "{} projection item was missing its {label} argument",
            function.projection_eval_name(),
        ))
        .into()
    })
}

fn eval_null_test_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let value = required_function_arg(function, args, 0, "value")?;

    if args.len() != 1 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected 1 argument but received {}",
            function.projection_eval_name(),
            args.len(),
        ))
        .into());
    }

    Ok(function
        .boolean_null_test_kind()
        .expect("null-test runtime dispatch must keep one null-test kind")
        .eval_value(value))
}

fn eval_unary_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(function, args, 0, "input")?;

    match input {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => Ok(function
            .unary_text_function_kind()
            .expect("unary-text runtime dispatch must keep one unary-text kind")
            .eval_text(text.as_str())),
        other => Err(text_input_error(function, other).into()),
    }
}

fn eval_unary_numeric_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(function, args, 0, "input")?;

    match input {
        Value::Null => Ok(Value::Null),
        value => {
            let Some(decimal) = coerce_numeric_decimal(value) else {
                return Err(QueryError::unsupported_query(format!(
                    "{}(...) requires numeric input, found {value:?}",
                    function.projection_eval_name(),
                ))
                .into());
            };

            Ok(function
                .unary_numeric_function_kind()
                .expect("unary-numeric runtime dispatch must keep one unary-numeric kind")
                .eval_decimal(decimal)
                .map_err(ProjectionFunctionEvalError::from)?)
        }
    }
}

fn eval_binary_numeric_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let left = required_function_arg(function, args, 0, "left")?;
    let right = required_function_arg(function, args, 1, "right")?;

    if args.len() != 2 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected 2 arguments but received {}",
            function.projection_eval_name(),
            args.len(),
        ))
        .into());
    }

    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (left, right) => {
            let Some(left) = coerce_numeric_decimal(left) else {
                return Err(QueryError::unsupported_query(format!(
                    "{}(...) requires numeric left input, found {left:?}",
                    function.projection_eval_name(),
                ))
                .into());
            };
            let Some(right) = coerce_numeric_decimal(right) else {
                return Err(QueryError::unsupported_query(format!(
                    "{}(...) requires numeric right input, found {right:?}",
                    function.projection_eval_name(),
                ))
                .into());
            };
            let value = function
                .binary_numeric_function_kind()
                .expect("binary-numeric runtime dispatch must keep one binary-numeric kind")
                .eval_decimal(left, right)
                .map_err(ProjectionFunctionEvalError::from)?;

            Ok(value)
        }
    }
}

fn eval_coalesce_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    if args.len() < 2 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected at least 2 arguments but received {}",
            function.projection_eval_name(),
            args.len(),
        ))
        .into());
    }

    Ok(function.eval_coalesce_values(args))
}

fn eval_nullif_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let left = required_function_arg(function, args, 0, "left")?;
    let right = required_function_arg(function, args, 1, "right")?;

    if args.len() != 2 {
        return Err(QueryError::invariant(format!(
            "projection function '{}' expected 2 arguments but received {}",
            function.projection_eval_name(),
            args.len(),
        ))
        .into());
    }

    let equals = eval_preview_binary_expr(BinaryOp::Eq, left, right)?;

    Ok(function.eval_nullif_values(left, right, matches!(equals, Value::Bool(true))))
}

fn eval_left_right_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let length = integer_literal_arg(function, args, 1, "length")?;

    match (input, length) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(length)) => Ok(function
            .left_right_text_function_kind()
            .expect("left/right runtime dispatch must keep one left/right kind")
            .eval_text(text.as_str(), length)),
        (other, _) => Err(text_input_error(function, other).into()),
    }
}

fn eval_text_predicate_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let literal = text_literal_arg(function, args, 1, "literal")?;

    match (input, literal) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(needle)) => Ok(function
            .boolean_text_predicate_kind()
            .expect("text-predicate runtime dispatch must keep one text-predicate kind")
            .eval_text(text, needle)),
        (other, _) => Err(text_input_error(function, other).into()),
    }
}

fn eval_position_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let needle = text_literal_arg(function, args, 0, "literal")?;
    let input = required_function_arg(function, args, 1, "input")?;

    match (needle, input) {
        (_, Value::Null) | (None, _) => Ok(Value::Null),
        (Some(needle), Value::Text(text)) => Ok(function.eval_position_text(text.as_str(), needle)),
        (_, other) => Err(text_input_error(function, other).into()),
    }
}

fn eval_replace_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let from = text_literal_arg(function, args, 1, "from")?;
    let to = text_literal_arg(function, args, 2, "to")?;

    match (input, from, to) {
        (Value::Null, _, _) | (_, None, _) | (_, _, None) => Ok(Value::Null),
        (Value::Text(text), Some(from), Some(to)) => {
            Ok(function.eval_replace_text(text.as_str(), from, to))
        }
        (other, _, _) => Err(text_input_error(function, other).into()),
    }
}

fn eval_substring_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let start = integer_literal_arg(function, args, 1, "start")?;
    let length = optional_integer_literal_arg(function, args, 2, "length")?;

    match (input, start) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(start)) => {
            Ok(function.eval_substring_text(text.as_str(), start, length))
        }
        (other, _) => Err(text_input_error(function, other).into()),
    }
}

fn eval_numeric_scale_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(function, args, 0, "input")?;
    let scale = integer_literal_arg(function, args, 1, "scale")?;

    match (input, scale) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (value, Some(scale)) => {
            let Some(scale) = u32::try_from(scale).ok() else {
                return Err(QueryError::unsupported_query(format!(
                    "{}(...) requires non-negative integer scale, found {scale}",
                    function.canonical_label(),
                ))
                .into());
            };
            let Some(value) = function.eval_numeric_scale(value, scale) else {
                return Err(QueryError::unsupported_query(format!(
                    "{}(...) requires numeric input, found {value:?}",
                    function.canonical_label(),
                ))
                .into());
            };

            Ok(value)
        }
    }
}

fn eval_preview_unary_expr(op: UnaryOp, value: &Value) -> Result<Value, QueryError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Not => {
            let Value::Bool(v) = value else {
                return Err(QueryError::unsupported_query(format!(
                    "projection unary operator '{}' is incompatible with operand value {value:?}",
                    unary_op_name(op),
                )));
            };

            Ok(Value::Bool(!*v))
        }
    }
}

fn eval_preview_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, QueryError> {
    match op {
        BinaryOp::Or | BinaryOp::And => eval_preview_boolean_binary_expr(op, left, right),
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => eval_preview_compare_binary_expr(op, left, right),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }

            eval_preview_numeric_binary_expr(op, left, right)
        }
    }
}

fn eval_preview_boolean_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, QueryError> {
    match op {
        BinaryOp::And => match (left, right) {
            (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(true) | Value::Null, Value::Null) | (Value::Null, Value::Bool(true)) => {
                Ok(Value::Null)
            }
            _ => Err(invalid_binary_operands(op, left, right)),
        },
        BinaryOp::Or => match (left, right) {
            (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(false) | Value::Null, Value::Null) | (Value::Null, Value::Bool(false)) => {
                Ok(Value::Null)
            }
            _ => Err(invalid_binary_operands(op, left, right)),
        },
        _ => unreachable!("boolean evaluator called with non-boolean operator"),
    }
}

fn eval_preview_numeric_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, QueryError> {
    let Some(result) = apply_numeric_arithmetic_checked(numeric_arithmetic_op(op), left, right)
        .map_err(QueryError::from_numeric_eval_error)?
    else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(Value::Decimal(result))
}

fn eval_preview_compare_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, QueryError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let numeric_widen_enabled =
        left.supports_numeric_coercion() || right.supports_numeric_coercion();
    let coercion = if numeric_widen_enabled {
        CoercionSpec::new(CoercionId::NumericWiden)
    } else {
        CoercionSpec::new(CoercionId::Strict)
    };
    let value = match op {
        BinaryOp::Eq => {
            if let Some(are_equal) = compare_eq(left, right, &coercion) {
                are_equal
            } else if !numeric_widen_enabled {
                left == right
            } else {
                return Err(invalid_binary_operands(op, left, right));
            }
        }
        BinaryOp::Ne => {
            if let Some(are_equal) = compare_eq(left, right, &coercion) {
                !are_equal
            } else if !numeric_widen_enabled {
                left != right
            } else {
                return Err(invalid_binary_operands(op, left, right));
            }
        }
        BinaryOp::Lt => eval_order_comparison(op, left, right, &coercion, Ordering::is_lt)?,
        BinaryOp::Lte => eval_order_comparison(op, left, right, &coercion, Ordering::is_le)?,
        BinaryOp::Gt => eval_order_comparison(op, left, right, &coercion, Ordering::is_gt)?,
        BinaryOp::Gte => eval_order_comparison(op, left, right, &coercion, Ordering::is_ge)?,
        _ => unreachable!("comparison evaluator called with non-comparison operator"),
    };

    Ok(Value::Bool(value))
}

fn eval_order_comparison(
    op: BinaryOp,
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Result<bool, QueryError> {
    let Some(ordering) = compare_order(left, right, coercion) else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(predicate(ordering))
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
    match required_function_arg(function, args, index, label)
        .map_err(ProjectionFunctionEvalError::into_query_error)?
    {
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
    match required_function_arg(function, args, index, label)
        .map_err(ProjectionFunctionEvalError::into_query_error)?
    {
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

fn invalid_binary_operands(op: BinaryOp, left: &Value, right: &Value) -> QueryError {
    QueryError::unsupported_query(format!(
        "projection binary operator '{}' is incompatible with operand values ({left:?}, {right:?})",
        op.canonical_label(),
    ))
}

const fn numeric_arithmetic_op(op: BinaryOp) -> NumericArithmeticOp {
    match op {
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::Add => NumericArithmeticOp::Add,
        BinaryOp::Sub => NumericArithmeticOp::Sub,
        BinaryOp::Mul => NumericArithmeticOp::Mul,
        BinaryOp::Div => NumericArithmeticOp::Div,
    }
}

const fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
    }
}
