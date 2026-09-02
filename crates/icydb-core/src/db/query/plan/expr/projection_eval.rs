//! Module: query::plan::expr::projection_eval
//! Responsibility: neutral scalar projection expression evaluation helpers
//! shared by query builders and executor projection runtime.
//! Does not own: row materialization, grouped aggregate folds, or executor route
//! selection, predicate compilation, or boolean canonicalization.
//! Boundary: evaluates already-bound scalar expression arguments and builder
//! preview expressions without importing executor modules or predicate runtime
//! semantics. Boolean-context truth admission is delegated to the shared
//! `truth_value` policy after this module has materialized a condition value.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use super::scalar::ScalarProjectionField;
use crate::{
    db::{
        QueryError,
        numeric::{
            NumericEvalError, apply_value_arithmetic_checked, coerce_numeric_decimal,
            compare_numeric_eq, compare_numeric_or_strict_order,
        },
        query::plan::expr::{
            BinaryOp, CompiledExpr, CompiledExprValueReader, Expr, Function, ProjectionEvalError,
            ScalarEvalFunctionShape, ScalarProjectionCaseArm, ScalarProjectionExpr,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::QueryProjectionCode;
use std::{borrow::Cow, cmp::Ordering};

const PREVIEW_VALUE_SLOT: usize = 0;

struct PreviewValueReader<'value> {
    value: &'value Value,
}

impl CompiledExprValueReader for PreviewValueReader<'_> {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        (slot == PREVIEW_VALUE_SLOT).then_some(Cow::Borrowed(self.value))
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }
}

///
/// ProjectionFunctionEvalError
///
/// ProjectionFunctionEvalError keeps checked numeric failures distinct from
/// ordinary function-shape failures so executor projection paths can preserve
/// numeric overflow as a query execution error instead of reclassifying it as
/// an invalid logical plan.
///

pub(in crate::db) enum ProjectionFunctionEvalError {
    InvalidCall,
    InvalidProjection(QueryProjectionCode),
    Numeric(NumericEvalError),
}

impl ProjectionFunctionEvalError {
    /// Convert this function-evaluation failure into the query-facing error
    /// taxonomy used by builder preview paths.
    #[cfg(test)]
    pub(in crate::db) fn into_query_error(self) -> QueryError {
        match self {
            Self::InvalidCall => QueryError::invariant(),
            Self::InvalidProjection(reason) => QueryError::unsupported_projection(reason),
            Self::Numeric(err) => QueryError::from_numeric_eval_error(err),
        }
    }
}

impl From<NumericEvalError> for ProjectionFunctionEvalError {
    fn from(err: NumericEvalError) -> Self {
        Self::Numeric(err)
    }
}

const fn projection_unsupported(reason: QueryProjectionCode) -> ProjectionFunctionEvalError {
    ProjectionFunctionEvalError::InvalidProjection(reason)
}

/// Evaluate one bounded projection-function call over already-evaluated
/// argument values.
#[cfg(test)]
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
        ScalarEvalFunctionShape::NonExecutableProjection => {
            Err(ProjectionFunctionEvalError::InvalidCall)
        }
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
        ScalarEvalFunctionShape::OctetLength => eval_octet_length_function_call(args),
        ScalarEvalFunctionShape::Membership => eval_membership_function_call(args),
    }
}

/// Evaluate one builder-owned preview expression against one already-loaded
/// source field value.
pub(in crate::db) fn eval_builder_expr_for_value_preview(
    expr: &Expr,
    field_name: &str,
    value: &Value,
) -> Result<Value, QueryError> {
    let preview_expr = compile_builder_preview_expr(expr, field_name)?;
    let compiled = CompiledExpr::compile(&preview_expr);
    let reader = PreviewValueReader { value };

    compiled
        .evaluate(&reader)
        .map(Cow::into_owned)
        .map_err(preview_eval_error_into_query_error)
}

fn compile_builder_preview_expr(
    expr: &Expr,
    field_name: &str,
) -> Result<ScalarProjectionExpr, QueryError> {
    match expr {
        Expr::Field(field) => {
            if field.as_str() != field_name {
                return Err(QueryError::invariant());
            }

            Ok(ScalarProjectionExpr::Field(ScalarProjectionField::new(
                field.as_str().to_string(),
                PREVIEW_VALUE_SLOT,
            )))
        }
        Expr::FieldPath(_) => Err(QueryError::unsupported_projection(
            QueryProjectionCode::NestedFieldPathPreview,
        )),
        Expr::Literal(value) => Ok(ScalarProjectionExpr::Literal(value.clone())),
        Expr::FunctionCall { function, args } => {
            let args = args
                .iter()
                .map(|arg| compile_builder_preview_expr(arg, field_name))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ScalarProjectionExpr::FunctionCall {
                function: *function,
                args,
            })
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let when_then_arms = when_then_arms
                .iter()
                .map(|arm| {
                    Ok(ScalarProjectionCaseArm::new(
                        compile_builder_preview_expr(arm.condition(), field_name)?,
                        compile_builder_preview_expr(arm.result(), field_name)?,
                    ))
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            let else_expr = Box::new(compile_builder_preview_expr(else_expr, field_name)?);

            Ok(ScalarProjectionExpr::Case {
                when_then_arms,
                else_expr,
            })
        }
        Expr::Aggregate(_) => Err(QueryError::invariant()),
        Expr::Binary { op, left, right } => {
            let left = compile_builder_preview_expr(left, field_name)?;
            let right = compile_builder_preview_expr(right, field_name)?;

            Ok(ScalarProjectionExpr::Binary {
                op: *op,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        Expr::Unary { op, expr } => {
            let expr = compile_builder_preview_expr(expr, field_name)?;

            Ok(ScalarProjectionExpr::Unary {
                op: *op,
                expr: Box::new(expr),
            })
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => compile_builder_preview_expr(expr, field_name),
    }
}

fn preview_eval_error_into_query_error(err: ProjectionEvalError) -> QueryError {
    match err {
        ProjectionEvalError::Numeric(err) => QueryError::from_numeric_eval_error(err),
        ProjectionEvalError::InvalidProjection { reason } => {
            QueryError::unsupported_projection(reason)
        }
        ProjectionEvalError::InvalidUnaryOperand { .. } => {
            QueryError::unsupported_projection(QueryProjectionCode::UnaryOperandIncompatible)
        }
        ProjectionEvalError::InvalidCaseCondition { .. } => {
            QueryError::unsupported_projection(QueryProjectionCode::CaseConditionBooleanRequired)
        }
        ProjectionEvalError::InvalidBinaryOperands { .. } => {
            QueryError::unsupported_projection(QueryProjectionCode::BinaryOperandsIncompatible)
        }
        ProjectionEvalError::UnknownField { .. }
        | ProjectionEvalError::MissingFieldValue { .. }
        | ProjectionEvalError::MissingFieldPathValue { .. }
        | ProjectionEvalError::FieldPathEvaluationFailed { .. }
        | ProjectionEvalError::ReaderFailed { .. }
        | ProjectionEvalError::UnknownGroupedAggregateExpression { .. }
        | ProjectionEvalError::MissingGroupedAggregateValue { .. }
        | ProjectionEvalError::InvalidFunctionCall { .. }
        | ProjectionEvalError::InvalidGroupedHavingResult { .. } => QueryError::invariant(),
    }
}

fn required_function_arg(
    args: &[Value],
    index: usize,
) -> Result<&Value, ProjectionFunctionEvalError> {
    args.get(index)
        .ok_or(ProjectionFunctionEvalError::InvalidCall)
}

fn eval_null_test_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let value = required_function_arg(args, 0)?;

    if args.len() != 1 {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    }

    let kind = function
        .boolean_null_test_kind()
        .ok_or(ProjectionFunctionEvalError::InvalidCall)?;

    Ok(kind.eval_value(value))
}

fn eval_unary_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;

    match input {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => {
            let kind = function
                .unary_text_function_kind()
                .ok_or(ProjectionFunctionEvalError::InvalidCall)?;

            Ok(kind.eval_text(text.as_str()))
        }
        _ => Err(text_input_error()),
    }
}

fn eval_unary_numeric_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;

    match input {
        Value::Null => Ok(Value::Null),
        value => {
            let Some(decimal) = coerce_numeric_decimal(value) else {
                return Err(projection_unsupported(
                    QueryProjectionCode::NumericInputRequired,
                ));
            };

            let kind = function
                .unary_numeric_function_kind()
                .ok_or(ProjectionFunctionEvalError::InvalidCall)?;

            Ok(kind
                .eval_decimal(decimal)
                .map_err(ProjectionFunctionEvalError::from)?)
        }
    }
}

fn eval_octet_length_function_call(args: &[Value]) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;

    if args.len() != 1 {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    }

    match input {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => Ok(Value::Nat64(u64::try_from(text.len()).unwrap_or(u64::MAX))),
        Value::Blob(bytes) => Ok(Value::Nat64(u64::try_from(bytes.len()).unwrap_or(u64::MAX))),
        _ => Err(projection_unsupported(
            QueryProjectionCode::TextOrBlobInputRequired,
        )),
    }
}

fn eval_binary_numeric_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let left = required_function_arg(args, 0)?;
    let right = required_function_arg(args, 1)?;

    if args.len() != 2 {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    }

    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (left, right) => {
            let kind = function
                .binary_numeric_function_kind()
                .ok_or(ProjectionFunctionEvalError::InvalidCall)?;
            let Some(value) = kind
                .eval_values(left, right)
                .map_err(ProjectionFunctionEvalError::from)?
            else {
                return Err(projection_unsupported(
                    QueryProjectionCode::NumericInputRequired,
                ));
            };

            Ok(value)
        }
    }
}

fn eval_coalesce_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    if args.len() < 2 {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    }

    Ok(function.eval_coalesce_values(args))
}

fn eval_nullif_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let left = required_function_arg(args, 0)?;
    let right = required_function_arg(args, 1)?;

    if args.len() != 2 {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    }

    let equals = eval_preview_binary_expr(BinaryOp::Eq, left, right)?;

    Ok(function.eval_nullif_values(left, right, matches!(equals, Value::Bool(true))))
}

fn eval_left_right_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;
    let length = integer_literal_arg(args, 1)?;

    match (input, length) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(length)) => {
            let kind = function
                .left_right_text_function_kind()
                .ok_or(ProjectionFunctionEvalError::InvalidCall)?;

            Ok(kind.eval_text(text.as_str(), length))
        }
        _ => Err(text_input_error()),
    }
}

fn eval_text_predicate_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;
    let literal = text_literal_arg(args, 1)?;

    match (input, literal) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(needle)) => {
            let kind = function
                .boolean_text_predicate_kind()
                .ok_or(ProjectionFunctionEvalError::InvalidCall)?;

            Ok(kind.eval_text(text, needle))
        }
        _ => Err(text_input_error()),
    }
}

fn eval_position_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let needle = text_literal_arg(args, 0)?;
    let input = required_function_arg(args, 1)?;

    match (needle, input) {
        (_, Value::Null) | (None, _) => Ok(Value::Null),
        (Some(needle), Value::Text(text)) => Ok(function.eval_position_text(text.as_str(), needle)),
        _ => Err(text_input_error()),
    }
}

fn eval_replace_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;
    let from = text_literal_arg(args, 1)?;
    let to = text_literal_arg(args, 2)?;

    match (input, from, to) {
        (Value::Null, _, _) | (_, None, _) | (_, _, None) => Ok(Value::Null),
        (Value::Text(text), Some(from), Some(to)) => {
            Ok(function.eval_replace_text(text.as_str(), from, to))
        }
        _ => Err(text_input_error()),
    }
}

fn eval_substring_text_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;
    let start = integer_literal_arg(args, 1)?;
    let length = optional_integer_literal_arg(args, 2)?;

    match (input, start) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (Value::Text(text), Some(start)) => {
            Ok(function.eval_substring_text(text.as_str(), start, length))
        }
        _ => Err(text_input_error()),
    }
}

fn eval_numeric_scale_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionFunctionEvalError> {
    let input = required_function_arg(args, 0)?;
    let scale = integer_literal_arg(args, 1)?;

    match (input, scale) {
        (Value::Null, _) | (_, None) => Ok(Value::Null),
        (value, Some(scale)) => {
            let Some(scale) = u32::try_from(scale).ok() else {
                return Err(projection_unsupported(
                    QueryProjectionCode::NumericScaleArguments,
                ));
            };
            let Some(value) = function.eval_numeric_scale(value, scale) else {
                return Err(projection_unsupported(
                    QueryProjectionCode::NumericInputRequired,
                ));
            };

            Ok(value)
        }
    }
}

fn eval_membership_function_call(args: &[Value]) -> Result<Value, ProjectionFunctionEvalError> {
    let target = required_function_arg(args, 0)?;
    let values = required_function_arg(args, 1)?;

    if args.len() != 2 {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    }
    if matches!(target, Value::Null) {
        return Ok(Value::Null);
    }

    let Value::List(values) = values else {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    };

    let mut saw_null = false;
    let mut matched = false;
    for value in values {
        if matches!(value, Value::Null) {
            saw_null = true;
            continue;
        }

        let comparison = eval_preview_compare_binary_expr(BinaryOp::Eq, target, value)?;
        if matches!(comparison, Value::Bool(true)) {
            matched = true;
        }
    }

    Ok(match (matched, saw_null) {
        (true, _) => Value::Bool(true),
        (false, true) => Value::Null,
        (false, false) => Value::Bool(false),
    })
}

fn eval_preview_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionFunctionEvalError> {
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

const fn eval_preview_boolean_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionFunctionEvalError> {
    match op {
        BinaryOp::And => match (left, right) {
            (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(true) | Value::Null, Value::Null) | (Value::Null, Value::Bool(true)) => {
                Ok(Value::Null)
            }
            _ => Err(invalid_binary_operands()),
        },
        BinaryOp::Or => match (left, right) {
            (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(false) | Value::Null, Value::Null) | (Value::Null, Value::Bool(false)) => {
                Ok(Value::Null)
            }
            _ => Err(invalid_binary_operands()),
        },
        _ => Err(ProjectionFunctionEvalError::InvalidCall),
    }
}

fn eval_preview_numeric_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionFunctionEvalError> {
    let Some(arithmetic_op) = op.numeric_arithmetic_op() else {
        return Err(ProjectionFunctionEvalError::InvalidCall);
    };
    let Some(result) = apply_value_arithmetic_checked(arithmetic_op, left, right)
        .map_err(ProjectionFunctionEvalError::from)?
    else {
        return Err(invalid_binary_operands());
    };

    Ok(result)
}

fn eval_preview_compare_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionFunctionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let numeric_widen_enabled =
        left.supports_numeric_coercion() || right.supports_numeric_coercion();
    let value = match op {
        BinaryOp::Eq => {
            if let Some(are_equal) = compare_numeric_eq(left, right) {
                are_equal
            } else if !numeric_widen_enabled {
                left == right
            } else {
                return Err(invalid_binary_operands());
            }
        }
        BinaryOp::Ne => {
            if let Some(are_equal) = compare_numeric_eq(left, right) {
                !are_equal
            } else if !numeric_widen_enabled {
                left != right
            } else {
                return Err(invalid_binary_operands());
            }
        }
        BinaryOp::Lt => eval_order_comparison(left, right, Ordering::is_lt)?,
        BinaryOp::Lte => eval_order_comparison(left, right, Ordering::is_le)?,
        BinaryOp::Gt => eval_order_comparison(left, right, Ordering::is_gt)?,
        BinaryOp::Gte => eval_order_comparison(left, right, Ordering::is_ge)?,
        _ => return Err(ProjectionFunctionEvalError::InvalidCall),
    };

    Ok(Value::Bool(value))
}

fn eval_order_comparison(
    left: &Value,
    right: &Value,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Result<bool, ProjectionFunctionEvalError> {
    let Some(ordering) = compare_numeric_or_strict_order(left, right) else {
        return Err(invalid_binary_operands());
    };

    Ok(predicate(ordering))
}

const fn text_input_error() -> ProjectionFunctionEvalError {
    projection_unsupported(QueryProjectionCode::TextInputRequired)
}

fn text_literal_arg(
    args: &[Value],
    index: usize,
) -> Result<Option<&str>, ProjectionFunctionEvalError> {
    match required_function_arg(args, index)? {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.as_str())),
        _ => Err(projection_unsupported(
            QueryProjectionCode::TextOrNullArgumentRequired,
        )),
    }
}

fn integer_literal_arg(
    args: &[Value],
    index: usize,
) -> Result<Option<i64>, ProjectionFunctionEvalError> {
    match required_function_arg(args, index)? {
        Value::Null => Ok(None),
        Value::Int64(value) => Ok(Some(*value)),
        Value::Nat64(value) => Ok(Some(i64::try_from(*value).unwrap_or(i64::MAX))),
        _ => Err(projection_unsupported(
            QueryProjectionCode::IntegerOrNullArgumentRequired,
        )),
    }
}

fn optional_integer_literal_arg(
    args: &[Value],
    index: usize,
) -> Result<Option<i64>, ProjectionFunctionEvalError> {
    if index >= args.len() {
        return Ok(None);
    }

    integer_literal_arg(args, index)
}

const fn invalid_binary_operands() -> ProjectionFunctionEvalError {
    projection_unsupported(QueryProjectionCode::BinaryOperandsIncompatible)
}
