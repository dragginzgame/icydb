//! Module: query::plan::expr::compiled_expr::evaluate
//! Responsibility: value-level evaluation for compiled expression programs.
//! Does not own: planner expression lowering or executor row storage.
//! Boundary: evaluates already-compiled expression nodes through CompiledExprValueReader.

use crate::{
    db::{
        numeric::NumericEvalError,
        query::plan::expr::{
            BinaryOp, CompiledExpr, CompiledExprCaseArm, CompiledExprValueReader, Function,
            ProjectionEvalError, ProjectionFunctionEvalError, UnaryOp,
            admit_true_only_boolean_value, collapse_true_only_boolean_admission,
            compiled_expr::missing_field_value, eval_projection_function_call_checked,
        },
    },
    value::{
        Value,
        ops::{numeric as value_numeric, ordering as value_ordering},
    },
};
use std::borrow::Cow;

impl CompiledExpr {
    /// Evaluate one compiled expression against a value reader.
    #[expect(
        clippy::too_many_lines,
        reason = "explicit compiled opcode dispatch keeps grouped hot-loop behavior auditably direct"
    )]
    pub(in crate::db) fn evaluate<'row>(
        &'row self,
        reader: &'row dyn CompiledExprValueReader,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        match self {
            Self::Slot { slot, field } => Self::evaluate_slot(reader, *slot, field),
            Self::GroupKey { offset, field } => Self::evaluate_group_key(reader, *offset, field),
            Self::Aggregate { index } => Self::evaluate_aggregate(reader, *index),
            Self::Literal(value) => Ok(Cow::Borrowed(value)),
            Self::Add {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_arithmetic(
                reader,
                BinaryOp::Add,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Sub {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_arithmetic(
                reader,
                BinaryOp::Sub,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Mul {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_arithmetic(
                reader,
                BinaryOp::Mul,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Div {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_arithmetic(
                reader,
                BinaryOp::Div,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Eq {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                reader,
                BinaryOp::Eq,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Ne {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                reader,
                BinaryOp::Ne,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Lt {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                reader,
                BinaryOp::Lt,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Lte {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                reader,
                BinaryOp::Lte,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Gt {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                reader,
                BinaryOp::Gt,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Gte {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                reader,
                BinaryOp::Gte,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::BinarySlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left,
            } => Self::evaluate_slot_literal_binary(
                reader,
                *op,
                *slot,
                field,
                literal,
                *slot_on_left,
            ),
            Self::CaseSlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left,
                then_expr,
                else_expr,
            } => Self::evaluate_case_slot_literal(
                reader,
                *op,
                (*slot, field),
                literal,
                *slot_on_left,
                then_expr,
                else_expr,
            ),
            Self::CaseSlotBool {
                slot,
                field,
                then_expr,
                else_expr,
            } => Self::evaluate_case_slot_bool(reader, *slot, field, then_expr, else_expr),
            Self::FieldPath {
                root_slot,
                field,
                segments,
                segment_bytes,
            } => Self::evaluate_field_path(reader, *root_slot, field, segments, segment_bytes),
            Self::FunctionCall { function, args } => {
                Self::evaluate_function_call(reader, *function, args)
            }
            Self::Unary { op, expr } => {
                let value = expr.evaluate(reader)?;

                evaluate_unary_expr(*op, value.as_ref()).map(Cow::Owned)
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => Self::evaluate_case(reader, when_then_arms, else_expr),
            Self::Binary { op, left, right } => {
                let left = left.evaluate(reader)?;
                let right = right.evaluate(reader)?;

                evaluate_binary_expr(*op, left.as_ref(), right.as_ref()).map(Cow::Owned)
            }
        }
    }

    // Resolve one required slot through row-view storage without constructing
    // a caller closure or walking another expression node.
    fn evaluate_slot<'row>(
        reader: &'row dyn CompiledExprValueReader,
        slot: usize,
        field: &str,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        reader
            .read_slot_checked(slot)?
            .ok_or_else(|| missing_field_value(field, slot))
    }

    // Resolve one grouped-key leaf through the same reader contract used by
    // slot expressions. Missing keys keep the field label resolved during
    // grouped planning.
    fn evaluate_group_key<'row>(
        reader: &'row dyn CompiledExprValueReader,
        offset: usize,
        field: &str,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        reader
            .read_group_key_checked(offset)?
            .ok_or_else(|| missing_field_value(field, offset))
    }

    // Resolve one finalized aggregate leaf by compiled aggregate index.
    // The reader abstraction intentionally hides aggregate-row shape, so a
    // missing aggregate reports the failed index without importing row state.
    fn evaluate_aggregate(
        reader: &dyn CompiledExprValueReader,
        index: usize,
    ) -> Result<Cow<'_, Value>, ProjectionEvalError> {
        reader.read_aggregate_checked(index)?.ok_or(
            ProjectionEvalError::MissingGroupedAggregateValue {
                aggregate_index: index,
                aggregate_count: 0,
            },
        )
    }

    // Resolve one nested field-path leaf through the reader-owned decoding
    // boundary. Missing nested paths preserve projection semantics by
    // materializing SQL NULL; unsupported readers fail loudly as missing root
    // field access rather than silently returning NULL.
    fn evaluate_field_path<'row>(
        reader: &'row dyn CompiledExprValueReader,
        root_slot: usize,
        field: &str,
        segments: &[String],
        segment_bytes: &[Box<[u8]>],
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        match reader.read_field_path(root_slot, field, segments, segment_bytes)? {
            Some(value) => Ok(value),
            None => Err(ProjectionEvalError::MissingFieldPathValue {
                field: field.to_string(),
                root_slot,
            }),
        }
    }

    // Evaluate one dedicated direct-slot arithmetic variant. NULL propagation
    // and checked numeric behavior stay delegated to the value numeric
    // boundary instead of the generic projection expression evaluator.
    fn evaluate_slot_binary_arithmetic<'row>(
        reader: &'row dyn CompiledExprValueReader,
        op: BinaryOp,
        left: (usize, &str),
        right: (usize, &str),
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let (left_slot, left_field) = left;
        let (right_slot, right_field) = right;
        let left = reader
            .read_slot_checked(left_slot)?
            .ok_or_else(|| missing_field_value(left_field, left_slot))?;
        let right = reader
            .read_slot_checked(right_slot)?
            .ok_or_else(|| missing_field_value(right_field, right_slot))?;

        evaluate_numeric_binary_expr(op, left.as_ref(), right.as_ref()).map(Cow::Owned)
    }

    // Evaluate one dedicated direct-slot comparison variant using the
    // value-local ordering helpers. This keeps grouped CASE/FILTER predicates
    // away from generic binary expression dispatch for slot-vs-slot shapes.
    fn evaluate_slot_binary_comparison<'row>(
        reader: &'row dyn CompiledExprValueReader,
        op: BinaryOp,
        left: (usize, &str),
        right: (usize, &str),
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let (left_slot, left_field) = left;
        let (right_slot, right_field) = right;
        let left = reader
            .read_slot_checked(left_slot)?
            .ok_or_else(|| missing_field_value(left_field, left_slot))?;
        let right = reader
            .read_slot_checked(right_slot)?
            .ok_or_else(|| missing_field_value(right_field, right_slot))?;

        evaluate_compare_binary_expr(op, left.as_ref(), right.as_ref()).map(Cow::Owned)
    }

    // Evaluate one slot-literal binary variant without recursively visiting
    // either operand node. Operand order remains explicit because comparisons
    // and division are not commutative.
    fn evaluate_slot_literal_binary<'row>(
        reader: &'row dyn CompiledExprValueReader,
        op: BinaryOp,
        slot: usize,
        field: &str,
        literal: &Value,
        slot_on_left: bool,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let value = reader
            .read_slot_checked(slot)?
            .ok_or_else(|| missing_field_value(field, slot))?;
        let result = if slot_on_left {
            evaluate_binary_expr(op, value.as_ref(), literal)
        } else {
            evaluate_binary_expr(op, literal, value.as_ref())
        }?;

        Ok(Cow::Owned(result))
    }

    // Evaluate a one-arm CASE with a direct slot/literal comparison. This
    // avoids producing an intermediate `Value::Bool`; NULL comparison results
    // still fall through exactly like the generic CASE admission helper.
    fn evaluate_case_slot_literal<'row>(
        reader: &'row dyn CompiledExprValueReader,
        op: BinaryOp,
        slot_ref: (usize, &str),
        literal: &Value,
        slot_on_left: bool,
        then_expr: &'row Self,
        else_expr: &'row Self,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        if Self::evaluate_slot_literal_condition(reader, op, slot_ref, literal, slot_on_left)? {
            return then_expr.evaluate(reader);
        }

        else_expr.evaluate(reader)
    }

    // Evaluate a one-arm CASE whose condition is a boolean slot. NULL follows
    // SQL searched-CASE behavior and does not select the branch; non-boolean
    // values retain the existing CASE-condition diagnostic.
    fn evaluate_case_slot_bool<'row>(
        reader: &'row dyn CompiledExprValueReader,
        slot: usize,
        field: &str,
        then_expr: &'row Self,
        else_expr: &'row Self,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let condition = reader
            .read_slot_checked(slot)?
            .ok_or_else(|| missing_field_value(field, slot))?;
        let select_then = match condition.as_ref() {
            Value::Bool(value) => *value,
            Value::Null => false,
            found => {
                return Err(ProjectionEvalError::InvalidCaseCondition {
                    found: Box::new(found.clone()),
                });
            }
        };

        if select_then {
            return then_expr.evaluate(reader);
        }

        else_expr.evaluate(reader)
    }

    // Compare one slot against one literal as a searched-CASE predicate. The
    // helper mirrors comparison expression NULL and invalid-operand semantics,
    // but returns the branch decision directly instead of wrapping it in Value.
    fn evaluate_slot_literal_condition(
        reader: &dyn CompiledExprValueReader,
        op: BinaryOp,
        slot_ref: (usize, &str),
        literal: &Value,
        slot_on_left: bool,
    ) -> Result<bool, ProjectionEvalError> {
        let (slot, field) = slot_ref;
        let slot_value = reader
            .read_slot_checked(slot)?
            .ok_or_else(|| missing_field_value(field, slot))?;
        let (left, right) = if slot_on_left {
            (slot_value.as_ref(), literal)
        } else {
            (literal, slot_value.as_ref())
        };

        evaluate_compare_binary_condition(op, left, right)
    }

    // Evaluate searched CASE through compiled condition/result programs.
    // Only TRUE selects an arm; FALSE and NULL fall through through the
    // shared boolean admission helper.
    fn evaluate_case<'row>(
        reader: &'row dyn CompiledExprValueReader,
        when_then_arms: &'row [CompiledExprCaseArm],
        else_expr: &'row Self,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        for arm in when_then_arms {
            let condition = arm.condition.evaluate(reader)?;
            if admit_true_only_boolean_value(condition.as_ref(), |found| {
                ProjectionEvalError::InvalidCaseCondition {
                    found: Box::new(found.clone()),
                }
            })? {
                return arm.result.evaluate(reader);
            }
        }

        else_expr.evaluate(reader)
    }

    // Evaluate scalar function calls without heap allocation for common arities.
    // Larger dynamic functions still allocate their argument vector, matching
    // the existing semantics while keeping common grouped aggregate expressions
    // allocation-free.
    fn evaluate_function_call<'row>(
        reader: &'row dyn CompiledExprValueReader,
        function: Function,
        args: &'row [Self],
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let value = match args {
            [] => eval_grouped_function_call(function, &[])?,
            [arg] => {
                let arg = arg.evaluate(reader)?.into_owned();
                let args = [arg];

                eval_grouped_function_call(function, &args)?
            }
            [left, right] => {
                let left = left.evaluate(reader)?.into_owned();
                let right = right.evaluate(reader)?.into_owned();
                let args = [left, right];

                eval_grouped_function_call(function, &args)?
            }
            [first, second, third] => {
                let first = first.evaluate(reader)?.into_owned();
                let second = second.evaluate(reader)?.into_owned();
                let third = third.evaluate(reader)?.into_owned();
                let args = [first, second, third];

                eval_grouped_function_call(function, &args)?
            }
            args => {
                let mut evaluated_args = Vec::with_capacity(args.len());
                for arg in args {
                    evaluated_args.push(arg.evaluate(reader)?.into_owned());
                }

                eval_grouped_function_call(function, evaluated_args.as_slice())?
            }
        };

        Ok(Cow::Owned(value))
    }
}
/// Evaluate one compiled grouped HAVING expression against one grouped output row.
pub(in crate::db) fn evaluate_grouped_having_expr(
    expr: &CompiledExpr,
    grouped_row: &dyn CompiledExprValueReader,
) -> Result<bool, ProjectionEvalError> {
    collapse_true_only_boolean_admission(expr.evaluate(grouped_row)?.into_owned(), |found| {
        ProjectionEvalError::InvalidGroupedHavingResult { found }
    })
}

fn eval_grouped_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionEvalError> {
    eval_projection_function_call_checked(function, args).map_err(|err| match err {
        ProjectionFunctionEvalError::Numeric(err) => ProjectionEvalError::Numeric(err),
        ProjectionFunctionEvalError::Query(err) => ProjectionEvalError::InvalidFunctionCall {
            function: function.projection_eval_name().to_string(),
            message: err.to_string(),
        },
    })
}

fn evaluate_unary_expr(op: UnaryOp, value: &Value) -> Result<Value, ProjectionEvalError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Not => {
            let Value::Bool(value) = value else {
                return Err(ProjectionEvalError::InvalidUnaryOperand {
                    op: unary_op_name(op).to_string(),
                    found: Box::new(value.clone()),
                });
            };

            Ok(Value::Bool(!*value))
        }
    }
}

fn evaluate_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    match op {
        BinaryOp::Or | BinaryOp::And => evaluate_boolean_binary_expr(op, left, right),
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => evaluate_compare_binary_expr(op, left, right),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            evaluate_numeric_binary_expr(op, left, right)
        }
    }
}

fn evaluate_boolean_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    match op {
        BinaryOp::And => evaluate_boolean_and(left, right),
        BinaryOp::Or => evaluate_boolean_or(left, right),
        _ => Err(invalid_binary_operands(op, left, right)),
    }
}

fn evaluate_boolean_and(left: &Value, right: &Value) -> Result<Value, ProjectionEvalError> {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(true) | Value::Null, Value::Null) | (Value::Null, Value::Bool(true)) => {
            Ok(Value::Null)
        }
        _ => Err(invalid_binary_operands(BinaryOp::And, left, right)),
    }
}

fn evaluate_boolean_or(left: &Value, right: &Value) -> Result<Value, ProjectionEvalError> {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(false) | Value::Null, Value::Null) | (Value::Null, Value::Bool(false)) => {
            Ok(Value::Null)
        }
        _ => Err(invalid_binary_operands(BinaryOp::Or, left, right)),
    }
}

fn evaluate_numeric_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let result = match op {
        BinaryOp::Add => value_numeric::add(left, right),
        BinaryOp::Sub => value_numeric::sub(left, right),
        BinaryOp::Mul => value_numeric::mul(left, right),
        BinaryOp::Div => value_numeric::div(left, right),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => return Err(invalid_binary_operands(op, left, right)),
    }
    .map_err(map_numeric_arithmetic_error)?;
    let Some(result) = result else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(Value::Decimal(result))
}

fn evaluate_compare_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let result = match op {
        BinaryOp::Eq => value_ordering::eq(left, right),
        BinaryOp::Ne => value_ordering::ne(left, right),
        BinaryOp::Lt => value_ordering::lt(left, right),
        BinaryOp::Lte => value_ordering::lte(left, right),
        BinaryOp::Gt => value_ordering::gt(left, right),
        BinaryOp::Gte => value_ordering::gte(left, right),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => return Err(invalid_binary_operands(op, left, right)),
    };
    let Some(result) = result else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(Value::Bool(result))
}

fn evaluate_compare_binary_condition(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<bool, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(false);
    }

    let result = match op {
        BinaryOp::Eq => value_ordering::eq(left, right),
        BinaryOp::Ne => value_ordering::ne(left, right),
        BinaryOp::Lt => value_ordering::lt(left, right),
        BinaryOp::Lte => value_ordering::lte(left, right),
        BinaryOp::Gt => value_ordering::gt(left, right),
        BinaryOp::Gte => value_ordering::gte(left, right),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => return Err(invalid_binary_operands(op, left, right)),
    };
    let Some(result) = result else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(result)
}

fn map_numeric_arithmetic_error(err: value_numeric::NumericArithmeticError) -> ProjectionEvalError {
    match err {
        value_numeric::NumericArithmeticError::Overflow => NumericEvalError::Overflow,
        value_numeric::NumericArithmeticError::NotRepresentable => {
            NumericEvalError::NotRepresentable
        }
    }
    .into()
}

fn invalid_binary_operands(op: BinaryOp, left: &Value, right: &Value) -> ProjectionEvalError {
    ProjectionEvalError::InvalidBinaryOperands {
        op: binary_op_name(op).to_string(),
        left: Box::new(left.clone()),
        right: Box::new(right.clone()),
    }
}

const fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
    }
}

const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "or",
        BinaryOp::And => "and",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
    }
}
