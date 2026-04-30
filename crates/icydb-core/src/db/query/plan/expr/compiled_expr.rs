//! Module: query::plan::expr::compiled_expr
//! Responsibility: compiled expression programs and expression evaluation.
//! Does not own: row loops, grouped aggregate reducer mechanics, or
//! scan/projection orchestration.
//! Boundary: expression-layer programs evaluate already-loaded slot values so
//! callers can stay on row loading, reducer updates, and LIMIT handling.
//!
//! Invariants:
//! - CompiledExpr is the single expression IR in the system.
//! - All expression evaluation must go through CompiledExpr::evaluate.
//! - Readers must fail, not return NULL, for invalid access patterns.
//! - All semantics for numeric, boolean, and comparison evaluation are centralized here.
//! - No planner or executor types may appear in this module.

use crate::{
    db::{
        numeric::NumericEvalError,
        query::plan::expr::{
            BinaryOp, Function, ProjectionFunctionEvalError, UnaryOp,
            admit_true_only_boolean_value, collapse_true_only_boolean_admission,
            eval_projection_function_call_checked,
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    value::{
        Value,
        ops::{numeric as value_numeric, ordering as value_ordering},
    },
};
use std::borrow::Cow;
use thiserror::Error as ThisError;

///
/// ProjectionEvalError
///
/// ProjectionEvalError is the expression-layer failure taxonomy for compiled
/// expression evaluation.
/// It lives beside `CompiledExpr` so scalar, grouped, HAVING, and aggregate
/// input evaluation share one set of diagnostics instead of recreating error
/// boundaries in caller modules.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum ProjectionEvalError {
    #[error("projection expression references unknown field '{field}'")]
    UnknownField { field: String },

    #[error("projection expression could not read field '{field}' at index={index}")]
    MissingFieldValue { field: String, index: usize },

    #[error(
        "projection expression could not read field-path '{field}' rooted at index={root_slot}"
    )]
    MissingFieldPathValue { field: String, root_slot: usize },

    #[error("projection field-path '{field}' failed evaluation: {message}")]
    FieldPathEvaluationFailed {
        field: String,
        message: String,
        class: ErrorClass,
        origin: ErrorOrigin,
    },

    #[error("projection value reader failed: {message}")]
    ReaderFailed {
        message: String,
        class: ErrorClass,
        origin: ErrorOrigin,
    },

    #[error("projection unary operator '{op}' is incompatible with operand value {found:?}")]
    InvalidUnaryOperand { op: String, found: Box<Value> },

    #[error("projection CASE condition produced non-boolean value {found:?}")]
    InvalidCaseCondition { found: Box<Value> },

    #[error(
        "projection binary operator '{op}' is incompatible with operand values ({left:?}, {right:?})"
    )]
    InvalidBinaryOperands {
        op: String,
        left: Box<Value>,
        right: Box<Value>,
    },

    #[error(
        "grouped projection expression references unknown aggregate expression kind={kind} target_field={target_field:?} distinct={distinct}"
    )]
    UnknownGroupedAggregateExpression {
        kind: String,
        target_field: Option<String>,
        distinct: bool,
    },

    #[error(
        "grouped projection expression references aggregate output index={aggregate_index} but only {aggregate_count} outputs are available"
    )]
    MissingGroupedAggregateValue {
        aggregate_index: usize,
        aggregate_count: usize,
    },

    #[error("projection function '{function}' failed evaluation: {message}")]
    InvalidFunctionCall { function: String, message: String },

    #[error("{0}")]
    Numeric(#[from] NumericEvalError),

    #[error("grouped HAVING expression produced non-boolean value {found:?}")]
    InvalidGroupedHavingResult { found: Box<Value> },
}

impl ProjectionEvalError {
    /// Map one projection evaluation failure into the invalid-logical-plan boundary.
    pub(in crate::db) fn into_invalid_logical_plan_internal_error(self) -> InternalError {
        match self {
            Self::Numeric(err) => err.into_internal_error(),
            Self::FieldPathEvaluationFailed {
                message,
                class,
                origin,
                ..
            }
            | Self::ReaderFailed {
                message,
                class,
                origin,
            } => InternalError::new(class, origin, message),
            err => InternalError::query_invalid_logical_plan(err.to_string()),
        }
    }

    /// Map one grouped projection evaluation failure into the grouped-output
    /// invalid-logical-plan boundary while preserving grouped context.
    pub(in crate::db) fn into_grouped_projection_internal_error(self) -> InternalError {
        match self {
            Self::Numeric(err) => err.into_internal_error(),
            Self::FieldPathEvaluationFailed {
                message,
                class,
                origin,
                ..
            }
            | Self::ReaderFailed {
                message,
                class,
                origin,
            } => InternalError::new(class, origin, message),
            err => InternalError::query_invalid_logical_plan(format!(
                "grouped projection evaluation failed: {err}",
            )),
        }
    }
}

///
/// CompiledExprValueReader
///
/// CompiledExprValueReader is the only value-access contract visible to the
/// compiled expression evaluator.
/// Row, grouped-output, and HAVING execution expose their context-specific
/// values through this trait so the expression engine depends only on resolved
/// value locations after compilation.
///

pub(in crate::db) trait CompiledExprValueReader {
    /// Borrow one row-local slot value by compiled slot index.
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>>;

    /// Read one row-local slot value, preserving reader-owned failures.
    fn read_slot_checked(
        &self,
        slot: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Ok(self.read_slot(slot))
    }

    /// Borrow one finalized grouped-key value by compiled group-field offset.
    fn read_group_key(&self, offset: usize) -> Option<Cow<'_, Value>>;

    /// Read one finalized grouped-key value, preserving reader-owned failures.
    fn read_group_key_checked(
        &self,
        offset: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Ok(self.read_group_key(offset))
    }

    /// Borrow one finalized aggregate value by compiled aggregate output index.
    fn read_aggregate(&self, index: usize) -> Option<Cow<'_, Value>>;

    /// Read one finalized aggregate value, preserving reader-owned failures.
    fn read_aggregate_checked(
        &self,
        index: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Ok(self.read_aggregate(index))
    }

    /// Read one nested field-path value rooted at a compiled slot.
    fn read_field_path(
        &self,
        root_slot: usize,
        field: &str,
        _segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Err(missing_field_value(field, root_slot))
    }
}

///
/// CompiledExpr
///
/// CompiledExpr is the single executable scalar-expression IR used by row
/// evaluation, grouped aggregate input/filter evaluation, grouped output
/// projection, and HAVING.
/// Slot, grouped-key, and aggregate leaves are all resolved before this type is
/// built, keeping expression execution on resolved value locations while sharing
/// one evaluator for every runtime context.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CompiledExpr {
    Slot {
        slot: usize,
        field: String,
    },
    GroupKey {
        offset: usize,
        field: String,
    },
    Aggregate {
        index: usize,
    },
    Literal(Value),
    Add {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Sub {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Mul {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Div {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Eq {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Ne {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Lt {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Lte {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Gt {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Gte {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    BinarySlotLiteral {
        op: BinaryOp,
        slot: usize,
        field: String,
        literal: Value,
        slot_on_left: bool,
    },
    CaseSlotLiteral {
        op: BinaryOp,
        slot: usize,
        field: String,
        literal: Value,
        slot_on_left: bool,
        then_expr: Box<Self>,
        else_expr: Box<Self>,
    },
    CaseSlotBool {
        slot: usize,
        field: String,
        then_expr: Box<Self>,
        else_expr: Box<Self>,
    },
    FieldPath {
        root_slot: usize,
        field: String,
        segments: Box<[String]>,
        segment_bytes: Box<[Box<[u8]>]>,
    },
    FunctionCall {
        function: Function,
        args: Box<[Self]>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Box<[CompiledExprCaseArm]>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

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
    // and checked numeric behavior stay delegated to `value::ops::numeric`
    // instead of the generic projection expression evaluator.
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

///
/// CompiledExprCaseArm
///
/// CompiledExprCaseArm stores one searched-CASE condition/result pair after
/// both expressions have been compiled into the single expression IR.
/// It keeps CASE branch laziness inside the expression layer without retaining
/// pre-compilation CASE arm structures after compilation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CompiledExprCaseArm {
    condition: CompiledExpr,
    result: CompiledExpr,
}

impl CompiledExprCaseArm {
    /// Build one compiled CASE arm from already-compiled condition/result nodes.
    #[must_use]
    pub(in crate::db) const fn new(condition: CompiledExpr, result: CompiledExpr) -> Self {
        Self { condition, result }
    }
}

impl CompiledExpr {
    /// Return whether this compiled expression contains a nested field-path leaf.
    #[must_use]
    pub(in crate::db) fn contains_field_path(&self) -> bool {
        match self {
            Self::FieldPath { .. } => true,
            Self::FunctionCall { args, .. } => args.iter().any(Self::contains_field_path),
            Self::Unary { expr, .. } => expr.contains_field_path(),
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().any(|arm| {
                    arm.condition.contains_field_path() || arm.result.contains_field_path()
                }) || else_expr.contains_field_path()
            }
            Self::Binary { left, right, .. } => {
                left.contains_field_path() || right.contains_field_path()
            }
            Self::CaseSlotLiteral {
                then_expr,
                else_expr,
                ..
            }
            | Self::CaseSlotBool {
                then_expr,
                else_expr,
                ..
            } => then_expr.contains_field_path() || else_expr.contains_field_path(),
            Self::Slot { .. }
            | Self::GroupKey { .. }
            | Self::Aggregate { .. }
            | Self::Literal(_)
            | Self::Add { .. }
            | Self::Sub { .. }
            | Self::Mul { .. }
            | Self::Div { .. }
            | Self::Eq { .. }
            | Self::Ne { .. }
            | Self::Lt { .. }
            | Self::Lte { .. }
            | Self::Gt { .. }
            | Self::Gte { .. }
            | Self::BinarySlotLiteral { .. } => false,
        }
    }

    /// Visit every row slot referenced by this compiled expression.
    pub(in crate::db) fn for_each_referenced_slot(&self, visit: &mut impl FnMut(usize)) {
        match self {
            Self::Slot { slot, .. }
            | Self::FieldPath {
                root_slot: slot, ..
            }
            | Self::BinarySlotLiteral { slot, .. }
            | Self::CaseSlotLiteral { slot, .. }
            | Self::CaseSlotBool { slot, .. } => visit(*slot),
            Self::Add {
                left_slot,
                right_slot,
                ..
            }
            | Self::Sub {
                left_slot,
                right_slot,
                ..
            }
            | Self::Mul {
                left_slot,
                right_slot,
                ..
            }
            | Self::Div {
                left_slot,
                right_slot,
                ..
            }
            | Self::Eq {
                left_slot,
                right_slot,
                ..
            }
            | Self::Ne {
                left_slot,
                right_slot,
                ..
            }
            | Self::Lt {
                left_slot,
                right_slot,
                ..
            }
            | Self::Lte {
                left_slot,
                right_slot,
                ..
            }
            | Self::Gt {
                left_slot,
                right_slot,
                ..
            }
            | Self::Gte {
                left_slot,
                right_slot,
                ..
            } => {
                visit(*left_slot);
                visit(*right_slot);
            }
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    arg.for_each_referenced_slot(visit);
                }
            }
            Self::Unary { expr, .. } => expr.for_each_referenced_slot(visit),
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    arm.condition.for_each_referenced_slot(visit);
                    arm.result.for_each_referenced_slot(visit);
                }
                else_expr.for_each_referenced_slot(visit);
            }
            Self::Binary { left, right, .. } => {
                left.for_each_referenced_slot(visit);
                right.for_each_referenced_slot(visit);
            }
            Self::GroupKey { .. } | Self::Aggregate { .. } | Self::Literal(_) => {}
        }
    }

    /// Extend one slot list with every unique row slot referenced by this expression.
    pub(in crate::db) fn extend_referenced_slots(&self, referenced: &mut Vec<usize>) {
        self.for_each_referenced_slot(&mut |slot| {
            if !referenced.contains(&slot) {
                referenced.push(slot);
            }
        });
    }

    /// Mark every row slot referenced by this expression on a caller-owned bitset.
    pub(in crate::db) fn mark_referenced_slots(&self, referenced: &mut [bool]) {
        self.for_each_referenced_slot(&mut |slot| {
            if let Some(required) = referenced.get_mut(slot) {
                *required = true;
            }
        });
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

pub(in crate::db) fn evaluate_unary_expr(
    op: UnaryOp,
    value: &Value,
) -> Result<Value, ProjectionEvalError> {
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

pub(in crate::db) fn evaluate_binary_expr(
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

fn missing_field_value(field: &str, index: usize) -> ProjectionEvalError {
    ProjectionEvalError::MissingFieldValue {
        field: field.to_string(),
        index,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::plan::expr::{
            BinaryOp, CompiledExpr, CompiledExprValueReader, Function, ProjectionEvalError,
        },
        value::Value,
    };
    use std::borrow::Cow;
    use std::cmp::Ordering;

    struct TestRowView {
        slots: Vec<Option<Value>>,
    }

    struct TestGroupedView {
        group_keys: Vec<Value>,
        aggregates: Vec<Value>,
    }

    impl CompiledExprValueReader for TestRowView {
        fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.slots
                .get(slot)
                .and_then(Option::as_ref)
                .map(Cow::Borrowed)
        }

        fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
            None
        }

        fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
            None
        }
    }

    impl CompiledExprValueReader for TestGroupedView {
        fn read_slot(&self, _slot: usize) -> Option<Cow<'_, Value>> {
            None
        }

        fn read_group_key(&self, offset: usize) -> Option<Cow<'_, Value>> {
            self.group_keys.get(offset).map(Cow::Borrowed)
        }

        fn read_aggregate(&self, index: usize) -> Option<Cow<'_, Value>> {
            self.aggregates.get(index).map(Cow::Borrowed)
        }
    }

    fn row_view() -> TestRowView {
        TestRowView {
            slots: vec![
                Some(Value::Uint(7)),
                Some(Value::Int(3)),
                Some(Value::Null),
                Some(Value::Text("MiXeD".to_string())),
                Some(Value::Bool(true)),
            ],
        }
    }

    fn grouped_view() -> TestGroupedView {
        TestGroupedView {
            group_keys: vec![Value::Text("fighter".to_string())],
            aggregates: vec![Value::Uint(2)],
        }
    }

    fn evaluate(expr: &CompiledExpr) -> Value {
        expr.evaluate(&row_view())
            .expect("grouped compiled expression should evaluate")
            .into_owned()
    }

    #[test]
    fn grouped_compiled_expr_reads_slots_without_cloning_contract_drift() {
        let expr = CompiledExpr::Slot {
            slot: 0,
            field: "age".to_string(),
        };

        assert_eq!(evaluate(&expr), Value::Uint(7));
    }

    #[test]
    fn grouped_compiled_expr_preserves_slot_arithmetic_semantics() {
        let expr = CompiledExpr::Add {
            left_slot: 0,
            left_field: "age".to_string(),
            right_slot: 1,
            right_field: "rank".to_string(),
        };
        let value = evaluate(&expr);

        assert_eq!(
            value.cmp_numeric(&Value::Int(10)),
            Some(Ordering::Equal),
            "direct slot arithmetic must preserve shared numeric coercion semantics",
        );
    }

    #[test]
    fn grouped_compiled_expr_case_only_true_selects_branch() {
        let expr = CompiledExpr::Case {
            when_then_arms: vec![
                super::CompiledExprCaseArm {
                    condition: CompiledExpr::Literal(Value::Null),
                    result: CompiledExpr::Literal(Value::Text("null".to_string())),
                },
                super::CompiledExprCaseArm {
                    condition: CompiledExpr::BinarySlotLiteral {
                        op: BinaryOp::Gt,
                        slot: 0,
                        field: "age".to_string(),
                        literal: Value::Uint(5),
                        slot_on_left: true,
                    },
                    result: CompiledExpr::Literal(Value::Text("selected".to_string())),
                },
            ]
            .into_boxed_slice(),
            else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
        };

        assert_eq!(evaluate(&expr), Value::Text("selected".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_case_false_and_null_fall_through() {
        let expr = CompiledExpr::Case {
            when_then_arms: vec![
                super::CompiledExprCaseArm {
                    condition: CompiledExpr::Literal(Value::Null),
                    result: CompiledExpr::Literal(Value::Text("null".to_string())),
                },
                super::CompiledExprCaseArm {
                    condition: CompiledExpr::Literal(Value::Bool(false)),
                    result: CompiledExpr::Literal(Value::Text("false".to_string())),
                },
            ]
            .into_boxed_slice(),
            else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
        };

        assert_eq!(evaluate(&expr), Value::Text("else".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_case_slot_literal_selects_without_condition_value() {
        let expr = CompiledExpr::CaseSlotLiteral {
            op: BinaryOp::Gt,
            slot: 0,
            field: "age".to_string(),
            literal: Value::Uint(5),
            slot_on_left: true,
            then_expr: Box::new(CompiledExpr::Literal(Value::Text("selected".to_string()))),
            else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
        };

        assert_eq!(evaluate(&expr), Value::Text("selected".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_case_slot_bool_preserves_null_fallthrough() {
        let expr = CompiledExpr::CaseSlotBool {
            slot: 2,
            field: "maybe_flag".to_string(),
            then_expr: Box::new(CompiledExpr::Literal(Value::Text("selected".to_string()))),
            else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
        };

        assert_eq!(evaluate(&expr), Value::Text("else".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_function_calls_reuse_projection_semantics() {
        let expr = CompiledExpr::FunctionCall {
            function: Function::Lower,
            args: vec![CompiledExpr::Slot {
                slot: 3,
                field: "name".to_string(),
            }]
            .into_boxed_slice(),
        };

        assert_eq!(evaluate(&expr), Value::Text("mixed".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_missing_slot_keeps_field_diagnostic() {
        let expr = CompiledExpr::Slot {
            slot: 99,
            field: "missing_field".to_string(),
        };
        let err = expr
            .evaluate(&row_view())
            .expect_err("missing grouped slot should stay a projection error");

        assert_eq!(
            err.to_string(),
            "projection expression could not read field 'missing_field' at index=99",
        );
    }

    #[test]
    fn compiled_expr_aggregate_in_row_context_errors_not_null() {
        let expr = CompiledExpr::Aggregate { index: 0 };
        let err = expr
            .evaluate(&row_view())
            .expect_err("row readers must not silently NULL aggregate leaves");

        assert_eq!(
            err,
            ProjectionEvalError::MissingGroupedAggregateValue {
                aggregate_index: 0,
                aggregate_count: 0,
            },
        );
    }

    #[test]
    fn compiled_expr_group_key_in_row_context_errors_not_null() {
        let expr = CompiledExpr::GroupKey {
            offset: 0,
            field: "class".to_string(),
        };
        let err = expr
            .evaluate(&row_view())
            .expect_err("row readers must not silently NULL grouped-key leaves");

        assert_eq!(
            err,
            ProjectionEvalError::MissingFieldValue {
                field: "class".to_string(),
                index: 0,
            },
        );
    }

    #[test]
    fn compiled_expr_slot_in_grouped_context_errors_not_null() {
        let expr = CompiledExpr::Slot {
            slot: 0,
            field: "age".to_string(),
        };
        let err = expr
            .evaluate(&grouped_view())
            .expect_err("grouped-output readers must not silently NULL slot leaves");

        assert_eq!(
            err,
            ProjectionEvalError::MissingFieldValue {
                field: "age".to_string(),
                index: 0,
            },
        );
    }

    #[test]
    fn compiled_expr_out_of_bounds_grouped_reads_error_not_null() {
        let grouped_view = grouped_view();
        let group_key = CompiledExpr::GroupKey {
            offset: 9,
            field: "class".to_string(),
        };
        let aggregate = CompiledExpr::Aggregate { index: 9 };

        assert!(matches!(
            group_key.evaluate(&grouped_view),
            Err(ProjectionEvalError::MissingFieldValue { field, index })
                if field == "class" && index == 9
        ));
        assert!(matches!(
            aggregate.evaluate(&grouped_view),
            Err(ProjectionEvalError::MissingGroupedAggregateValue {
                aggregate_index: 9,
                ..
            })
        ));
    }

    #[test]
    fn compiled_expr_case_missing_condition_read_errors_before_else() {
        let expr = CompiledExpr::Case {
            when_then_arms: vec![super::CompiledExprCaseArm {
                condition: CompiledExpr::Aggregate { index: 0 },
                result: CompiledExpr::Literal(Value::Text("then".to_string())),
            }]
            .into_boxed_slice(),
            else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
        };
        let err = expr
            .evaluate(&row_view())
            .expect_err("missing CASE condition reads must not fall through as NULL");

        assert_eq!(
            err,
            ProjectionEvalError::MissingGroupedAggregateValue {
                aggregate_index: 0,
                aggregate_count: 0,
            },
        );
    }

    #[test]
    fn compiled_expr_case_slot_bool_matches_generic_non_boolean_admission() {
        let generic = CompiledExpr::Case {
            when_then_arms: vec![super::CompiledExprCaseArm {
                condition: CompiledExpr::Slot {
                    slot: 3,
                    field: "name".to_string(),
                },
                result: CompiledExpr::Literal(Value::Text("then".to_string())),
            }]
            .into_boxed_slice(),
            else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
        };
        let specialized = CompiledExpr::CaseSlotBool {
            slot: 3,
            field: "name".to_string(),
            then_expr: Box::new(CompiledExpr::Literal(Value::Text("then".to_string()))),
            else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
        };

        assert_eq!(
            generic
                .evaluate(&row_view())
                .expect_err("generic CASE should reject text condition"),
            specialized
                .evaluate(&row_view())
                .expect_err("specialized CASE should reject text condition"),
        );
    }
}
