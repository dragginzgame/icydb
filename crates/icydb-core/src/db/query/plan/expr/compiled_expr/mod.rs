//! Module: query::plan::expr::compiled_expr
//! Responsibility: compiled expression programs, compilation, and evaluation.
//! Does not own: row loops, grouped aggregate reducer mechanics, or
//! scan/projection orchestration.
//! Boundary: expression-layer programs evaluate already-loaded slot values so
//! callers can stay on row loading, reducer updates, and LIMIT handling.
//!
//! Invariants:
//! - CompiledExpr is the single expression IR in the system.
//! - The compile submodule is the only planner tree to CompiledExpr translation boundary.
//! - All expression evaluation must go through CompiledExpr::evaluate.
//! - Readers must fail, not return NULL, for invalid access patterns.
//! - All semantics for numeric, boolean, and comparison evaluation are centralized here.
//! - Executor row shapes stay behind CompiledExprValueReader implementations.

mod compile;
mod evaluate;

use crate::{
    db::{
        numeric::NumericEvalError,
        query::plan::expr::{BinaryOp, Function, UnaryOp},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    value::Value,
};
use std::borrow::Cow;
use thiserror::Error as ThisError;

pub(in crate::db) use compile::{compile_grouped_projection_expr, compile_grouped_projection_plan};
pub(in crate::db) use evaluate::evaluate_grouped_having_expr;

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
    /// Return the direct row slot used by `OCTET_LENGTH(slot)` when the
    /// expression has that exact shape.
    ///
    /// This keeps raw-row readers from pattern matching expression internals
    /// when they can answer byte-length requests from their storage-native
    /// scalar view.
    #[must_use]
    pub(in crate::db) fn direct_octet_length_slot(&self) -> Option<(usize, &str)> {
        let Self::FunctionCall {
            function: Function::OctetLength,
            args,
        } = self
        else {
            return None;
        };
        let [Self::Slot { slot, field }] = args.as_ref() else {
            return None;
        };

        Some((*slot, field.as_str()))
    }

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
mod tests;
