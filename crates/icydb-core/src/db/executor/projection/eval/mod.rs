//! Module: executor::projection::eval
//! Responsibility: shared projection expression error taxonomy and scalar evaluation helpers.
//! Does not own: generic test evaluators, expression type inference, or planner semantic validation policy.
//! Boundary: production execution stays on compiled scalar programs while test
//! helpers exercise the same compiled evaluator paths directly.

mod operators;
mod scalar;

use crate::{
    db::{
        data::CanonicalSlotReader,
        numeric::NumericEvalError,
        query::plan::{EffectiveRuntimeFilterProgram, expr::admit_true_only_boolean_value},
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;
use thiserror::Error as ThisError;

pub(in crate::db) use crate::db::query::plan::expr::ScalarProjectionExpr;
pub(in crate::db) use operators::eval_binary_expr;
pub(in crate::db) use operators::eval_unary_expr;
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_canonical_scalar_projection_expr;
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr;
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_reader;
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_ref_reader;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use scalar::{
    eval_canonical_scalar_filter_expr_with_required_slot_reader_cow,
    eval_canonical_scalar_projection_expr_with_required_slot_reader_cow,
    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
    try_eval_field_path_literal_filter_expr,
};

///
/// ProjectionEvalError
///
/// Pure expression-evaluation failures for scalar projection execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum ProjectionEvalError {
    #[error("projection expression references unknown field '{field}'")]
    UnknownField { field: String },

    #[error("projection expression could not read field '{field}' at index={index}")]
    MissingFieldValue { field: String, index: usize },

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
    /// Map one projection evaluation failure into the executor invalid-logical-plan boundary.
    pub(in crate::db) fn into_invalid_logical_plan_internal_error(self) -> InternalError {
        if let Self::Numeric(err) = self {
            return err.into_internal_error();
        }

        InternalError::query_invalid_logical_plan(self.to_string())
    }

    /// Map one grouped projection evaluation failure into the grouped-output
    /// invalid-logical-plan boundary while preserving grouped context.
    pub(in crate::db::executor) fn into_grouped_projection_internal_error(self) -> InternalError {
        if let Self::Numeric(err) = self {
            return err.into_internal_error();
        }

        InternalError::query_invalid_logical_plan(format!(
            "grouped projection evaluation failed: {self}",
        ))
    }
}

// Evaluate one compiled scalar boolean filter expression through one required
// borrowed slot reader and collapse it through the shared TRUE-only admission
// boundary used by WHERE-style residual filtering.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_scalar_filter_expr_with_required_value_reader_cow<'a>(
    expr: &'a ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Result<std::borrow::Cow<'a, Value>, InternalError>,
) -> Result<bool, InternalError> {
    let value =
        eval_canonical_scalar_projection_expr_with_required_value_reader_cow(expr, read_slot)?;

    admit_true_only_boolean_value(value.as_ref(), |found| {
        InternalError::query_invalid_logical_plan(format!(
            "scalar filter expression produced non-boolean value {found:?}",
        ))
    })
}

// Evaluate one compiled scalar boolean filter expression through one canonical
// slot reader so field-path leaves can borrow raw value-storage bytes during
// scan-time filtering.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_scalar_filter_expr_with_required_slot_reader(
    expr: &ScalarProjectionExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, InternalError> {
    if let Some(admitted) = try_eval_field_path_literal_filter_expr(expr, slots)? {
        return Ok(admitted);
    }

    let Some(value) = eval_canonical_scalar_filter_expr_with_required_slot_reader_cow(expr, slots)?
    else {
        return Ok(false);
    };

    admit_true_only_boolean_value(value.as_ref(), |found| {
        InternalError::query_invalid_logical_plan(format!(
            "scalar filter expression produced non-boolean value {found:?}",
        ))
    })
}

// Evaluate one planner-selected effective runtime filter program directly
// against a canonical structural slot reader. This is the scan-time lane used
// before raw rows are reduced to retained `Value` slots, so field-path filters
// can resolve nested value-storage payloads without planner/index pushdown.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_effective_runtime_filter_program_with_slot_reader(
    filter_program: &EffectiveRuntimeFilterProgram,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, InternalError> {
    match filter_program {
        EffectiveRuntimeFilterProgram::Predicate(predicate_program) => {
            predicate_program.eval_with_structural_slot_reader(slots)
        }
        EffectiveRuntimeFilterProgram::Expr(filter_expr) => {
            eval_scalar_filter_expr_with_required_slot_reader(filter_expr, slots)
        }
    }
}

// Evaluate one planner-selected effective runtime filter program through one
// borrowed slot reader. Predicate-backed filters stay on the predicate hot
// loop while expression-backed residual filters reuse the shared scalar
// TRUE-only boolean admission boundary.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_effective_runtime_filter_program_with_value_ref_reader<'a, F>(
    filter_program: &EffectiveRuntimeFilterProgram,
    read_slot: &mut F,
    missing_slot_context: &str,
) -> Result<bool, InternalError>
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    match filter_program {
        EffectiveRuntimeFilterProgram::Predicate(predicate_program) => {
            let mut cached_read_slot = SlotRefEvaluationCache::new(read_slot);

            Ok(predicate_program
                .eval_with_slot_value_ref_reader(&mut |slot| cached_read_slot.read(slot)))
        }
        EffectiveRuntimeFilterProgram::Expr(filter_expr) => {
            eval_scalar_filter_expr_with_required_value_reader_cow(filter_expr, &mut |slot| {
                let Some(value) = read_slot(slot) else {
                    return Err(InternalError::query_invalid_logical_plan(format!(
                        "{missing_slot_context} {slot}",
                    )));
                };

                Ok(Cow::Borrowed(value))
            })
        }
    }
}

///
/// SlotRefEvaluationCache
///
/// SlotRefEvaluationCache memoizes borrowed slot reads during one row-local
/// predicate evaluation.
/// It exists for retained-row predicate paths where normalized predicates may
/// touch the same slot more than once, while keeping the cache stack-resident
/// so simple predicates do not allocate per row.
///

struct SlotRefEvaluationCache<'reader, 'value, F>
where
    F: FnMut(usize) -> Option<&'value Value>,
{
    read_slot: &'reader mut F,
    entries: [Option<(usize, Option<&'value Value>)>; 8],
    len: usize,
}

impl<'reader, 'value, F> SlotRefEvaluationCache<'reader, 'value, F>
where
    F: FnMut(usize) -> Option<&'value Value>,
{
    // Build one empty stack cache around the caller-owned slot reader.
    const fn new(read_slot: &'reader mut F) -> Self {
        Self {
            read_slot,
            entries: [None; 8],
            len: 0,
        }
    }

    // Read one slot from cache when possible, preserving both hit and miss
    // results. If a predicate references more than eight unique slots, later
    // unique slots bypass the cache instead of allocating.
    fn read(&mut self, slot: usize) -> Option<&'value Value> {
        for entry in self.entries.iter().take(self.len).flatten() {
            if entry.0 == slot {
                return entry.1;
            }
        }

        let value = (self.read_slot)(slot);
        if self.len < self.entries.len() {
            self.entries[self.len] = Some((slot, value));
            self.len += 1;
        }

        value
    }
}

// Evaluate one planner-selected effective runtime filter program through one
// slot reader that may return borrowed or owned values. This keeps predicate
// evaluation on the canonical cow-reader path while letting expression-backed
// residual filters reuse the same TRUE-only boolean admission seam.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_effective_runtime_filter_program_with_value_cow_reader<'a, F>(
    filter_program: &EffectiveRuntimeFilterProgram,
    read_slot: &mut F,
    missing_slot_context: &str,
) -> Result<bool, InternalError>
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    match filter_program {
        EffectiveRuntimeFilterProgram::Predicate(predicate_program) => {
            Ok(predicate_program.eval_with_slot_value_cow_reader(read_slot))
        }
        EffectiveRuntimeFilterProgram::Expr(filter_expr) => {
            eval_scalar_filter_expr_with_required_value_reader_cow(filter_expr, &mut |slot| {
                let Some(value) = read_slot(slot) else {
                    return Err(InternalError::query_invalid_logical_plan(format!(
                        "{missing_slot_context} {slot}",
                    )));
                };

                Ok(value)
            })
        }
    }
}
