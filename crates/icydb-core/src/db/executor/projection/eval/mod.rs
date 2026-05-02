//! Module: executor::projection::eval
//! Responsibility: shared projection expression error taxonomy and scalar evaluation helpers.
//! Does not own: generic test evaluators, expression type inference, or planner semantic validation policy.
//! Boundary: production execution stays on compiled scalar programs while test
//! helpers exercise the same compiled evaluator paths directly.

mod scalar;

use crate::{
    db::{data::CanonicalSlotReader, query::plan::EffectiveRuntimeFilterProgram},
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

pub(in crate::db) use crate::db::query::plan::expr::ProjectionEvalError;
pub(in crate::db::executor) use scalar::eval_compiled_expr_with_required_slot_reader_cow;
pub(in crate::db::executor) use scalar::eval_compiled_expr_with_value_reader;
pub(in crate::db::executor) use scalar::eval_compiled_expr_with_value_ref_reader;
pub(in crate::db) use scalar::{
    eval_compiled_filter_expr_with_required_slot_reader,
    eval_compiled_filter_expr_with_value_cow_reader,
    eval_compiled_filter_expr_with_value_ref_reader,
};

///
/// SlotRefEvaluationCache
///
/// SlotRefEvaluationCache memoizes borrowed slot reads during one row-local
/// predicate evaluation.
/// It is used by executor retained-row filter paths where normalized
/// predicates may touch the same slot more than once.
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
    // results. Later unique slots bypass the cache instead of allocating.
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

// Evaluate one planner-selected effective runtime filter program directly
// against a canonical structural slot reader. This is the scan-time lane used
// before raw rows are reduced to retained `Value` slots, so field-path filters
// can resolve nested value-storage payloads without planner/index pushdown.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_effective_runtime_filter_program_with_slot_reader(
    filter_program: &EffectiveRuntimeFilterProgram,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, InternalError> {
    if let Some(predicate_program) = filter_program.predicate_program() {
        return predicate_program.eval_with_structural_slot_reader(slots);
    }

    let Some(filter_expr) = filter_program.expression_filter() else {
        return Err(InternalError::query_executor_invariant(
            "effective runtime filter must contain a predicate or expression program",
        ));
    };

    eval_compiled_filter_expr_with_required_slot_reader(filter_expr, slots)
}

// Evaluate one planner-selected effective runtime filter program through one
// borrowed slot reader. Predicate-backed filters stay on the predicate hot
// loop while expression-backed residual filters reuse the shared scalar
// TRUE-only boolean admission boundary.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_effective_runtime_filter_program_with_value_ref_reader<'a, F>(
    filter_program: &EffectiveRuntimeFilterProgram,
    read_slot: &mut F,
    missing_slot_context: &str,
) -> Result<bool, InternalError>
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    if let Some(predicate_program) = filter_program.predicate_program() {
        let mut cached_read_slot = SlotRefEvaluationCache::new(read_slot);

        return Ok(predicate_program
            .eval_with_slot_value_ref_reader(&mut |slot| cached_read_slot.read(slot)));
    }

    let Some(filter_expr) = filter_program.expression_filter() else {
        return Err(InternalError::query_executor_invariant(
            "effective runtime filter must contain a predicate or expression program",
        ));
    };

    eval_compiled_filter_expr_with_value_ref_reader(filter_expr, read_slot, missing_slot_context)
}

// Evaluate one planner-selected effective runtime filter program through one
// slot reader that may return borrowed or owned values. This keeps predicate
// evaluation on the canonical cow-reader path while letting expression-backed
// residual filters reuse the same TRUE-only boolean admission seam.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_effective_runtime_filter_program_with_value_cow_reader<'a, F>(
    filter_program: &EffectiveRuntimeFilterProgram,
    read_slot: &mut F,
    missing_slot_context: &str,
) -> Result<bool, InternalError>
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    if let Some(predicate_program) = filter_program.predicate_program() {
        return Ok(predicate_program.eval_with_slot_value_cow_reader(read_slot));
    }

    let Some(filter_expr) = filter_program.expression_filter() else {
        return Err(InternalError::query_executor_invariant(
            "effective runtime filter must contain a predicate or expression program",
        ));
    };

    eval_compiled_filter_expr_with_value_cow_reader(filter_expr, read_slot, missing_slot_context)
}
