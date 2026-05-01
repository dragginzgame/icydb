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

// Evaluate one planner-selected effective runtime filter program directly
// against a canonical structural slot reader. This is the scan-time lane used
// before raw rows are reduced to retained `Value` slots, so field-path filters
// can resolve nested value-storage payloads without planner/index pushdown.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_effective_runtime_filter_program_with_slot_reader(
    filter_program: &EffectiveRuntimeFilterProgram,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, InternalError> {
    filter_program.eval_with_slot_reader(slots)
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
    filter_program.eval_with_value_ref_reader(read_slot, missing_slot_context)
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
    filter_program.eval_with_value_cow_reader(read_slot, missing_slot_context)
}
