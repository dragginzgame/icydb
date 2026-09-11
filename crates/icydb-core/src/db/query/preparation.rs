//! Short-lived preparation accounting against the session's existing request.
//! Counters are never created here or retained in prepared/cached artifacts.

mod expr;
mod predicate;
mod text;

use crate::{
    db::{
        QueryError, executor::budget::HardExecutionContext,
        query::construction::ConstructionBudget, session::RequestExecutionScope,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};
use std::cell::Cell;

/// Borrowed request authority for actual preparation work, including failures.
pub(in crate::db) struct PreparationWork<'a> {
    scope: &'a RequestExecutionScope,
    context: HardExecutionContext,
    last_instruction_counter: Cell<u64>,
    charges_since_watermark: Cell<u8>,
}

impl PreparationWork<'_> {
    /// Copy an admitted name, charging traversal, bytes and destination backing.
    pub(in crate::db) fn copy_text(&self, text: &str) -> Result<String, QueryError> {
        (self as &dyn ConstructionBudget)
            .copy_text(text)
            .map_err(QueryError::execute)
    }

    /// Copy admitted values through the shared construction owner.
    pub(in crate::db) fn copy_value(&self, value: &Value) -> Result<Value, QueryError> {
        (self as &dyn ConstructionBudget)
            .copy_value(value)
            .map_err(QueryError::execute)
    }

    /// Reserve once for a known output length. Child construction owns its
    /// payload charges; this accounts for the destination container backing.
    pub(in crate::db) fn copy_slice<T, U>(
        &self,
        values: &[T],
        mut copy: impl FnMut(&T) -> Result<U, QueryError>,
    ) -> Result<Vec<U>, QueryError> {
        let mut copied = self.vec_with_capacity(values.len())?;
        for value in values {
            copied.push(copy(value)?);
        }
        Ok(copied)
    }

    /// Charge known destination backing before allocating a clause container.
    /// Callers must not grow the vector beyond this admitted capacity.
    pub(in crate::db) fn vec_with_capacity<T>(&self, len: usize) -> Result<Vec<T>, QueryError> {
        (self as &dyn ConstructionBudget)
            .vec_with_capacity(len)
            .map_err(QueryError::execute)
    }

    /// Run one preparation segment without resetting request counters. Capture
    /// instructions on both successful and rejected work before leaving it.
    pub(in crate::db) fn run<T>(
        scope: &RequestExecutionScope,
        lane: DiagnosticExecutionLane,
        run: impl FnOnce(&PreparationWork<'_>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let work = PreparationWork {
            scope,
            // No canonical identity exists yet; attribution contains no input.
            context: HardExecutionContext::new(DiagnosticExecutionBudgetScope::Request, lane, 0),
            last_instruction_counter: Cell::new(crate::runtime::local_instruction_counter()),
            charges_since_watermark: Cell::new(0),
        };
        let result = run(&work);
        work.check_instruction_watermark()
            .map_err(QueryError::execute)?;

        result
    }

    /// Charge before the next unit of work, never through the optional active
    /// row-execution tracker: preparation also runs without that tracker.
    pub(in crate::db) fn charge(
        &self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), QueryError> {
        ConstructionBudget::charge(self, resource, amount).map_err(QueryError::execute)
    }

    /// Charge the requested new backing allocation (including retained-prefix
    /// copies on growth) before reserving it. This is cumulative construction
    /// accounting, not an allocator/live-heap measurement.
    pub(in crate::db) fn reserve_vec<T>(
        &self,
        values: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), QueryError> {
        (self as &dyn ConstructionBudget)
            .reserve_vec(values, additional)
            .map_err(QueryError::execute)
    }

    /// Reserve text construction under the same cumulative allocation policy.
    pub(in crate::db) fn reserve_string(
        &self,
        text: &mut String,
        additional: usize,
    ) -> Result<(), QueryError> {
        (self as &dyn ConstructionBudget)
            .reserve_string(text, additional)
            .map_err(QueryError::execute)
    }

    fn check_instruction_watermark(&self) -> Result<(), InternalError> {
        let current = crate::runtime::local_instruction_counter();
        let previous = self.last_instruction_counter.replace(current);
        self.charges_since_watermark.set(0);
        self.scope
            .charge(
                self.context,
                DiagnosticExecutionBudgetResource::InstructionUnits,
                current.saturating_sub(previous),
            )
            .map_err(InternalError::from)
    }
}

impl ConstructionBudget for PreparationWork<'_> {
    fn charge(
        &self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), InternalError> {
        self.scope
            .charge(self.context, resource, amount)
            .map_err(InternalError::from)?;
        let charges = self.charges_since_watermark.get() + 1;
        self.charges_since_watermark.set(charges);
        if charges == 64 {
            self.check_instruction_watermark()?;
        }
        Ok(())
    }
}

/// Give unit-level preparation fixtures an explicit finite request, just as
/// session entrypoints do. Callers testing exhaustion supply their own root.
#[cfg(test)]
pub(in crate::db) fn with_preparation_work<T>(run: impl FnOnce(&PreparationWork<'_>) -> T) -> T {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        Ok(run(work))
    })
    .expect("fixture preparation fits a production request")
}
