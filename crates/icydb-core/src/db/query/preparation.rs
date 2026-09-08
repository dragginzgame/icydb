//! Short-lived preparation accounting against the session's existing request.
//! Counters are never created here or retained in prepared/cached artifacts.

use crate::{
    db::{QueryError, executor::budget::HardExecutionContext, session::RequestExecutionScope},
    error::InternalError,
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
        work.check_instruction_watermark()?;

        result
    }

    /// Charge before the next unit of work, never through the optional active
    /// row-execution tracker: preparation also runs without that tracker.
    pub(in crate::db) fn charge(
        &self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), QueryError> {
        self.scope
            .charge(self.context, resource, amount)
            .map_err(InternalError::from)
            .map_err(QueryError::execute)?;
        let charges = self.charges_since_watermark.get() + 1;
        self.charges_since_watermark.set(charges);
        if charges == 64 {
            self.check_instruction_watermark()?;
        }

        Ok(())
    }

    /// Charge the requested new backing allocation (including retained-prefix
    /// copies on growth) before reserving it. This is cumulative construction
    /// accounting, not an allocator/live-heap measurement.
    pub(in crate::db) fn reserve_vec<T>(
        &self,
        values: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), QueryError> {
        let capacity =
            self.reserve_capacity(values.len(), values.capacity(), additional, size_of::<T>())?;
        if capacity > values.capacity() {
            values.reserve_exact(capacity - values.len());
        }
        Ok(())
    }

    /// Reserve text construction under the same cumulative allocation policy.
    pub(in crate::db) fn reserve_string(
        &self,
        text: &mut String,
        additional: usize,
    ) -> Result<(), QueryError> {
        let capacity = self.reserve_capacity(text.len(), text.capacity(), additional, 1)?;
        if capacity > text.capacity() {
            text.reserve_exact(capacity - text.len());
        }
        Ok(())
    }

    fn reserve_capacity(
        &self,
        len: usize,
        capacity: usize,
        additional: usize,
        element_bytes: usize,
    ) -> Result<usize, QueryError> {
        let required = len
            .checked_add(additional)
            .ok_or_else(QueryError::invariant)?;
        if required <= capacity {
            return Ok(capacity);
        }
        let next = required.max(capacity.saturating_mul(2)).max(4);
        self.charge(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            (next as u64).saturating_mul(element_bytes as u64),
        )?;
        Ok(next)
    }

    fn check_instruction_watermark(&self) -> Result<(), QueryError> {
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
            .map_err(QueryError::execute)
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
