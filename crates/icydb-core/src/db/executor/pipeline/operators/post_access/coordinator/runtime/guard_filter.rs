use crate::{
    db::{
        cursor::CursorBoundary, executor::OrderReadableRow,
        executor::pipeline::operators::post_access::coordinator::PostAccessPlan,
        predicate::PredicateProgram,
    },
    error::InternalError,
};

impl<K> PostAccessPlan<'_, K> {
    // Enforce load/delete cursor compatibility before execution phases.
    pub(in crate::db::executor::pipeline::operators::post_access::coordinator::runtime) fn validate_cursor_mode(
        &self,
        cursor: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if cursor.is_some() && !self.contract.mode().is_load() {
            return Err(InternalError::query_invalid_logical_plan(
                "delete plans must not carry cursor boundaries",
            ));
        }

        Ok(())
    }

    // Predicate phase (already normalized and validated during planning).
    pub(in crate::db::executor::pipeline::operators::post_access::coordinator::runtime) fn apply_filter_phase<
        R,
    >(
        &self,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
        predicate_preapplied: bool,
    ) -> Result<(bool, usize), InternalError>
    where
        R: OrderReadableRow,
    {
        let filtered = if self.contract.has_predicate() {
            if predicate_preapplied {
                return Ok((true, rows.len()));
            }

            let Some(compiled_predicate) = compiled_predicate else {
                return Err(InternalError::query_executor_invariant(
                    "post-access filtering requires precompiled predicate slots",
                ));
            };

            rows.retain(|row| {
                compiled_predicate.eval_with_slot_reader(&mut |slot| row.read_order_slot(slot))
            });
            true
        } else {
            false
        };

        Ok((filtered, rows.len()))
    }
}
