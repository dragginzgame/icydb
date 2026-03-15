use crate::{
    db::{
        cursor::CursorBoundary,
        executor::pipeline::operators::post_access::{
            contracts::PlanRow, coordinator::PostAccessPlan,
        },
        predicate::PredicateProgram,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<K> PostAccessPlan<'_, K> {
    // Enforce load/delete cursor compatibility before execution phases.
    pub(in crate::db::executor::pipeline::operators::post_access::coordinator::runtime) fn validate_cursor_mode(
        &self,
        cursor: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if cursor.is_some() && !self.mode().is_load() {
            return Err(crate::db::error::query_invalid_logical_plan(
                "delete plans must not carry cursor boundaries",
            ));
        }

        Ok(())
    }

    // Predicate phase (already normalized and validated during planning).
    pub(in crate::db::executor::pipeline::operators::post_access::coordinator::runtime) fn apply_filter_phase<
        E,
        R,
    >(
        &self,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<(bool, usize), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        let filtered = if self.has_predicate() {
            let Some(compiled_predicate) = compiled_predicate else {
                return Err(crate::db::error::query_executor_invariant(
                    "post-access filtering requires precompiled predicate slots",
                ));
            };

            rows.retain(|row| compiled_predicate.eval(row.entity()));
            true
        } else {
            false
        };

        Ok((filtered, rows.len()))
    }
}
