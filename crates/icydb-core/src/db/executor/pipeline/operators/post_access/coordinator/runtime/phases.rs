use crate::db::executor::pipeline::operators::post_access::terminal::{
    apply_delete_limit_phase as apply_post_access_delete_limit_phase,
    apply_order_phase as apply_post_access_order_phase,
    apply_page_phase as apply_post_access_page_phase,
};
use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel,
            pipeline::operators::post_access::{
                contracts::{PlanRow, PostAccessStats},
                coordinator::PostAccessPlan,
            },
        },
        predicate::PredicateProgram,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<K> PostAccessPlan<'_, K> {
    /// Apply predicate, ordering, and pagination in plan order with one precompiled predicate.
    pub(in crate::db::executor::pipeline::operators::post_access) fn apply_post_access_with_compiled_predicate<
        E,
        R,
    >(
        &self,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        self.apply_post_access_with_cursor_and_compiled_predicate::<E, R>(
            rows,
            None,
            compiled_predicate,
        )
    }

    /// Apply predicate, ordering, cursor boundary, and pagination with a precompiled predicate.
    pub(in crate::db::executor::pipeline::operators::post_access) fn apply_post_access_with_cursor_and_compiled_predicate<
        E,
        R,
    >(
        &self,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        self.validate_cursor_mode(cursor)?;

        // Phase 1: predicate filtering.
        let (filtered, rows_after_filter) =
            self.apply_filter_phase::<E, R>(rows, compiled_predicate)?;

        // Phase 2: ordering.
        let (ordered, rows_after_order) = apply_post_access_order_phase::<E, R, K>(
            self.plan,
            self.order_spec(),
            self.has_predicate(),
            rows,
            cursor,
            filtered,
        )?;

        // Phase 3: continuation boundary.
        let (_cursor_skipped, rows_after_cursor) =
            ExecutionKernel::apply_cursor_boundary_phase::<K, E, R>(
                self.plan,
                rows,
                cursor,
                ordered,
                rows_after_order,
            )?;

        // Phase 4: load pagination.
        let (paged, rows_after_page) = apply_post_access_page_phase(
            self.mode(),
            self.order_spec(),
            self.page_spec(),
            self.plan,
            rows,
            ordered,
            cursor,
        )?;

        // Phase 5: delete limiting.
        let (delete_was_limited, rows_after_delete_limit) = apply_post_access_delete_limit_phase(
            self.mode(),
            self.order_spec(),
            self.delete_limit_spec(),
            rows,
            ordered,
        )?;

        #[cfg(not(test))]
        let _ = (
            rows_after_filter,
            paged,
            rows_after_page,
            rows_after_delete_limit,
        );

        Ok(PostAccessStats {
            delete_was_limited,
            rows_after_cursor,
            #[cfg(test)]
            filtered,
            #[cfg(test)]
            ordered,
            #[cfg(test)]
            paged,
            #[cfg(test)]
            rows_after_filter,
            #[cfg(test)]
            rows_after_order,
            #[cfg(test)]
            rows_after_page,
            #[cfg(test)]
            rows_after_delete_limit,
        })
    }
}
