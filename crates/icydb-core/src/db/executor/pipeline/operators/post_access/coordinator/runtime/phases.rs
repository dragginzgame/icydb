use crate::db::executor::pipeline::operators::post_access::terminal::{
    apply_delete_limit_phase as apply_post_access_delete_limit_phase,
    apply_order_phase as apply_post_access_order_phase,
    apply_page_phase as apply_post_access_page_phase,
};
use crate::{
    db::{
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
use std::cell::RefCell;

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
        self.apply_post_access_with_compiled_predicate_internal::<E, R>(rows, compiled_predicate)
    }

    fn apply_post_access_with_compiled_predicate_internal<E, R>(
        &self,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        let cursor = None;
        self.validate_cursor_mode(cursor)?;
        let rows = RefCell::new(rows);

        // Phase 1: bind typed row operators once and delegate phase control flow
        // to the shared dynamic post-access kernel.
        let mut apply_filter_phase = || {
            let rows = &mut *rows.borrow_mut();
            self.apply_filter_phase::<E, R>(rows, compiled_predicate, false)
        };
        let mut apply_order_phase = |filtered| {
            let rows = &mut *rows.borrow_mut();
            apply_post_access_order_phase::<E, R, K>(
                self.contract.plan(),
                self.contract.order_spec(),
                self.contract.has_predicate(),
                rows,
                cursor,
                filtered,
            )
        };
        let mut apply_cursor_boundary_phase = |ordered, rows_after_order| {
            let rows = &mut *rows.borrow_mut();
            ExecutionKernel::apply_cursor_boundary_phase::<K, E, R>(
                self.contract.plan(),
                rows,
                cursor,
                ordered,
                rows_after_order,
            )
        };
        let mut apply_page_phase = |ordered| {
            let rows = &mut *rows.borrow_mut();
            apply_post_access_page_phase(
                self.contract.mode(),
                self.contract.order_spec(),
                self.contract.page_spec(),
                self.contract.plan(),
                rows,
                ordered,
                cursor,
            )
        };
        let mut apply_delete_limit_phase = |ordered| {
            let rows = &mut *rows.borrow_mut();
            apply_post_access_delete_limit_phase(
                self.contract.mode(),
                self.contract.order_spec(),
                self.contract.delete_limit_spec(),
                rows,
                ordered,
            )
        };

        apply_post_access_kernel_dyn(
            &mut apply_filter_phase,
            &mut apply_order_phase,
            &mut apply_cursor_boundary_phase,
            &mut apply_page_phase,
            &mut apply_delete_limit_phase,
        )
    }
}

// Shared post-access phase control flow used by scalar and delete runtime paths.
// Typed row operators are injected via callbacks so this orchestration remains
// non-generic while preserving canonical phase ordering and fail-closed checks.
fn apply_post_access_kernel_dyn(
    apply_filter_phase: &mut dyn FnMut() -> Result<(bool, usize), InternalError>,
    apply_order_phase: &mut dyn FnMut(bool) -> Result<(bool, usize), InternalError>,
    apply_cursor_boundary_phase: &mut dyn FnMut(
        bool,
        usize,
    ) -> Result<(bool, usize), InternalError>,
    apply_page_phase: &mut dyn FnMut(bool) -> Result<(bool, usize), InternalError>,
    apply_delete_limit_phase: &mut dyn FnMut(bool) -> Result<(bool, usize), InternalError>,
) -> Result<PostAccessStats, InternalError> {
    // Phase 1: predicate filtering.
    let (filtered, rows_after_filter) = apply_filter_phase()?;

    // Phase 2: ordering.
    let (ordered, rows_after_order) = apply_order_phase(filtered)?;

    // Phase 3: continuation boundary.
    let (_cursor_skipped, rows_after_cursor) =
        apply_cursor_boundary_phase(ordered, rows_after_order)?;

    // Phase 4: load pagination.
    let (paged, rows_after_page) = apply_page_phase(ordered)?;

    // Phase 5: delete limiting.
    let (delete_was_limited, rows_after_delete_limit) = apply_delete_limit_phase(ordered)?;

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
