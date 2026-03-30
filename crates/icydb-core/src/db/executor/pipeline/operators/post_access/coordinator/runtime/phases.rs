//! Module: db::executor::pipeline::operators::post_access::coordinator::runtime::phases
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::post_access::coordinator::runtime::phases.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::pipeline::operators::post_access::terminal::{
    apply_delete_limit_phase as apply_post_access_delete_limit_phase,
    apply_order_phase as apply_post_access_order_phase,
};
use crate::{
    db::{
        executor::OrderReadableRow,
        executor::pipeline::operators::post_access::{
            contracts::PostAccessStats, coordinator::PostAccessPlan,
        },
        predicate::PredicateProgram,
    },
    error::InternalError,
    model::entity::EntityModel,
};
use std::cell::RefCell;

impl<K> PostAccessPlan<'_, K> {
    /// Apply delete post-access phases (predicate, order, delete-limit) without
    /// load-only cursor/page orchestration.
    pub(in crate::db::executor::pipeline::operators::post_access) fn apply_delete_post_access_with_compiled_predicate<
        R,
    >(
        &self,
        model: &'static EntityModel,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        R: OrderReadableRow,
    {
        self.apply_delete_post_access_with_compiled_predicate_internal(
            model,
            rows,
            compiled_predicate,
        )
    }

    fn apply_delete_post_access_with_compiled_predicate_internal<R>(
        &self,
        model: &'static EntityModel,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        R: OrderReadableRow,
    {
        let cursor = None;
        self.validate_cursor_mode(cursor)?;
        let rows = RefCell::new(rows);

        // Phase 1: bind delete row operators once and delegate execution order
        // to the shared delete-only post-access control-flow helper.
        let mut apply_filter_phase = || {
            let rows = &mut **rows.borrow_mut();
            self.apply_filter_phase::<R>(rows, compiled_predicate, false)
        };
        let mut apply_order_phase = |filtered| {
            let rows = &mut **rows.borrow_mut();
            apply_post_access_order_phase(
                model,
                self.contract.plan(),
                self.contract.order_spec(),
                self.contract.has_predicate(),
                rows,
                cursor,
                filtered,
            )
        };
        let mut apply_delete_limit_phase = |ordered| {
            let rows = &mut **rows.borrow_mut();
            apply_post_access_delete_limit_phase(
                self.contract.mode(),
                self.contract.order_spec(),
                self.contract.delete_limit_spec(),
                rows,
                ordered,
            )
        };

        apply_delete_post_access_kernel_dyn(
            &mut apply_filter_phase,
            &mut apply_order_phase,
            &mut apply_delete_limit_phase,
        )
    }
}

// Shared delete-only post-access phase control flow. Keeps delete execution off
// load-only cursor/page phase wiring.
fn apply_delete_post_access_kernel_dyn(
    apply_filter_phase: &mut dyn FnMut() -> Result<(bool, usize), InternalError>,
    apply_order_phase: &mut dyn FnMut(bool) -> Result<(bool, usize), InternalError>,
    apply_delete_limit_phase: &mut dyn FnMut(bool) -> Result<(bool, usize), InternalError>,
) -> Result<PostAccessStats, InternalError> {
    // Phase 1: predicate filtering.
    let (filtered, _) = apply_filter_phase()?;

    // Phase 2: ordering.
    let (ordered, rows_after_order) = apply_order_phase(filtered)?;

    // Phase 3: delete limiting.
    let (delete_was_limited, _) = apply_delete_limit_phase(ordered)?;

    Ok(PostAccessStats {
        delete_was_limited,
        rows_after_cursor: rows_after_order,
    })
}
