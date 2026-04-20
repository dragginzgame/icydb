//! Module: db::executor::pipeline::operators::post_access::coordinator::runtime::phases
//! Defines the coordinator runtime phases for post-access execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::pipeline::operators::post_access::terminal::{
    apply_delete_window_phase as apply_post_access_delete_window_phase,
    apply_order_phase as apply_post_access_order_phase,
};
use crate::{
    db::{
        executor::OrderReadableRow,
        executor::pipeline::operators::post_access::{
            contracts::PostAccessStats, coordinator::PostAccessPlan,
        },
        query::plan::EffectiveRuntimeFilterProgram,
    },
    error::InternalError,
};

impl<K> PostAccessPlan<'_, K> {
    /// Apply delete post-access phases (filter, order, delete-limit) without
    /// load-only cursor/page orchestration.
    pub(in crate::db::executor::pipeline::operators::post_access) fn apply_delete_post_access_with_filter_program<
        R,
    >(
        &self,
        rows: &mut Vec<R>,
        filter_program: Option<&EffectiveRuntimeFilterProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        R: OrderReadableRow,
    {
        self.apply_delete_post_access_with_filter_program_internal(rows, filter_program)
    }

    fn apply_delete_post_access_with_filter_program_internal<R>(
        &self,
        rows: &mut Vec<R>,
        filter_program: Option<&EffectiveRuntimeFilterProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        R: OrderReadableRow,
    {
        let cursor = None;
        self.validate_cursor_mode(cursor)?;

        // Phase 1: apply residual filter semantics directly against the owned row buffer.
        let (filtered, _) = self.apply_filter_phase::<R>(rows, filter_program, false)?;

        // Phase 2: apply ordering directly against the same row buffer.
        let (ordered, rows_after_order) = apply_post_access_order_phase(
            self.contract.plan(),
            self.contract.has_filter(),
            rows,
            cursor,
            filtered,
        )?;

        // Phase 3: apply the ordered delete window directly against the same row buffer.
        let (delete_was_limited, _) = apply_post_access_delete_window_phase(
            self.contract.mode(),
            self.contract.order_spec(),
            self.contract.delete_limit_spec(),
            rows,
            ordered,
        )?;

        Ok(PostAccessStats {
            delete_was_limited,
            rows_after_cursor: rows_after_order,
        })
    }
}
