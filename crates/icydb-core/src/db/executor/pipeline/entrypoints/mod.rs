//! Module: executor::pipeline::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

mod grouped;
mod pipeline;
mod scalar;

use crate::{
    db::{
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            ContinuationEngine, ExecutablePlan, ExecutionTrace, LoadCursorInput,
            ResolvedLoadCursorContext,
            shared::load_contracts::{CursorPage, GroupedCursorPage, LoadExecutor},
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(in crate::db::executor) use pipeline::{
    LoadExecutionMode, LoadExecutionSurface, LoadTracingMode,
};
#[cfg(test)]
pub(in crate::db::executor) use pipeline::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Keep continuation-resolution authority in the entrypoint root module.
    // Leaf modules consume prepared cursor contracts only.
    fn resolve_entrypoint_cursor(
        plan: &ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<ResolvedLoadCursorContext, InternalError> {
        ContinuationEngine::resolve_load_cursor_context(
            plan,
            cursor,
            execution_mode.requested_shape(),
        )
    }

    // Execute one scalar load plan without explicit cursor input.
    pub(crate) fn execute(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_load_scalar_rows(plan, LoadCursorInput::scalar(PlannedCursor::none()))
    }

    // Execute one scalar load plan with optional cursor input.
    // Retained as a direct scalar pagination adapter for executor-level tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) fn execute_paged_with_cursor(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        self.execute_load_scalar_page(plan, LoadCursorInput::scalar(cursor))
    }

    // Execute one scalar load plan and optionally emit execution trace output.
    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        self.execute_load_scalar_page_with_trace(plan, LoadCursorInput::scalar(cursor))
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        self.execute_load_grouped_page_with_trace(plan, LoadCursorInput::grouped(cursor))
    }
}
