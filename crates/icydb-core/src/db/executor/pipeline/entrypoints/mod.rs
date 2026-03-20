//! Module: executor::pipeline::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

mod grouped;
mod scalar;

use crate::{
    db::{
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            ContinuationEngine, ExecutablePlan, ExecutionTrace, LoadCursorInput, PreparedLoadPlan,
            ResolvedLoadCursorContext,
            pipeline::contracts::{CursorPage, GroupedCursorPage, LoadExecutor},
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::{
    LoadExecutionMode, LoadTracingMode,
};
pub(in crate::db::executor) use grouped::{
    PreparedGroupedRouteRuntime, execute_prepared_grouped_route_runtime,
};
pub(in crate::db::executor) use scalar::{
    PreparedScalarMaterializedBoundary, PreparedScalarRouteRuntime,
    execute_prepared_scalar_route_runtime,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Keep continuation-resolution authority in the entrypoint root module.
    // Leaf modules consume prepared cursor contracts only.
    pub(in crate::db::executor::pipeline) fn resolve_entrypoint_cursor(
        plan: &PreparedLoadPlan,
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
        self.execute_load_scalar_rows(
            plan.into_prepared_load_plan(),
            LoadCursorInput::scalar(PlannedCursor::none()),
        )
    }

    // Execute one scalar load plan and optionally emit execution trace output.
    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        self.execute_load_scalar_page_with_trace(
            plan.into_prepared_load_plan(),
            LoadCursorInput::scalar(cursor),
        )
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        self.execute_load_grouped_page_with_trace(
            plan.into_prepared_load_plan(),
            LoadCursorInput::grouped(cursor),
        )
    }
}
