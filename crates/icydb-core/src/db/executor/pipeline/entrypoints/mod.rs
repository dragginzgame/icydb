//! Module: executor::pipeline::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

mod grouped;
mod scalar;

use crate::{
    db::{
        PersistedRow,
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput, LoadCursorResolver,
            PreparedLoadCursor, PreparedLoadPlan,
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
#[cfg(test)]
pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
#[cfg(feature = "sql")]
pub(in crate::db) use grouped::execute_initial_grouped_rows_for_canister;
pub(in crate::db::executor) use grouped::{
    PreparedGroupedRouteRuntime, execute_prepared_grouped_route_runtime,
};
#[cfg(feature = "sql")]
pub(in crate::db) use scalar::execute_initial_scalar_sql_projection_page_for_canister;
pub(in crate::db::executor) use scalar::{
    PreparedScalarMaterializedBoundary, PreparedScalarRouteRuntime,
    execute_prepared_scalar_route_runtime, execute_prepared_scalar_rows_for_canister,
};
#[cfg(feature = "sql")]
pub(in crate::db) use scalar::{
    execute_initial_scalar_sql_projection_rows_for_canister,
    execute_initial_scalar_sql_projection_text_rows_for_canister,
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
    ) -> Result<PreparedLoadCursor, InternalError> {
        LoadCursorResolver::resolve_load_cursor_context(plan, cursor, execution_mode)
    }
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Execute one scalar load plan without explicit cursor input.
    pub(crate) fn execute(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        let page = execute_prepared_scalar_rows_for_canister(
            &self.db,
            self.debug,
            plan.into_prepared_load_plan(),
        )?;

        page.into_entity_response::<E>()
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

    // Execute one scalar load plan with cursor input and discard tracing.
    #[cfg(test)]
    pub(in crate::db) fn execute_paged_with_cursor(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        let (page, _) = self.execute_paged_with_cursor_traced(plan, cursor)?;

        Ok(page)
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
