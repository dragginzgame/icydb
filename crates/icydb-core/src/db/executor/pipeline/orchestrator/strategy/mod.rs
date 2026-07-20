//! Module: executor::pipeline::orchestrator::strategy
//! Responsibility: strategy seams for pre-access and grouping/projection execution.
//! Does not own: stage dispatch mechanics or terminal payload materialization.
//! Boundary: exposes strategy helpers consumed by orchestrator stage dispatch.

use crate::{
    db::executor::{
        LoadCursorInput, LoadCursorResolver, PreparedLoadCursor, PreparedLoadPlan,
        ScalarContinuationContext,
        pipeline::{
            contracts::LoadExecutor, entrypoints::PreparedLoadRouteRuntime,
            orchestrator::LoadSurfaceMode,
        },
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Prepare one canonical route runtime from mode, plan, and cursor inputs.
    pub(in crate::db::executor::pipeline::orchestrator) fn prepare_load_surface_runtime(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
        execution_mode: LoadSurfaceMode,
    ) -> Result<PreparedLoadRouteRuntime, InternalError> {
        if !plan.mode().is_load() {
            return Err(InternalError::load_executor_load_plan_required());
        }

        match execution_mode {
            LoadSurfaceMode::ScalarPage => {
                let resolved_cursor = LoadCursorResolver::resolve_load_cursor_context(
                    &plan,
                    cursor,
                    LoadSurfaceMode::ScalarPage,
                )?;
                let PreparedLoadCursor::Scalar(resolved_continuation) = resolved_cursor else {
                    return Err(InternalError::query_executor_invariant());
                };

                self.build_scalar_prepared_route_runtime(plan, *resolved_continuation, false)
            }
            LoadSurfaceMode::GroupedPage => self
                .prepare_grouped_load_route_runtime(plan, cursor)
                .map(PreparedLoadRouteRuntime::Grouped),
        }
    }

    // Build one scalar prepared route runtime from one prepared scalar cursor
    // while keeping scalar runtime assembly under one local owner.
    fn build_scalar_prepared_route_runtime(
        &self,
        plan: PreparedLoadPlan,
        resolved_continuation: ScalarContinuationContext,
        scalar_rows_mode: bool,
    ) -> Result<PreparedLoadRouteRuntime, InternalError> {
        let prepared =
            self.prepare_scalar_route_runtime(plan, resolved_continuation, scalar_rows_mode)?;

        Ok(PreparedLoadRouteRuntime::Scalar(prepared))
    }
}
