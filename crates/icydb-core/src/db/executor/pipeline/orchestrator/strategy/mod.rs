//! Module: executor::pipeline::orchestrator::strategy
//! Responsibility: strategy seams for pre-access and grouping/projection execution.
//! Does not own: stage dispatch mechanics or terminal payload materialization.
//! Boundary: exposes strategy helpers consumed by orchestrator stage dispatch.

use crate::{
    db::executor::{
        LoadCursorInput, LoadCursorResolver, PreparedLoadCursor, PreparedLoadPlan,
        ScalarContinuationContext,
        pipeline::{
            contracts::LoadExecutor,
            entrypoints::PreparedLoadRouteRuntime,
            grouped_runtime::resolve_grouped_route_for_plan,
            orchestrator::{
                LoadSurfaceMode,
                state::{
                    LoadAccessInputs, LoadAccessState, LoadExecutionContext, LoadPayloadState,
                },
            },
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Build one canonical execution context from mode + plan + cursor inputs.
    pub(in crate::db::executor::pipeline::orchestrator) fn build_execution_context(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
        execution_mode: LoadSurfaceMode,
    ) -> Result<LoadAccessState, InternalError> {
        if !plan.mode().is_load() {
            return Err(InternalError::load_executor_load_plan_required());
        }

        let resolved_cursor =
            LoadCursorResolver::resolve_load_cursor_context(&plan, cursor, execution_mode)?;
        let prepared_runtime = self.build_prepared_route_runtime(plan, resolved_cursor, false)?;

        Ok(LoadAccessState {
            context: LoadExecutionContext::new(execution_mode),
            access_inputs: LoadAccessInputs { prepared_runtime },
        })
    }

    // Apply grouping/projection contracts over staged payload artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) fn apply_grouping_projection(
        state: LoadAccessState,
    ) -> Result<LoadPayloadState, InternalError> {
        let LoadAccessState {
            context,
            access_inputs,
        } = state;
        let LoadAccessInputs { prepared_runtime } = access_inputs;

        prepared_runtime.execute(context)
    }

    // Build one canonical prepared route runtime from one typed execution context.
    pub(in crate::db::executor::pipeline::orchestrator) fn build_prepared_route_runtime(
        &self,
        plan: PreparedLoadPlan,
        cursor: PreparedLoadCursor,
        scalar_rows_mode: bool,
    ) -> Result<PreparedLoadRouteRuntime, InternalError> {
        match cursor {
            PreparedLoadCursor::Scalar(resolved_continuation) => self
                .build_scalar_prepared_route_runtime(
                    plan,
                    *resolved_continuation,
                    scalar_rows_mode,
                ),
            PreparedLoadCursor::Grouped(cursor) => {
                self.build_grouped_prepared_route_runtime(plan, cursor)
            }
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

        Ok(PreparedLoadRouteRuntime::scalar(prepared))
    }

    // Build one grouped prepared route runtime from one prepared grouped cursor
    // while keeping grouped route/runtime assembly under one local owner.
    fn build_grouped_prepared_route_runtime(
        &self,
        plan: PreparedLoadPlan,
        cursor: crate::db::cursor::GroupedPlannedCursor,
    ) -> Result<PreparedLoadRouteRuntime, InternalError> {
        let prepared_execution_preparation = plan.cloned_grouped_execution_preparation();
        let prepared_grouped_slot_layout = plan.cloned_grouped_slot_layout();
        let route = resolve_grouped_route_for_plan(plan, cursor, self.debug)?;
        let prepared = self.prepare_grouped_route_runtime(
            route,
            prepared_execution_preparation,
            prepared_grouped_slot_layout,
        )?;

        Ok(PreparedLoadRouteRuntime::grouped(prepared))
    }
}
