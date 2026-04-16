//! Module: executor::pipeline::orchestrator::strategy
//! Responsibility: strategy seams for pre-access and grouping/projection execution.
//! Does not own: stage dispatch mechanics or terminal payload materialization.
//! Boundary: exposes strategy helpers consumed by orchestrator stage dispatch.

use crate::{
    db::executor::{
        ExecutionTrace, LoadCursorInput, LoadCursorResolver, PreparedLoadCursor, PreparedLoadPlan,
        ScalarContinuationContext,
        pipeline::{
            contracts::LoadExecutor,
            entrypoints::{
                PreparedGroupedRouteRuntime, PreparedScalarRouteRuntime,
                execute_prepared_grouped_route_runtime, execute_prepared_scalar_route_runtime,
            },
            grouped_runtime::resolve_grouped_route_for_plan,
            orchestrator::{
                LoadSurfaceMode,
                state::{
                    LoadAccessInputs, LoadAccessState, LoadExecutionContext, LoadExecutionPayload,
                    LoadPayloadState,
                },
            },
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionSpec
///
/// Non-generic kernel descriptor consumed by canonical kernel orchestration.
/// Captures one pre-bound scalar or grouped lane without an extra boxed
/// trait-object shell around the already-concrete prepared runtime.
///
#[expect(
    clippy::large_enum_variant,
    reason = "prepared runtimes stay inline to avoid reworking orchestrator ownership during this cleanup pass"
)]
pub(in crate::db::executor::pipeline::orchestrator) enum ExecutionSpec {
    Scalar(PreparedScalarRouteRuntime),
    Grouped(PreparedGroupedRouteRuntime),
}

impl ExecutionSpec {
    // Build one scalar execution descriptor.
    const fn scalar(prepared: PreparedScalarRouteRuntime) -> Self {
        Self::Scalar(prepared)
    }

    // Build one grouped execution descriptor.
    const fn grouped(prepared: PreparedGroupedRouteRuntime) -> Self {
        Self::Grouped(prepared)
    }

    // Execute one variant-owned prepared runtime and return the payload plus
    // trace pair before the shared outer execution state is rebuilt.
    fn execute_payload(
        self,
    ) -> Result<(LoadExecutionPayload, Option<ExecutionTrace>), InternalError> {
        match self {
            Self::Scalar(prepared) => Self::execute_scalar(prepared),
            Self::Grouped(prepared) => Self::execute_grouped(prepared),
        }
    }

    // Execute one prepared scalar runtime and wrap the resulting page in the
    // scalar payload family.
    fn execute_scalar(
        prepared: PreparedScalarRouteRuntime,
    ) -> Result<(LoadExecutionPayload, Option<ExecutionTrace>), InternalError> {
        let (page, trace) = execute_prepared_scalar_route_runtime(prepared)?;

        Ok((LoadExecutionPayload::Scalar(page), trace))
    }

    // Execute one prepared grouped runtime and wrap the resulting page in the
    // grouped payload family.
    fn execute_grouped(
        prepared: PreparedGroupedRouteRuntime,
    ) -> Result<(LoadExecutionPayload, Option<ExecutionTrace>), InternalError> {
        let (page, trace) = execute_prepared_grouped_route_runtime(prepared)?;

        Ok((LoadExecutionPayload::Grouped(page), trace))
    }

    // Execute one canonical kernel dispatch over one pre-bound scalar or
    // grouped runtime descriptor.
    fn execute(self, context: LoadExecutionContext) -> Result<LoadPayloadState, InternalError> {
        let (payload, trace) = self.execute_payload()?;

        Ok(LoadPayloadState {
            context,
            payload,
            trace,
        })
    }
}

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
        let execution_spec = self.build_execution_spec(plan, resolved_cursor, false)?;

        Ok(LoadAccessState {
            context: LoadExecutionContext::new(execution_mode),
            access_inputs: LoadAccessInputs { execution_spec },
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
        let LoadAccessInputs { execution_spec } = access_inputs;

        execution_spec.execute(context)
    }

    // Build one non-generic kernel descriptor from one typed execution context.
    pub(in crate::db::executor::pipeline::orchestrator) fn build_execution_spec(
        &self,
        plan: PreparedLoadPlan,
        cursor: PreparedLoadCursor,
        scalar_rows_mode: bool,
    ) -> Result<ExecutionSpec, InternalError> {
        match cursor {
            PreparedLoadCursor::Scalar(resolved_continuation) => {
                self.build_scalar_execution_spec(plan, *resolved_continuation, scalar_rows_mode)
            }
            PreparedLoadCursor::Grouped(cursor) => self.build_grouped_execution_spec(plan, cursor),
        }
    }

    // Build one scalar execution descriptor from one prepared scalar cursor
    // while keeping scalar runtime assembly under one local owner.
    fn build_scalar_execution_spec(
        &self,
        plan: PreparedLoadPlan,
        resolved_continuation: ScalarContinuationContext,
        scalar_rows_mode: bool,
    ) -> Result<ExecutionSpec, InternalError> {
        let prepared =
            self.prepare_scalar_route_runtime(plan, resolved_continuation, scalar_rows_mode)?;

        Ok(ExecutionSpec::scalar(prepared))
    }

    // Build one grouped execution descriptor from one prepared grouped cursor
    // while keeping grouped route/runtime assembly under one local owner.
    fn build_grouped_execution_spec(
        &self,
        plan: PreparedLoadPlan,
        cursor: crate::db::cursor::GroupedPlannedCursor,
    ) -> Result<ExecutionSpec, InternalError> {
        let prepared_execution_preparation = plan.cloned_grouped_execution_preparation();
        let prepared_grouped_slot_layout = plan.cloned_grouped_slot_layout();
        let route = resolve_grouped_route_for_plan(plan, cursor, self.debug)?;
        let prepared = self.prepare_grouped_route_runtime(
            route,
            prepared_execution_preparation,
            prepared_grouped_slot_layout,
        )?;

        Ok(ExecutionSpec::grouped(prepared))
    }
}
