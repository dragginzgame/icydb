//! Module: executor::pipeline::orchestrator::strategy
//! Responsibility: strategy seams for pre-access and grouping/projection execution.
//! Does not own: stage dispatch mechanics or terminal payload materialization.
//! Boundary: exposes strategy helpers consumed by orchestrator stage dispatch.

use crate::{
    db::executor::{
        LoadCursorInput, LoadCursorResolver, PreparedLoadCursor, PreparedLoadPlan,
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
}

// Execute one canonical kernel dispatch over one runtime execution descriptor.
fn execute_kernel(
    context: LoadExecutionContext,
    spec: ExecutionSpec,
) -> Result<LoadPayloadState, InternalError> {
    let (payload, trace) = match spec {
        ExecutionSpec::Scalar(prepared) => {
            let (page, trace) = execute_prepared_scalar_route_runtime(prepared)?;

            (LoadExecutionPayload::Scalar(page), trace)
        }
        ExecutionSpec::Grouped(prepared) => {
            let (page, trace) = execute_prepared_grouped_route_runtime(prepared)?;

            (LoadExecutionPayload::Grouped(page), trace)
        }
    };

    Ok(LoadPayloadState {
        context,
        payload,
        trace,
    })
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

        execute_kernel(context, execution_spec)
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
                let prepared = self.prepare_scalar_route_runtime(
                    plan,
                    *resolved_continuation,
                    scalar_rows_mode,
                )?;

                Ok(ExecutionSpec::scalar(prepared))
            }
            PreparedLoadCursor::Grouped(cursor) => {
                let route = resolve_grouped_route_for_plan(plan, cursor, self.debug)?;
                let prepared = self.prepare_grouped_route_runtime(route)?;

                Ok(ExecutionSpec::grouped(prepared))
            }
        }
    }
}
