//! Module: db::executor::pipeline::orchestrator::strategy::grouping
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::orchestrator::strategy::grouping.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        ExecutionTrace, PreparedLoadCursor, PreparedLoadPlan,
        pipeline::{
            contracts::LoadExecutor,
            entrypoints::{
                PreparedGroupedRouteRuntime, PreparedScalarRouteRuntime,
                execute_prepared_grouped_route_runtime, execute_prepared_scalar_route_runtime,
            },
            orchestrator::state::{
                LoadAccessInputs, LoadAccessState, LoadExecutionContext, LoadExecutionPayload,
                LoadPayloadState,
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

///
/// KernelDispatchOutput
///
/// Output emitted by one lane-specific leaf kernel operation.
/// Carries payload and optional execution trace.
///

struct KernelDispatchOutput {
    payload: LoadExecutionPayload,
    trace: Option<ExecutionTrace>,
}

///
/// KernelState
///
/// Full kernel output state emitted by non-generic kernel orchestration.
/// Preserves execution context with payload and optional trace output.
///

struct KernelState {
    context: LoadExecutionContext,
    payload: LoadExecutionPayload,
    trace: Option<ExecutionTrace>,
}

// Execute one canonical kernel dispatch over one runtime execution descriptor.
fn execute_kernel(
    context: LoadExecutionContext,
    spec: ExecutionSpec,
) -> Result<KernelState, InternalError> {
    let output = match spec {
        ExecutionSpec::Scalar(prepared) => {
            let (page, trace) = execute_prepared_scalar_route_runtime(prepared)?;

            KernelDispatchOutput {
                payload: LoadExecutionPayload::Scalar(page),
                trace,
            }
        }
        ExecutionSpec::Grouped(prepared) => {
            let (page, trace) = execute_prepared_grouped_route_runtime(prepared)?;

            KernelDispatchOutput {
                payload: LoadExecutionPayload::Grouped(page),
                trace,
            }
        }
    };

    Ok(KernelState {
        context,
        payload: output.payload,
        trace: output.trace,
    })
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Apply grouping/projection contracts over staged payload artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) fn apply_grouping_projection(
        state: LoadAccessState,
    ) -> Result<LoadPayloadState, InternalError> {
        let LoadAccessState {
            context,
            access_inputs,
        } = state;
        let LoadAccessInputs { execution_spec } = access_inputs;
        let kernel_state = execute_kernel(context, execution_spec)?;
        let KernelState {
            context,
            payload,
            trace,
        } = kernel_state;

        Ok(LoadPayloadState {
            context,
            payload,
            trace,
        })
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
                let route = Self::resolve_grouped_route(plan, cursor, self.debug)?;
                let prepared = self.prepare_grouped_route_runtime(route)?;

                Ok(ExecutionSpec::grouped(prepared))
            }
        }
    }
}
