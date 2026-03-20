use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutionTrace, PreparedLoadCursor, PreparedLoadPlan,
            ResolvedScalarContinuationContext,
            pipeline::{
                contracts::{GroupedCursorPage, LoadExecutor, StructuralCursorPage},
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
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionMode
///
/// Runtime kernel lane selector for load payload materialization.
///
enum ExecutionMode {
    Scalar(ResolvedScalarContinuationContext),
    Grouped(GroupedPlannedCursor),
}

///
/// ExecutionSpec
///
/// Non-generic kernel descriptor consumed by canonical kernel orchestration.
/// Captures one pre-bound lane operation with no typed closure capture.
///

pub(in crate::db::executor::pipeline::orchestrator) struct ExecutionSpec {
    op: Box<dyn KernelOp>,
}

impl ExecutionSpec {
    // Build one scalar execution descriptor.
    fn scalar(op: impl KernelOp + 'static) -> Self {
        Self { op: Box::new(op) }
    }

    // Build one grouped execution descriptor.
    fn grouped(op: impl KernelOp + 'static) -> Self {
        Self { op: Box::new(op) }
    }
}

///
/// KernelPayload
///
/// Type-erased payload envelope emitted by non-generic kernel orchestration.
///

enum KernelPayload {
    Scalar(StructuralCursorPage),
    Grouped(GroupedCursorPage),
}

///
/// KernelDispatchOutput
///
/// Output emitted by one lane-specific leaf kernel operation.
/// Carries payload and optional execution trace.
///

struct KernelDispatchOutput {
    payload: KernelPayload,
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
    payload: KernelPayload,
    trace: Option<ExecutionTrace>,
}

///
/// KernelOp
///
/// Monomorphic kernel operation trait used by the non-generic orchestration
/// layer. Typed entity work stays confined to the boxed leaf adapters.
///

trait KernelOp {
    // Execute one pre-bound kernel lane operation exactly once.
    fn execute(
        self: Box<Self>,
        context: &mut LoadExecutionContext,
    ) -> Result<KernelDispatchOutput, InternalError>;
}

///
/// ScalarKernelOp
///
/// Non-generic scalar kernel operation shell bound once at descriptor
/// construction. Typed executor/plan state is owned by one leaf runtime
/// adapter so the runtime op type itself stays monomorphic.
///

struct ScalarKernelOp {
    prepared: PreparedScalarRouteRuntime,
}

///
/// GroupedKernelOp
///
/// Non-generic grouped kernel operation shell bound once at descriptor
/// construction. Typed executor/plan state is owned by one leaf runtime
/// adapter so the runtime op type itself stays monomorphic.
///

struct GroupedKernelOp {
    prepared: PreparedGroupedRouteRuntime,
}

impl KernelOp for ScalarKernelOp {
    fn execute(
        self: Box<Self>,
        _context: &mut LoadExecutionContext,
    ) -> Result<KernelDispatchOutput, InternalError> {
        let Self { prepared } = *self;
        let (page, trace) = execute_prepared_scalar_route_runtime(prepared)?;

        Ok(KernelDispatchOutput {
            payload: KernelPayload::Scalar(page),
            trace,
        })
    }
}

impl KernelOp for GroupedKernelOp {
    fn execute(
        self: Box<Self>,
        _context: &mut LoadExecutionContext,
    ) -> Result<KernelDispatchOutput, InternalError> {
        let Self { prepared } = *self;
        let (page, trace) = execute_prepared_grouped_route_runtime(prepared)?;

        Ok(KernelDispatchOutput {
            payload: KernelPayload::Grouped(page),
            trace,
        })
    }
}

// Execute one canonical kernel dispatch over one runtime execution descriptor.
fn execute_kernel(
    mut context: LoadExecutionContext,
    spec: ExecutionSpec,
) -> Result<KernelState, InternalError> {
    let output = spec.op.execute(&mut context)?;

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

        Ok(Self::materialize_payload_state(kernel_state))
    }

    // Build one non-generic kernel descriptor from one typed execution context.
    pub(in crate::db::executor::pipeline::orchestrator) fn build_execution_spec(
        &self,
        plan: PreparedLoadPlan,
        cursor: PreparedLoadCursor,
        scalar_rows_mode: bool,
    ) -> Result<ExecutionSpec, InternalError> {
        let mode = match cursor {
            PreparedLoadCursor::Scalar(resolved_continuation) => {
                ExecutionMode::Scalar(*resolved_continuation)
            }
            PreparedLoadCursor::Grouped(cursor) => ExecutionMode::Grouped(cursor),
        };

        match mode {
            ExecutionMode::Scalar(resolved_continuation) => {
                let prepared = self.prepare_scalar_route_runtime(
                    plan,
                    resolved_continuation,
                    scalar_rows_mode,
                )?;
                let op = ScalarKernelOp { prepared };
                Ok(ExecutionSpec::scalar(op))
            }
            ExecutionMode::Grouped(cursor) => {
                let route = Self::resolve_grouped_route(plan, cursor, self.debug)?;
                let prepared = self.prepare_grouped_route_runtime(route)?;
                let op = GroupedKernelOp { prepared };

                Ok(ExecutionSpec::grouped(op))
            }
        }
    }

    // Re-materialize one typed payload state from one non-generic kernel state.
    fn materialize_payload_state(kernel_state: KernelState) -> LoadPayloadState {
        let KernelState {
            context,
            payload,
            trace,
        } = kernel_state;
        let payload = match payload {
            KernelPayload::Scalar(page) => LoadExecutionPayload::Scalar(page),
            KernelPayload::Grouped(page) => LoadExecutionPayload::Grouped(page),
        };

        LoadPayloadState {
            context,
            payload,
            trace,
        }
    }
}
