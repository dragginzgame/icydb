use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, ExecutionTrace, PreparedLoadCursor, ResolvedScalarContinuationContext,
            pipeline::{
                contracts::{GroupedCursorPage, LoadExecutor},
                orchestrator::ErasedLoadPayload,
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

struct ExecutionSpec {
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
    Scalar(ErasedLoadPayload),
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
    runtime: Box<dyn ScalarKernelRuntime>,
    resolved_continuation: ResolvedScalarContinuationContext,
    scalar_rows_mode: bool,
}

///
/// GroupedKernelOp
///
/// Non-generic grouped kernel operation shell bound once at descriptor
/// construction. Typed executor/plan state is owned by one leaf runtime
/// adapter so the runtime op type itself stays monomorphic.
///

struct GroupedKernelOp {
    runtime: Box<dyn GroupedKernelRuntime>,
    cursor: GroupedPlannedCursor,
}

///
/// ScalarKernelRuntime
///
/// Typed scalar execution leaf adapter resolved once before runtime
/// orchestration starts. This keeps entity-specific execution off the
/// non-generic kernel op type while preserving the existing scalar behavior.
///

trait ScalarKernelRuntime {
    // Execute one typed scalar leaf and return one type-erased scalar payload.
    fn execute_scalar(
        self: Box<Self>,
        resolved_continuation: ResolvedScalarContinuationContext,
        scalar_rows_mode: bool,
    ) -> Result<KernelDispatchOutput, InternalError>;
}

///
/// GroupedKernelRuntime
///
/// Typed grouped execution leaf adapter resolved once before runtime
/// orchestration starts. This keeps entity-specific execution off the
/// non-generic kernel op type while preserving the existing grouped behavior.
///

trait GroupedKernelRuntime {
    // Execute one typed grouped leaf and return one grouped payload.
    fn execute_grouped(
        self: Box<Self>,
        cursor: GroupedPlannedCursor,
    ) -> Result<KernelDispatchOutput, InternalError>;
}

///
/// ScalarKernelRuntimeAdapter
///
/// Typed scalar leaf adapter holding the exact executor and plan required for
/// scalar execution. This type is generic, but it stays behind one trait
/// object resolved before non-generic runtime orchestration begins.
///

struct ScalarKernelRuntimeAdapter<E>
where
    E: EntityKind + EntityValue,
{
    executor: LoadExecutor<E>,
    plan: ExecutablePlan<E>,
}

///
/// GroupedKernelRuntimeAdapter
///
/// Typed grouped leaf adapter holding the exact executor and plan required for
/// grouped execution. This type is generic, but it stays behind one trait
/// object resolved before non-generic runtime orchestration begins.
///

struct GroupedKernelRuntimeAdapter<E>
where
    E: EntityKind + EntityValue,
{
    executor: LoadExecutor<E>,
    plan: ExecutablePlan<E>,
}

// Split one typed access-state envelope into shared execution context and one
// lane-specific execution mode before non-generic kernel dispatch starts.
fn split_access_state<E>(
    state: LoadAccessState<E>,
) -> (LoadExecutionContext, ExecutablePlan<E>, ExecutionMode)
where
    E: EntityKind,
{
    let LoadAccessState {
        context,
        access_inputs,
    } = state;
    let LoadAccessInputs { plan, cursor } = access_inputs;
    let mode = match cursor {
        PreparedLoadCursor::Scalar(resolved_continuation) => {
            ExecutionMode::Scalar(*resolved_continuation)
        }
        PreparedLoadCursor::Grouped(cursor) => ExecutionMode::Grouped(cursor),
    };

    (context, plan, mode)
}

impl<E> ScalarKernelRuntime for ScalarKernelRuntimeAdapter<E>
where
    E: EntityKind + EntityValue,
{
    fn execute_scalar(
        self: Box<Self>,
        resolved_continuation: ResolvedScalarContinuationContext,
        scalar_rows_mode: bool,
    ) -> Result<KernelDispatchOutput, InternalError> {
        let Self { executor, plan } = *self;
        let (page, trace) =
            executor.execute_scalar_path(plan, resolved_continuation, scalar_rows_mode)?;

        Ok(KernelDispatchOutput {
            payload: KernelPayload::Scalar(ErasedLoadPayload::new(page)),
            trace,
        })
    }
}

impl<E> GroupedKernelRuntime for GroupedKernelRuntimeAdapter<E>
where
    E: EntityKind + EntityValue,
{
    fn execute_grouped(
        self: Box<Self>,
        cursor: GroupedPlannedCursor,
    ) -> Result<KernelDispatchOutput, InternalError> {
        let Self { executor, plan } = *self;
        let (page, trace) = executor.execute_grouped_path(plan, cursor)?;

        Ok(KernelDispatchOutput {
            payload: KernelPayload::Grouped(page),
            trace,
        })
    }
}

impl KernelOp for ScalarKernelOp {
    fn execute(
        self: Box<Self>,
        _context: &mut LoadExecutionContext,
    ) -> Result<KernelDispatchOutput, InternalError> {
        let Self {
            runtime,
            resolved_continuation,
            scalar_rows_mode,
        } = *self;

        runtime.execute_scalar(resolved_continuation, scalar_rows_mode)
    }
}

impl KernelOp for GroupedKernelOp {
    fn execute(
        self: Box<Self>,
        _context: &mut LoadExecutionContext,
    ) -> Result<KernelDispatchOutput, InternalError> {
        let Self { runtime, cursor } = *self;

        runtime.execute_grouped(cursor)
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
    // Clone one executor handle without carrying any typed plan/cursor state.
    fn clone_runtime_handle(&self) -> Self {
        Self {
            db: self.db.clone(),
            debug: self.debug,
        }
    }

    // Apply grouping/projection contracts over staged payload artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) fn apply_grouping_projection(
        &self,
        state: LoadAccessState<E>,
    ) -> Result<LoadPayloadState, InternalError> {
        let (context, plan, mode) = split_access_state(state);
        let scalar_rows_mode = context.mode.scalar_rows_mode();
        let spec = self.build_execution_spec(plan, mode, scalar_rows_mode);
        let kernel_state = execute_kernel(context, spec)?;

        Ok(Self::materialize_payload_state(kernel_state))
    }

    // Build one non-generic kernel descriptor from one typed execution context.
    fn build_execution_spec(
        &self,
        plan: ExecutablePlan<E>,
        mode: ExecutionMode,
        scalar_rows_mode: bool,
    ) -> ExecutionSpec {
        match mode {
            ExecutionMode::Scalar(resolved_continuation) => {
                let op = ScalarKernelOp {
                    runtime: Box::new(ScalarKernelRuntimeAdapter {
                        executor: self.clone_runtime_handle(),
                        plan,
                    }),
                    resolved_continuation,
                    scalar_rows_mode,
                };
                ExecutionSpec::scalar(op)
            }
            ExecutionMode::Grouped(cursor) => {
                let op = GroupedKernelOp {
                    runtime: Box::new(GroupedKernelRuntimeAdapter {
                        executor: self.clone_runtime_handle(),
                        plan,
                    }),
                    cursor,
                };
                ExecutionSpec::grouped(op)
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
