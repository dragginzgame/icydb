use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, ExecutionTrace, PreparedLoadCursor, ResolvedScalarContinuationContext,
            pipeline::{
                contracts::{CursorPage, GroupedCursorPage, LoadExecutor},
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
use std::any::Any;

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
/// ExecutionLane
///
/// Runtime lane classification used by non-generic kernel orchestration.
///

enum ExecutionLane {
    Scalar,
    Grouped,
}

///
/// ExecCtx
///
/// Immutable kernel execution context built once from staged access inputs.
/// Keeps lane classification in one authority before kernel dispatch starts.
///

struct ExecCtx<E: EntityKind> {
    context: LoadExecutionContext,
    plan: ExecutablePlan<E>,
    mode: ExecutionMode,
}

impl<E> ExecCtx<E>
where
    E: EntityKind,
{
    // Build one kernel execution context from one staged access-state envelope.
    fn from_access_state(state: LoadAccessState<E>) -> Self {
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

        Self {
            context,
            plan,
            mode,
        }
    }
}

///
/// KernelOps
///
/// Lane-specific kernel operations built once from typed executor inputs.
/// Holds leaf call targets for scalar and grouped execution.
///

struct KernelOps {
    execute_scalar: Option<KernelOp>,
    execute_grouped: Option<KernelOp>,
}

impl KernelOps {
    // Build lane operations for scalar execution.
    const fn scalar(execute_scalar: KernelOp) -> Self {
        Self {
            execute_scalar: Some(execute_scalar),
            execute_grouped: None,
        }
    }

    // Build lane operations for grouped execution.
    const fn grouped(execute_grouped: KernelOp) -> Self {
        Self {
            execute_scalar: None,
            execute_grouped: Some(execute_grouped),
        }
    }
}

///
/// ExecutionSpec
///
/// Non-generic kernel descriptor consumed by canonical kernel orchestration.
/// Captures lane mode and pre-bound lane operations.
///

struct ExecutionSpec {
    mode: ExecutionLane,
    ops: KernelOps,
}

impl ExecutionSpec {
    // Build one scalar execution descriptor.
    const fn scalar(execute_scalar: KernelOp) -> Self {
        Self {
            mode: ExecutionLane::Scalar,
            ops: KernelOps::scalar(execute_scalar),
        }
    }

    // Build one grouped execution descriptor.
    const fn grouped(execute_grouped: KernelOp) -> Self {
        Self {
            mode: ExecutionLane::Grouped,
            ops: KernelOps::grouped(execute_grouped),
        }
    }
}

///
/// ErasedScalarPage
///
/// Type-erased scalar cursor page container for non-generic kernel dispatch.
/// Re-materialized into `CursorPage<E>` at the typed payload boundary.
///

struct ErasedScalarPage {
    page: Box<dyn Any>,
}

impl ErasedScalarPage {
    // Erase one typed scalar cursor page.
    fn new<E>(page: CursorPage<E>) -> Self
    where
        E: EntityKind,
    {
        Self {
            page: Box::new(page),
        }
    }

    // Recover one typed scalar cursor page and classify type drift.
    fn into_typed<E>(self) -> Result<CursorPage<E>, InternalError>
    where
        E: EntityKind,
    {
        self.page
            .downcast::<CursorPage<E>>()
            .map(|page| *page)
            .map_err(|_| {
                crate::db::error::query_executor_invariant(
                    "kernel scalar payload type must match load executor entity type",
                )
            })
    }
}

///
/// KernelPayload
///
/// Type-erased payload envelope emitted by non-generic kernel orchestration.
///

enum KernelPayload {
    Scalar(ErasedScalarPage),
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
/// Erased lane operation descriptor backed by one function pointer and one
/// boxed typed data payload. Keeps orchestration non-generic without closure
/// captures.
///

struct KernelOp {
    func: fn(&mut dyn Any) -> Result<KernelDispatchOutput, InternalError>,
    data: Box<dyn Any>,
}

impl KernelOp {
    // Build one erased scalar operation descriptor.
    fn scalar<E>(data: ScalarKernelOpData<E>) -> Self
    where
        E: EntityKind + EntityValue,
    {
        Self {
            func: execute_scalar_kernel_operation::<E>,
            data: Box::new(data),
        }
    }

    // Build one erased grouped operation descriptor.
    fn grouped<E>(data: GroupedKernelOpData<E>) -> Self
    where
        E: EntityKind + EntityValue,
    {
        Self {
            func: execute_grouped_kernel_operation::<E>,
            data: Box::new(data),
        }
    }

    // Execute one erased operation with its typed data payload.
    fn run(mut self) -> Result<KernelDispatchOutput, InternalError> {
        (self.func)(self.data.as_mut())
    }
}

///
/// ScalarKernelOpData
///
/// Typed scalar operation payload bound once at descriptor construction.
/// Stores all entity-specific data consumed by scalar leaf execution.
///

struct ScalarKernelOpData<E>
where
    E: EntityKind + EntityValue,
{
    executor: *const LoadExecutor<E>,
    plan: Option<ExecutablePlan<E>>,
    resolved_continuation: Option<ResolvedScalarContinuationContext>,
    scalar_rows_mode: bool,
}

///
/// GroupedKernelOpData
///
/// Typed grouped operation payload bound once at descriptor construction.
/// Stores all entity-specific data consumed by grouped leaf execution.
///

struct GroupedKernelOpData<E>
where
    E: EntityKind + EntityValue,
{
    executor: *const LoadExecutor<E>,
    plan: Option<ExecutablePlan<E>>,
    cursor: Option<GroupedPlannedCursor>,
}

// Execute one typed scalar leaf operation from erased operation data.
fn execute_scalar_kernel_operation<E>(
    data: &mut dyn Any,
) -> Result<KernelDispatchOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    let Some(data) = data.downcast_mut::<ScalarKernelOpData<E>>() else {
        return Err(crate::db::error::query_executor_invariant(
            "scalar kernel operation data must match executor entity type",
        ));
    };
    let Some(plan) = data.plan.take() else {
        return Err(crate::db::error::query_executor_invariant(
            "scalar kernel operation plan must be present before dispatch",
        ));
    };
    let Some(resolved_continuation) = data.resolved_continuation.take() else {
        return Err(crate::db::error::query_executor_invariant(
            "scalar kernel continuation must be present before dispatch",
        ));
    };
    // SAFETY: the pointer is created from `&self` in `build_execution_spec` and
    // this operation executes synchronously before `apply_grouping_projection`
    // returns, so the referenced executor outlives this dereference.
    let executor = unsafe { &*data.executor };
    let (page, trace) =
        executor.execute_scalar_path(plan, resolved_continuation, data.scalar_rows_mode)?;

    Ok(KernelDispatchOutput {
        payload: KernelPayload::Scalar(ErasedScalarPage::new(page)),
        trace,
    })
}

// Execute one typed grouped leaf operation from erased operation data.
fn execute_grouped_kernel_operation<E>(
    data: &mut dyn Any,
) -> Result<KernelDispatchOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    let Some(data) = data.downcast_mut::<GroupedKernelOpData<E>>() else {
        return Err(crate::db::error::query_executor_invariant(
            "grouped kernel operation data must match executor entity type",
        ));
    };
    let Some(plan) = data.plan.take() else {
        return Err(crate::db::error::query_executor_invariant(
            "grouped kernel operation plan must be present before dispatch",
        ));
    };
    let Some(cursor) = data.cursor.take() else {
        return Err(crate::db::error::query_executor_invariant(
            "grouped kernel cursor must be present before dispatch",
        ));
    };
    // SAFETY: the pointer is created from `&self` in `build_execution_spec` and
    // this operation executes synchronously before `apply_grouping_projection`
    // returns, so the referenced executor outlives this dereference.
    let executor = unsafe { &*data.executor };
    let (page, trace) = executor.execute_grouped_path(plan, cursor)?;

    Ok(KernelDispatchOutput {
        payload: KernelPayload::Grouped(page),
        trace,
    })
}

// Execute one canonical kernel dispatch over one runtime execution mode.
fn execute_kernel(
    context: LoadExecutionContext,
    spec: ExecutionSpec,
) -> Result<KernelState, InternalError> {
    let ExecutionSpec { mode, ops } = spec;
    let KernelOps {
        execute_scalar,
        execute_grouped,
    } = ops;

    // Dispatch by lane mode and enforce descriptor integrity.
    let output = match mode {
        ExecutionLane::Scalar => {
            let Some(execute_scalar) = execute_scalar else {
                return Err(crate::db::error::query_executor_invariant(
                    "scalar execution mode requires a scalar kernel operation",
                ));
            };
            execute_scalar.run()?
        }
        ExecutionLane::Grouped => {
            let Some(execute_grouped) = execute_grouped else {
                return Err(crate::db::error::query_executor_invariant(
                    "grouped execution mode requires a grouped kernel operation",
                ));
            };
            execute_grouped.run()?
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
        &self,
        state: LoadAccessState<E>,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        let ctx = ExecCtx::from_access_state(state);
        let ExecCtx {
            context,
            plan,
            mode,
        } = ctx;
        let scalar_rows_mode = context.mode.scalar_rows_mode();
        let spec = self.build_execution_spec(plan, mode, scalar_rows_mode);
        let kernel_state = execute_kernel(context, spec)?;

        Self::materialize_payload_state(kernel_state)
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
                let execute_scalar = KernelOp::scalar(ScalarKernelOpData {
                    executor: self,
                    plan: Some(plan),
                    resolved_continuation: Some(resolved_continuation),
                    scalar_rows_mode,
                });
                ExecutionSpec::scalar(execute_scalar)
            }
            ExecutionMode::Grouped(cursor) => {
                let execute_grouped = KernelOp::grouped(GroupedKernelOpData {
                    executor: self,
                    plan: Some(plan),
                    cursor: Some(cursor),
                });
                ExecutionSpec::grouped(execute_grouped)
            }
        }
    }

    // Re-materialize one typed payload state from one non-generic kernel state.
    fn materialize_payload_state(
        kernel_state: KernelState,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        let KernelState {
            context,
            payload,
            trace,
        } = kernel_state;
        let payload = match payload {
            KernelPayload::Scalar(page) => LoadExecutionPayload::Scalar(page.into_typed::<E>()?),
            KernelPayload::Grouped(page) => LoadExecutionPayload::Grouped(page),
        };

        Ok(LoadPayloadState {
            context,
            payload,
            trace,
        })
    }
}
