use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, PreparedLoadCursor, ResolvedScalarContinuationContext,
            pipeline::{
                contracts::LoadExecutor,
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

        self.execute_kernel(ctx)
    }

    // Execute one canonical kernel dispatch over one runtime execution mode.
    fn execute_kernel(&self, ctx: ExecCtx<E>) -> Result<LoadPayloadState<E>, InternalError> {
        let ExecCtx {
            context,
            plan,
            mode,
        } = ctx;
        let scalar_rows_mode = context.mode.scalar_rows_mode();
        let (payload, trace) = match mode {
            ExecutionMode::Scalar(resolved_continuation) => {
                let (page, trace) =
                    self.execute_scalar_path(plan, resolved_continuation, scalar_rows_mode)?;
                (LoadExecutionPayload::Scalar(page), trace)
            }
            ExecutionMode::Grouped(cursor) => {
                let (page, trace) = self.execute_grouped_path(plan, cursor)?;
                (LoadExecutionPayload::Grouped(page), trace)
            }
        };

        Ok(LoadPayloadState {
            context,
            payload,
            trace,
        })
    }
}
