use crate::{
    db::executor::{
        PreparedLoadCursor,
        pipeline::{
            contracts::LoadExecutor,
            orchestrator::state::{
                LoadAccessInputs, LoadAccessState, LoadExecutionPayload, LoadPayloadState,
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
    // Apply grouping/projection contracts over staged payload artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) fn apply_grouping_projection(
        &self,
        state: LoadAccessState<E>,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        let LoadAccessState {
            context,
            access_inputs,
        } = state;
        let scalar_rows_mode = context.mode.scalar_rows_mode();
        let LoadAccessInputs { plan, cursor } = access_inputs;
        let (payload, trace) = match cursor {
            PreparedLoadCursor::Scalar(resolved_continuation) => {
                let (page, trace) =
                    self.execute_scalar_path(plan, *resolved_continuation, scalar_rows_mode)?;
                (LoadExecutionPayload::Scalar(page), trace)
            }
            PreparedLoadCursor::Grouped(cursor) => {
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
