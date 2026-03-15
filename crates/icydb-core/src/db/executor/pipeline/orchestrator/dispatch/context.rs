use crate::{
    db::executor::pipeline::{
        contracts::LoadExecutor, orchestrator::state::LoadPipelineState, stages::LoadPipelineStage,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one context-phase stage transition over typed stage-state artifacts.
    pub(super) fn execute_context_stage(
        &self,
        stage_descriptor: LoadPipelineStage,
        stage_state: LoadPipelineState<E>,
    ) -> Result<LoadPipelineState<E>, InternalError> {
        match stage_descriptor {
            LoadPipelineStage::BuildExecutionContext => {
                let (plan, cursor, execution_mode) = stage_state.expect_inputs(
                    "build_execution_context stage requires pre-context input artifacts",
                )?;
                let next = Self::build_execution_context(plan, cursor, execution_mode)?;

                Ok(LoadPipelineState::from_access(next))
            }
            LoadPipelineStage::ExecuteAccessPath => {
                let access_state = stage_state
                    .expect_access("execute_access_path stage requires access-state artifacts")?;
                let next = Self::execute_access_path(access_state);

                Ok(LoadPipelineState::from_access(next))
            }
            LoadPipelineStage::ApplyGroupingProjection => {
                let access_state = stage_state.expect_access(
                    "apply_grouping_projection stage requires access-state artifacts",
                )?;
                let next = self.apply_grouping_projection(access_state)?;

                Ok(LoadPipelineState::from_payload(next))
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "context stage dispatcher received terminal stage descriptor",
            )),
        }
    }
}
