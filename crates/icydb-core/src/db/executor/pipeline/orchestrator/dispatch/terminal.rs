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
    // Execute one terminal-phase stage transition over typed stage-state artifacts.
    #[expect(clippy::unused_self)]
    pub(super) fn execute_terminal_stage(
        &self,
        stage_descriptor: LoadPipelineStage,
        stage_state: LoadPipelineState<E>,
    ) -> Result<LoadPipelineState<E>, InternalError> {
        match stage_descriptor {
            LoadPipelineStage::ApplyPaging => {
                let payload_state = stage_state
                    .expect_payload("apply_paging stage requires payload-state artifacts")?;
                let next = Self::apply_paging(payload_state)?;

                Ok(LoadPipelineState::from_payload(next))
            }
            LoadPipelineStage::ApplyTracing => {
                let payload_state = stage_state
                    .expect_payload("apply_tracing stage requires payload-state artifacts")?;
                let next = Self::apply_tracing(payload_state);

                Ok(LoadPipelineState::from_payload(next))
            }
            LoadPipelineStage::MaterializeSurface => {
                let payload_state = stage_state
                    .expect_payload("materialize_surface stage requires payload-state artifacts")?;
                let next = Self::materialize_surface(payload_state)?;

                Ok(LoadPipelineState::from_surface(next))
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "terminal stage dispatcher received context stage descriptor",
            )),
        }
    }
}
