//! Module: executor::pipeline::orchestrator::dispatch
//! Responsibility: stage-to-state dispatch mapping for load orchestration.
//! Does not own: mode contracts or payload materialization mechanics.
//! Boundary: executes deterministic stage transitions over typed pipeline state.

#![allow(dead_code)]

mod context;
mod terminal;

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
    // Execute one deterministic stage descriptor over stage-local state artifacts.
    pub(super) fn execute_load_stage(
        &self,
        stage_descriptor: LoadPipelineStage,
        stage_state: LoadPipelineState<E>,
    ) -> Result<LoadPipelineState<E>, InternalError> {
        match stage_descriptor {
            LoadPipelineStage::BuildExecutionContext
            | LoadPipelineStage::ExecuteAccessPath
            | LoadPipelineStage::ApplyGroupingProjection => {
                self.execute_context_stage(stage_descriptor, stage_state)
            }
            LoadPipelineStage::ApplyPaging
            | LoadPipelineStage::ApplyTracing
            | LoadPipelineStage::MaterializeSurface => {
                self.execute_terminal_stage(stage_descriptor, stage_state)
            }
        }
    }
}
