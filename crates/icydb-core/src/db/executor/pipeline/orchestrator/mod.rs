//! Module: executor::pipeline::orchestrator
//! Responsibility: load staged orchestration wiring and contract-boundary exports.
//! Does not own: row materialization mechanics or continuation cursor resolution internals.
//! Boundary: executes staged orchestration and exposes stable load contracts.

mod contracts;
mod dispatch;
#[cfg(test)]
mod guards;
mod payload;
mod state;
mod strategy;

use crate::{
    db::executor::{
        ExecutablePlan, LoadCursorInput,
        pipeline::{
            contracts::LoadExecutor, orchestrator::state::LoadPipelineState,
            stages::plan_load_pipeline_stages,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(in crate::db::executor) use contracts::{
    LoadExecutionMode, LoadExecutionSurface, LoadTracingMode,
};
#[cfg(test)]
pub(in crate::db::executor) use guards::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Unified load entrypoint pipeline:
    // 1) build execution context
    // 2) execute access path
    // 3) apply grouping/projection contract
    // 4) apply paging contract
    // 5) apply tracing contract
    // 6) materialize response surface
    pub(in crate::db::executor) fn execute_load(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        let mut state = LoadPipelineState::Inputs {
            plan,
            cursor,
            execution_mode,
        };

        for stage in plan_load_pipeline_stages() {
            state = self.execute_load_stage(stage, state)?;
        }

        state.into_surface()
    }
}
