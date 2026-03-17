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
            contracts::LoadExecutor,
            orchestrator::{contracts::LoadExecutionDescriptor, state::LoadPipelineState},
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
    // B1 dynamic load entrypoint:
    // consumes one immutable descriptor that owns stage-loop authority.
    // Existing typed entrypoints delegate here so subsequent slices can
    // migrate runtime internals without changing public call sites.
    pub(in crate::db::executor) fn execute_load_dyn(
        &self,
        descriptor: LoadExecutionDescriptor,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        let mut state = LoadPipelineState::Inputs {
            plan,
            cursor,
            execution_mode,
        };

        for stage in descriptor.stage_plan() {
            state = self.execute_load_stage(*stage, state)?;
        }

        state.into_surface()
    }

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
        let descriptor = LoadExecutionDescriptor::canonical();

        self.execute_load_dyn(descriptor, plan, cursor, execution_mode)
    }
}
