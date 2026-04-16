//! Module: executor::pipeline::orchestrator
//! Responsibility: load entrypoint runtime wiring and contract-boundary exports.
//! Does not own: row materialization mechanics or continuation cursor resolution internals.
//! Boundary: executes the canonical structural load surface path and exposes the
//! stable load contracts needed by entrypoints and runtime leaves.

mod contracts;
mod guards;
mod state;
mod strategy;

use crate::{
    db::executor::{LoadCursorInput, PreparedLoadPlan, pipeline::contracts::LoadExecutor},
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
pub(in crate::db::executor) use contracts::{
    LoadExecutionSurface, LoadSurfaceMode, LoadTracingMode,
};
#[cfg(test)]
pub(in crate::db::executor) use guards::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one load plan through the canonical structural load surface path.
    pub(in crate::db::executor) fn execute_load_surface(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
        execution_mode: LoadSurfaceMode,
    ) -> Result<LoadExecutionSurface, InternalError> {
        let access_state = self.build_execution_context(plan, cursor, execution_mode)?;
        let payload_state = Self::apply_grouping_projection(access_state)?;
        let payload_state = payload_state.apply_paging()?;
        let payload_state = payload_state.apply_tracing();

        payload_state.into_surface()
    }
}
