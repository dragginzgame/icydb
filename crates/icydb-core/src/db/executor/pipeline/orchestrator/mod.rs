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
    metrics::sink::{ExecKind, record_exec_error_for_path},
    traits::{EntityKind, EntityValue},
};
pub(in crate::db::executor) use contracts::{
    LoadExecutionSurface, LoadSurfaceMode, LoadTracingMode,
};
pub(in crate::db::executor::pipeline) use state::{
    LoadExecutionContext, LoadExecutionPayload, LoadPayloadState,
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
        let result = (|| {
            let access_state = self.build_execution_context(plan, cursor, execution_mode)?;
            let payload_state = Self::apply_grouping_projection(access_state)?;
            let payload_state = payload_state.apply_paging()?;
            let payload_state = payload_state.apply_tracing();

            payload_state.into_surface()
        })();
        if let Err(err) = &result {
            record_exec_error_for_path(ExecKind::Load, E::PATH, err);
        }

        result
    }
}
