//! Module: executor::pipeline::orchestrator
//! Responsibility: load entrypoint runtime wiring and contract-boundary exports.
//! Does not own: row materialization mechanics or continuation cursor resolution internals.
//! Boundary: executes the canonical structural load surface path and exposes the
//! stable load contracts needed by entrypoints and runtime leaves.

mod contracts;
mod guards;
mod strategy;

use crate::{
    db::executor::{LoadCursorInput, PreparedLoadPlan, pipeline::contracts::LoadExecutor},
    entity::{EntityKind, EntityValue},
    error::InternalError,
    metrics::sink::{ExecKind, record_exec_error_for_path},
};
pub(in crate::db::executor) use contracts::{LoadExecutionSurface, LoadSurfaceMode};

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
            let prepared_runtime =
                self.prepare_load_surface_runtime(plan, cursor, execution_mode)?;
            prepared_runtime.execute()
        })();
        if let Err(err) = &result {
            record_exec_error_for_path(ExecKind::Load, E::PATH, err);
        }

        result
    }
}
