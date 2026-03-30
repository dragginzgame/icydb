//! Module: db::executor::pipeline::orchestrator::strategy::context
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::orchestrator::strategy::context.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        LoadCursorInput, PreparedLoadPlan,
        pipeline::{
            contracts::LoadExecutor,
            orchestrator::{
                LoadExecutionMode,
                state::{LoadAccessInputs, LoadAccessState, LoadExecutionContext},
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
    // Build one canonical execution context from mode + plan + cursor inputs.
    pub(in crate::db::executor::pipeline::orchestrator) fn build_execution_context(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadAccessState, InternalError> {
        if !plan.mode().is_load() {
            return Err(InternalError::load_executor_load_plan_required());
        }

        let resolved_cursor = Self::resolve_entrypoint_cursor(&plan, cursor, execution_mode)?;
        let execution_spec =
            self.build_execution_spec(plan, resolved_cursor.into_cursor(), false)?;
        Ok(LoadAccessState {
            context: LoadExecutionContext::new(execution_mode),
            access_inputs: LoadAccessInputs { execution_spec },
        })
    }
}
