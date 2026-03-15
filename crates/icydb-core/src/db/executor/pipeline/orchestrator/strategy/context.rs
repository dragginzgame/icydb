use crate::{
    db::executor::{
        ExecutablePlan, LoadCursorInput,
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
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadAccessState<E>, InternalError> {
        execution_mode.validate()?;
        if !plan.mode().is_load() {
            return Err(crate::db::error::query_executor_invariant(
                "load executor requires load plans",
            ));
        }

        let resolved_cursor = Self::resolve_entrypoint_cursor(&plan, cursor, execution_mode)?;
        Ok(LoadAccessState {
            context: LoadExecutionContext::new(execution_mode),
            access_inputs: LoadAccessInputs {
                plan,
                cursor: resolved_cursor.into_cursor(),
            },
        })
    }

    // Execute one canonical access path and stage payload + trace artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) const fn execute_access_path(
        state: LoadAccessState<E>,
    ) -> LoadAccessState<E> {
        // Mechanical stage boundary: access inputs stay normalized and stage-owned.
        state
    }
}
