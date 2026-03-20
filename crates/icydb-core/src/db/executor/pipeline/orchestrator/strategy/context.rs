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
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadAccessState, InternalError> {
        execution_mode.validate()?;
        if !plan.mode().is_load() {
            return Err(crate::db::error::query_executor_invariant(
                "load executor requires load plans",
            ));
        }

        let resolved_cursor = Self::resolve_entrypoint_cursor(&plan, cursor, execution_mode)?;
        let execution_spec = self.build_execution_spec(
            plan,
            resolved_cursor.into_cursor(),
            execution_mode.scalar_rows_mode(),
        )?;
        Ok(LoadAccessState {
            context: LoadExecutionContext::new(execution_mode),
            access_inputs: LoadAccessInputs { execution_spec },
        })
    }
}
