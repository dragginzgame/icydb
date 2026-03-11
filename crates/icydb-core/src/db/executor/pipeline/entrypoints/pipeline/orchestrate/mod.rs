//! Module: executor::pipeline::entrypoints::pipeline::orchestrate
//! Responsibility: staged orchestration for unified scalar/grouped load execution.
//! Does not own: load mode contract definitions or route/access planning semantics.
//! Boundary: executes the canonical six-stage load orchestration pipeline.

#[cfg(test)]
mod guards;
mod payload;
mod state;

use crate::{
    db::executor::{
        ExecutablePlan, LoadCursorInput, PreparedLoadCursor,
        pipeline::entrypoints::pipeline::{LoadExecutionMode, LoadExecutionSurface, LoadMode},
        shared::load_contracts::LoadExecutor,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::pipeline::entrypoints::pipeline::orchestrate::state::{
    LoadAccessInputs, LoadAccessState, LoadExecutionContext, LoadExecutionPayload, LoadPayloadState,
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
        let state = Self::build_execution_context(plan, cursor, execution_mode)?;
        let state = Self::execute_access_path(state);
        let state = self.apply_grouping_projection(state)?;
        let state = Self::apply_paging(state)?;
        let state = Self::apply_tracing(state);

        Self::materialize_surface(state)
    }

    // Build one canonical execution context from mode + plan + cursor inputs.
    fn build_execution_context(
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
    const fn execute_access_path(state: LoadAccessState<E>) -> LoadAccessState<E> {
        // Mechanical stage boundary: access inputs stay normalized and stage-owned.
        state
    }

    // Apply grouping/projection contracts over staged payload artifacts.
    fn apply_grouping_projection(
        &self,
        state: LoadAccessState<E>,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        let LoadAccessState {
            context,
            access_inputs,
        } = state;
        let load_mode = context.mode.mode;
        let LoadAccessInputs { plan, cursor } = access_inputs;
        let (payload, trace) = match cursor {
            PreparedLoadCursor::Scalar(resolved_continuation) => {
                let (page, trace) = self.execute_scalar_path(
                    plan,
                    *resolved_continuation,
                    matches!(load_mode, LoadMode::ScalarRows),
                )?;
                (LoadExecutionPayload::Scalar(page), trace)
            }
            PreparedLoadCursor::Grouped(cursor) => {
                let (page, trace) = self.execute_grouped_path(plan, cursor)?;
                (LoadExecutionPayload::Grouped(page), trace)
            }
        };

        Ok(LoadPayloadState {
            context,
            payload,
            trace,
        })
    }
}
