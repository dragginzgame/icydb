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
    db::executor::{
        LoadCursorInput, PreparedLoadPlan, pipeline::contracts::LoadExecutor,
        pipeline::orchestrator::state::LoadPayloadState,
    },
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

// Validate that one staged payload shape matches the selected load surface
// mode before later paging/tracing/materialization phases consume it.
fn validate_runtime_payload_shape(
    execution_mode: LoadSurfaceMode,
    payload: &state::LoadExecutionPayload,
) -> Result<(), InternalError> {
    payload.validate_for_mode(execution_mode)
}

/// Apply paging contracts over generic-free payload artifacts.
fn apply_runtime_paging(state: LoadPayloadState) -> Result<LoadPayloadState, InternalError> {
    validate_runtime_payload_shape(state.context.mode, &state.payload)?;

    Ok(state)
}

// Apply tracing contracts over generic-free runtime payload artifacts.
const fn apply_runtime_tracing(mut state: LoadPayloadState) -> LoadPayloadState {
    if !state.context.mode.tracing_enabled() {
        state.trace = None;
    }

    state
}

// Materialize one finalized generic-free load surface from runtime payload artifacts.
fn materialize_runtime_surface(
    state: LoadPayloadState,
) -> Result<LoadExecutionSurface, InternalError> {
    let execution_mode = state.context.mode;
    validate_runtime_payload_shape(execution_mode, &state.payload)?;

    if execution_mode.is_scalar_page() {
        let page = state.payload.into_scalar_page()?;

        Ok(LoadExecutionSurface::ScalarPageWithTrace(page, state.trace))
    } else {
        debug_assert!(
            execution_mode.is_grouped_page(),
            "runtime surface materialization expects grouped mode for non-scalar load surfaces",
        );
        let page = state.payload.into_grouped_page()?;

        Ok(LoadExecutionSurface::GroupedPageWithTrace(
            page,
            state.trace,
        ))
    }
}

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
        let payload_state = apply_runtime_paging(payload_state)?;
        let payload_state = apply_runtime_tracing(payload_state);

        materialize_runtime_surface(payload_state)
    }
}
