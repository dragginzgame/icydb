//! Module: executor::pipeline::orchestrator
//! Responsibility: load entrypoint runtime wiring and contract-boundary exports.
//! Does not own: row materialization mechanics or continuation cursor resolution internals.
//! Boundary: executes the canonical structural load surface path and exposes the
//! stable load contracts needed by entrypoints and runtime leaves.

mod contracts;
mod state;
mod strategy;

use crate::{
    db::executor::{
        ExecutablePlan, LoadCursorInput, pipeline::contracts::LoadExecutor,
        pipeline::orchestrator::state::LoadPayloadState,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
pub(in crate::db::executor) use contracts::{
    LoadExecutionMode, LoadExecutionSurface, LoadTracingMode,
};

/// Apply paging contracts over generic-free payload artifacts.
fn apply_runtime_paging(mut state: LoadPayloadState) -> Result<LoadPayloadState, InternalError> {
    let execution_mode = state.context.mode;
    let payload = if execution_mode.scalar_rows_mode() || execution_mode.scalar_page_mode() {
        match state.payload {
            state::LoadExecutionPayload::Scalar(payload) => {
                state::LoadExecutionPayload::Scalar(payload)
            }
            state::LoadExecutionPayload::Grouped(_) => {
                return Err(crate::db::error::query_executor_invariant(
                    "scalar load mode must carry scalar runtime payload",
                ));
            }
        }
    } else {
        debug_assert!(
            execution_mode.grouped_page_mode(),
            "runtime payload paging expects grouped mode for non-scalar load surfaces",
        );
        match state.payload {
            state::LoadExecutionPayload::Grouped(page) => {
                state::LoadExecutionPayload::Grouped(page)
            }
            state::LoadExecutionPayload::Scalar(_) => {
                return Err(crate::db::error::query_executor_invariant(
                    "grouped load mode must carry grouped runtime payload",
                ));
            }
        }
    };
    state.payload = payload;

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
    if execution_mode.scalar_page_mode() {
        let state::LoadExecutionPayload::Scalar(page) = state.payload else {
            return Err(crate::db::error::query_executor_invariant(
                "scalar page load mode must carry scalar runtime payload",
            ));
        };

        if execution_mode.tracing_enabled() {
            Ok(LoadExecutionSurface::ScalarPageWithTrace(page, state.trace))
        } else {
            Ok(LoadExecutionSurface::ScalarPage(page))
        }
    } else if execution_mode.scalar_rows_mode() {
        let state::LoadExecutionPayload::Scalar(page) = state.payload else {
            return Err(crate::db::error::query_executor_invariant(
                "scalar rows load mode must carry scalar runtime payload",
            ));
        };

        Ok(LoadExecutionSurface::ScalarPage(page))
    } else {
        debug_assert!(
            execution_mode.grouped_page_mode(),
            "runtime surface materialization expects grouped mode for non-scalar load surfaces",
        );
        let state::LoadExecutionPayload::Grouped(page) = state.payload else {
            return Err(crate::db::error::query_executor_invariant(
                "grouped page load mode must carry grouped runtime payload",
            ));
        };

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
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionSurface, InternalError> {
        let prepared_plan = plan.into_prepared_load_plan();
        let access_state = self.build_execution_context(prepared_plan, cursor, execution_mode)?;
        let payload_state = Self::apply_grouping_projection(access_state)?;
        let payload_state = apply_runtime_paging(payload_state)?;
        let payload_state = apply_runtime_tracing(payload_state);

        materialize_runtime_surface(payload_state)
    }
}
