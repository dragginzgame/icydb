//! Module: executor::pipeline::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

mod grouped;
mod scalar;
#[cfg(feature = "diagnostics")]
use crate::db::executor::pipeline::entrypoints::scalar::execute_prepared_scalar_rows_for_canister_with_phase_attribution;
use crate::{
    db::{
        PersistedRow,
        cursor::{GroupedPlannedCursor, PlannedCursor},
        data::decode_data_rows_into_entity_response,
        executor::{
            CursorPage, ExecutionTrace, LoadCursorInput, PreparedExecutionPlan,
            pipeline::contracts::{GroupedCursorPage, LoadExecutor},
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityValue,
};

pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::{
    LoadSurfaceMode, LoadTracingMode,
};
#[cfg(test)]
pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
#[cfg(feature = "sql")]
pub(in crate::db) use grouped::execute_initial_grouped_rows_for_canister;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use grouped::execute_initial_grouped_rows_for_canister_with_phase_attribution;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use grouped::{GroupedCountAttribution, GroupedExecutePhaseAttribution};
pub(in crate::db::executor) use grouped::{
    PreparedGroupedRouteRuntime, execute_prepared_grouped_route_runtime,
};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use scalar::ScalarExecutePhaseAttribution;
#[cfg(feature = "sql")]
pub(in crate::db) use scalar::execute_initial_scalar_retained_slot_page_for_canister;
pub(in crate::db::executor) use scalar::{
    PreparedScalarMaterializedBoundary, PreparedScalarRouteRuntime,
    execute_prepared_scalar_route_runtime, execute_prepared_scalar_rows_for_canister,
};

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_load_entry_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "diagnostics")]
fn measure_load_entry_phase<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_load_entry_local_instruction_counter();
    let result = run();
    let delta = read_load_entry_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "diagnostics")]
fn resolve_grouped_perf_cursor(
    plan: &crate::db::executor::PreparedLoadPlan,
    cursor: LoadCursorInput,
) -> Result<crate::db::executor::PreparedLoadCursor, InternalError> {
    crate::db::executor::LoadCursorResolver::resolve_load_cursor_context(
        plan,
        cursor,
        LoadSurfaceMode::grouped_paged(LoadTracingMode::Enabled),
    )
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Execute one scalar load plan without explicit cursor input.
    pub(crate) fn execute(
        &self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        let page = execute_prepared_scalar_rows_for_canister(
            &self.db,
            self.debug,
            plan.into_prepared_load_plan(),
        )?;
        let (data_rows, _) = page.into_parts();

        decode_data_rows_into_entity_response::<E>(data_rows)
    }

    /// Execute one scalar load plan while reporting the internal execute split
    /// between runtime materialization, structural page finalization, and
    /// typed response decode.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_with_phase_attribution(
        &self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<(EntityResponse<E>, ScalarExecutePhaseAttribution, u64), InternalError> {
        // Phase 1: execute the scalar runtime through the shared structural
        // page boundary.
        let (page, phase_attribution) =
            execute_prepared_scalar_rows_for_canister_with_phase_attribution(
                &self.db,
                self.debug,
                plan.into_prepared_load_plan(),
            )?;
        let (data_rows, _) = page.into_parts();

        // Phase 2: decode the structural data rows into typed response rows.
        let (response_decode_local_instructions, response) =
            measure_load_entry_phase(|| decode_data_rows_into_entity_response::<E>(data_rows));
        let response = response?;

        Ok((
            response,
            phase_attribution,
            response_decode_local_instructions,
        ))
    }

    // Execute one scalar load plan and optionally emit execution trace output.
    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        self.execute_load_scalar_page_with_trace(
            plan.into_prepared_load_plan(),
            LoadCursorInput::scalar(cursor),
        )
    }

    // Execute one scalar load plan with cursor input and discard tracing.
    #[cfg(test)]
    pub(in crate::db) fn execute_paged_with_cursor(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        let (page, _) = self.execute_paged_with_cursor_traced(plan, cursor)?;

        Ok(page)
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        self.execute_load_grouped_page_with_trace(
            plan.into_prepared_load_plan(),
            LoadCursorInput::grouped(cursor),
        )
    }

    /// Execute one grouped load plan while reporting the grouped runtime
    /// stream/fold/finalize split for perf-only attribution surfaces.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced_with_phase_attribution(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<
        (
            GroupedCursorPage,
            Option<ExecutionTrace>,
            GroupedExecutePhaseAttribution,
        ),
        InternalError,
    > {
        self.execute_load_grouped_page_with_trace_with_phase_attribution(
            plan.into_prepared_load_plan(),
            LoadCursorInput::grouped(cursor),
        )
    }
}
