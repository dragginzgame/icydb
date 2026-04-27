//! Module: executor::pipeline::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

mod grouped;
mod scalar;
#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::measure_local_instruction_delta as measure_load_entry_phase;
#[cfg(feature = "diagnostics")]
use crate::db::executor::pipeline::entrypoints::scalar::execute_prepared_scalar_rows_for_canister_with_phase_attribution;
use crate::{
    db::{
        PersistedRow,
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            CursorPage, ExecutionTrace, LoadCursorInput, PreparedExecutionPlan,
            pipeline::{
                contracts::{LoadExecutor, StructuralGroupedProjectionResult},
                orchestrator::{LoadExecutionContext, LoadExecutionPayload, LoadPayloadState},
            },
            terminal::decode_data_rows_into_entity_response,
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityValue,
};

pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::{
    LoadSurfaceMode, LoadTracingMode,
};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use grouped::{GroupedCountAttribution, GroupedExecutePhaseAttribution};
pub(in crate::db::executor) use grouped::{
    PreparedGroupedRouteRuntime, execute_prepared_grouped_route_runtime,
    prepare_grouped_route_runtime_for_load_plan,
};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use scalar::ScalarExecutePhaseAttribution;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use scalar::execute_initial_scalar_retained_slot_page_from_runtime_parts_for_canister;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use scalar::execute_prepared_scalar_aggregate_kernel_row_sink_for_canister;
pub(in crate::db::executor) use scalar::{
    PreparedScalarMaterializedBoundary, PreparedScalarRouteRuntime,
    execute_prepared_scalar_route_runtime, execute_prepared_scalar_rows_for_canister,
};

///
/// PreparedLoadRouteRuntime
///
/// PreparedLoadRouteRuntime is the canonical prepared load-route runtime
/// envelope for scalar and grouped entrypoint lanes.
/// It keeps the route-family choice on one boundary type so orchestrator
/// staging does not wrap the two prepared runtime structs in a second enum.
///
#[expect(
    clippy::large_enum_variant,
    reason = "prepared runtimes stay inline so the entrypoint boundary owns the scalar/grouped split directly"
)]
pub(in crate::db::executor) enum PreparedLoadRouteRuntime {
    Scalar(PreparedScalarRouteRuntime),
    Grouped(PreparedGroupedRouteRuntime),
}

impl PreparedLoadRouteRuntime {
    // Build one scalar prepared route runtime envelope.
    pub(in crate::db::executor::pipeline) const fn scalar(
        prepared: PreparedScalarRouteRuntime,
    ) -> Self {
        Self::Scalar(prepared)
    }

    // Build one grouped prepared route runtime envelope.
    pub(in crate::db::executor::pipeline) const fn grouped(
        prepared: PreparedGroupedRouteRuntime,
    ) -> Self {
        Self::Grouped(prepared)
    }

    // Execute one variant-owned prepared route runtime and return the payload
    // plus trace pair before the shared outer execution state is rebuilt.
    fn execute_payload(
        self,
    ) -> Result<(LoadExecutionPayload, Option<ExecutionTrace>), InternalError> {
        match self {
            Self::Scalar(prepared) => {
                let (page, trace) = execute_prepared_scalar_route_runtime(prepared)?;

                Ok((LoadExecutionPayload::scalar(page), trace))
            }
            Self::Grouped(prepared) => {
                let (page, trace) = execute_prepared_grouped_route_runtime(prepared)?;

                Ok((LoadExecutionPayload::grouped(page), trace))
            }
        }
    }

    // Execute one canonical entrypoint dispatch over one already-prepared
    // scalar or grouped route runtime envelope.
    pub(in crate::db::executor::pipeline) fn execute(
        self,
        context: LoadExecutionContext,
    ) -> Result<LoadPayloadState, InternalError> {
        let (payload, trace) = self.execute_payload()?;

        Ok(LoadPayloadState::new(context, payload, trace))
    }
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
        cursor: PlannedCursor,
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
        cursor: PlannedCursor,
    ) -> Result<CursorPage<E>, InternalError> {
        let (page, _) = self.execute_paged_with_cursor_traced(plan, cursor)?;

        Ok(page)
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(StructuralGroupedProjectionResult, Option<ExecutionTrace>), InternalError> {
        let (page, trace) = self.execute_load_grouped_page_with_trace(
            plan.into_prepared_load_plan(),
            LoadCursorInput::grouped(cursor),
        )?;

        Ok((StructuralGroupedProjectionResult::from_page(page), trace))
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
            StructuralGroupedProjectionResult,
            Option<ExecutionTrace>,
            GroupedExecutePhaseAttribution,
        ),
        InternalError,
    > {
        let (page, trace, phase_attribution) = self
            .execute_load_grouped_page_with_trace_with_phase_attribution(
                plan.into_prepared_load_plan(),
                LoadCursorInput::grouped(cursor),
            )?;

        Ok((
            StructuralGroupedProjectionResult::from_page(page),
            trace,
            phase_attribution,
        ))
    }
}
