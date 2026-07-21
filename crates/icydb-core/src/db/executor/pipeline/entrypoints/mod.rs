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
        cursor::{ValidatedCursor, ValidatedGroupedCursor},
        executor::{
            CursorPage, ExecutionTrace, LoadCursorInput, LoadCursorResolver, PreparedExecutionPlan,
            PreparedLoadCursor, PreparedLoadPlan,
            pipeline::{
                contracts::{LoadExecutor, StructuralGroupedProjectionResult},
                orchestrator::LoadExecutionSurface,
            },
            terminal::decode_data_rows_into_entity_response,
        },
        response::EntityResponse,
        schema::SchemaInfo,
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
};

pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::LoadSurfaceMode;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use grouped::{
    GroupedCountAttribution, GroupedExecutePhaseAttribution, GroupedRuntimeAttribution,
};
pub(in crate::db::executor) use grouped::{
    PreparedGroupedRouteRuntime, execute_prepared_grouped_route_runtime,
};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use scalar::ScalarExecutePhaseAttribution;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use scalar::execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister;
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
    // Execute one variant-owned prepared route runtime directly into its final
    // load surface. The runtime variant is the scalar/grouped discriminator.
    pub(in crate::db::executor::pipeline) fn execute(
        self,
    ) -> Result<LoadExecutionSurface, InternalError> {
        match self {
            Self::Scalar(prepared) => {
                let (page, trace) = execute_prepared_scalar_route_runtime(prepared)?;

                Ok(LoadExecutionSurface::ScalarPageWithTrace(page, trace))
            }
            Self::Grouped(prepared) => {
                let (page, trace) = execute_prepared_grouped_route_runtime(prepared)?;

                Ok(LoadExecutionSurface::GroupedPageWithTrace(page, trace))
            }
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve public grouped cursor input at the parent entrypoint boundary,
    /// then prepare the canonical grouped route runtime shared by ordinary and
    /// diagnostic execution.
    pub(in crate::db::executor::pipeline) fn prepare_grouped_load_route_runtime(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
    ) -> Result<PreparedGroupedRouteRuntime, InternalError> {
        if !plan.mode().is_load() {
            return Err(InternalError::load_executor_load_plan_required());
        }

        let resolved_cursor = LoadCursorResolver::resolve_load_cursor_context(
            &plan,
            cursor,
            LoadSurfaceMode::GroupedPage,
        )?;
        let PreparedLoadCursor::Grouped(cursor) = resolved_cursor else {
            return Err(InternalError::query_executor_invariant());
        };

        self.prepare_grouped_route_runtime_from_resolved_cursor(plan, cursor)
    }
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow,
{
    // Execute one scalar load plan without explicit cursor input.
    pub(in crate::db) fn execute(
        &self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        let plan = plan.into_prepared_load_plan();
        let row_layout = plan.authority().row_layout()?;
        let page = execute_prepared_scalar_rows_for_canister(&self.db, self.debug, plan)?;
        let (data_rows, _) = page.require_data_rows_and_cursor()?;

        decode_data_rows_into_entity_response::<E>(&row_layout, data_rows)
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
        let (load_plan_local_instructions, plan) =
            measure_load_entry_phase(|| plan.into_prepared_load_plan());
        let (row_layout_local_instructions, row_layout) =
            measure_load_entry_phase(|| plan.authority().row_layout());
        let row_layout = row_layout?;
        let (page, mut phase_attribution) =
            execute_prepared_scalar_rows_for_canister_with_phase_attribution(
                &self.db, self.debug, plan,
            )?;
        phase_attribution.load_plan_local_instructions = load_plan_local_instructions;
        phase_attribution.row_layout_local_instructions = row_layout_local_instructions;
        let (data_rows, _) = page.require_data_rows_and_cursor()?;

        // Phase 2: decode the structural data rows into typed response rows.
        let (response_decode_local_instructions, response) = measure_load_entry_phase(|| {
            decode_data_rows_into_entity_response::<E>(&row_layout, data_rows)
        });
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
        cursor: ValidatedCursor,
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
        cursor: ValidatedCursor,
    ) -> Result<CursorPage<E>, InternalError> {
        let (page, _) = self.execute_paged_with_cursor_traced(plan, cursor)?;

        Ok(page)
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor: impl Into<ValidatedGroupedCursor>,
    ) -> Result<(StructuralGroupedProjectionResult, Option<ExecutionTrace>), InternalError> {
        let enum_catalog = plan
            .authority_ref()
            .accepted_schema_info()
            .and_then(SchemaInfo::value_catalog_handle)
            .cloned()
            .ok_or_else(InternalError::query_executor_invariant)?;
        let (page, trace) = self.execute_load_grouped_page_with_trace(
            plan.into_prepared_load_plan(),
            LoadCursorInput::grouped(cursor),
        )?;

        Ok((
            StructuralGroupedProjectionResult::from_page(page, enum_catalog),
            trace,
        ))
    }

    /// Execute one grouped load plan while reporting the grouped runtime
    /// stream/fold/finalize split for perf-only attribution surfaces.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced_with_phase_attribution(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor: impl Into<ValidatedGroupedCursor>,
    ) -> Result<
        (
            StructuralGroupedProjectionResult,
            Option<ExecutionTrace>,
            GroupedExecutePhaseAttribution,
        ),
        InternalError,
    > {
        let enum_catalog = plan
            .authority_ref()
            .accepted_schema_info()
            .and_then(SchemaInfo::value_catalog_handle)
            .cloned()
            .ok_or_else(InternalError::query_executor_invariant)?;
        let (page, trace, phase_attribution) = self
            .execute_load_grouped_page_with_trace_with_phase_attribution(
                plan.into_prepared_load_plan(),
                LoadCursorInput::grouped(cursor),
            )?;

        Ok((
            StructuralGroupedProjectionResult::from_page(page, enum_catalog),
            trace,
            phase_attribution,
        ))
    }
}
