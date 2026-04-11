//! Module: executor::pipeline::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

mod grouped;
mod scalar;

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
use crate::{
    db::{
        Db,
        executor::{
            EntityAuthority,
            pipeline::contracts::{ProjectionMaterializationMode, StructuralCursorPage},
        },
        query::plan::AccessPlannedQuery,
    },
    traits::CanisterKind,
};
use crate::{
    db::{
        PersistedRow,
        cursor::{GroupedPlannedCursor, PlannedCursor},
        data::decode_data_rows_into_entity_response,
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput,
            pipeline::contracts::{
                CursorPage, GroupedCursorPage, LoadExecutor, StructuralCursorPage,
            },
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityValue,
};

pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::{
    LoadExecutionMode, LoadTracingMode,
};
#[cfg(test)]
pub(in crate::db::executor) use crate::db::executor::pipeline::orchestrator::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
#[cfg(feature = "sql")]
pub(in crate::db) use grouped::execute_initial_grouped_rows_for_canister;
pub(in crate::db::executor) use grouped::{
    PreparedGroupedRouteRuntime, execute_prepared_grouped_route_runtime,
};
pub(in crate::db::executor) use scalar::{
    PreparedScalarMaterializedBoundary, PreparedScalarRouteRuntime,
    execute_prepared_scalar_route_runtime, execute_prepared_scalar_rows_for_canister,
};
#[cfg(feature = "sql")]
pub(in crate::db) use scalar::{
    execute_initial_scalar_sql_projection_rows_for_canister,
    execute_initial_scalar_sql_projection_text_rows_for_canister,
};

// Decode one structural scalar page into the final typed cursor page at the
// executor entrypoint boundary instead of on the structural page payload type.
pub(in crate::db::executor) fn decode_structural_page_into_cursor_page<E>(
    page: StructuralCursorPage,
) -> Result<CursorPage<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let (data_rows, next_cursor) = page.into_parts();

    Ok(CursorPage {
        items: decode_data_rows_into_entity_response::<E>(data_rows)?,
        next_cursor,
    })
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub(in crate::db) fn execute_initial_scalar_sql_projection_page_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
    projection_runtime_mode: ProjectionMaterializationMode,
) -> Result<StructuralCursorPage, InternalError>
where
    C: CanisterKind,
{
    scalar::execute_initial_scalar_sql_projection_page_for_canister(
        db,
        debug,
        authority,
        plan,
        projection_runtime_mode,
    )
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Execute one scalar load plan without explicit cursor input.
    pub(crate) fn execute(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        let page = execute_prepared_scalar_rows_for_canister(
            &self.db,
            self.debug,
            plan.into_prepared_load_plan(),
        )?;
        let (data_rows, _) = page.into_parts();

        decode_data_rows_into_entity_response::<E>(data_rows)
    }

    // Execute one scalar load plan and optionally emit execution trace output.
    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
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
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        let (page, _) = self.execute_paged_with_cursor_traced(plan, cursor)?;

        Ok(page)
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        self.execute_load_grouped_page_with_trace(
            plan.into_prepared_load_plan(),
            LoadCursorInput::grouped(cursor),
        )
    }
}
