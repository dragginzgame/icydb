//! Module: db::executor::pipeline::entrypoints::scalar::surface
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::scalar::surface.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput,
            pipeline::contracts::{CursorPage, LoadExecutor, StructuralCursorPage},
            pipeline::entrypoints::{LoadExecutionMode, LoadTracingMode},
            pipeline::orchestrator::ErasedLoadExecutionSurface,
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one unpaged scalar load and materialize rows.
    pub(in crate::db::executor) fn execute_load_scalar_rows(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<EntityResponse<E>, InternalError> {
        let surface =
            self.execute_load_erased(plan, cursor, LoadExecutionMode::scalar_unpaged_rows())?;

        Self::expect_scalar_rows_surface(surface)
    }

    // Execute one traced paged scalar load and materialize traced page output.
    pub(in crate::db::executor) fn execute_load_scalar_page_with_trace(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load_erased(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Enabled),
        )?;

        Self::expect_scalar_traced_surface(surface)
    }

    // Project one rows-only scalar load surface and classify shape mismatches.
    fn expect_scalar_rows_surface(
        surface: ErasedLoadExecutionSurface,
    ) -> Result<EntityResponse<E>, InternalError> {
        match surface {
            ErasedLoadExecutionSurface::ScalarPage(page) => {
                let page = page.into_typed::<StructuralCursorPage>(
                    "scalar rows entrypoint must receive structural cursor page payload",
                )?;

                page.into_entity_response::<E>()
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar rows entrypoint must produce scalar rows surface",
            )),
        }
    }

    // Project one traced paged scalar load surface and classify shape mismatches.
    fn expect_scalar_traced_surface(
        surface: ErasedLoadExecutionSurface,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        match surface {
            ErasedLoadExecutionSurface::ScalarPageWithTrace(page, trace) => Ok((
                page.into_typed::<StructuralCursorPage>(
                    "scalar traced entrypoint must receive structural cursor page payload",
                )?
                .into_cursor_page::<E>()?,
                trace,
            )),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar traced entrypoint must produce scalar traced page surface",
            )),
        }
    }
}
