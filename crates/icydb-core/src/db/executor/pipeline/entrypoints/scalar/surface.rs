//! Module: db::executor::pipeline::entrypoints::scalar::surface
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::scalar::surface.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput,
            pipeline::contracts::{CursorPage, LoadExecutor},
            pipeline::entrypoints::{LoadExecutionMode, LoadExecutionSurface, LoadTracingMode},
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
        let surface = self.execute_load(plan, cursor, LoadExecutionMode::scalar_unpaged_rows())?;

        Self::expect_scalar_rows_surface(surface)
    }

    // Execute one paged scalar load and materialize page output.
    pub(in crate::db::executor) fn execute_load_scalar_page(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<CursorPage<E>, InternalError> {
        let surface = self.execute_load(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Disabled),
        )?;

        Self::expect_scalar_page_surface(surface)
    }

    // Execute one traced paged scalar load and materialize traced page output.
    pub(in crate::db::executor) fn execute_load_scalar_page_with_trace(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Enabled),
        )?;

        Self::expect_scalar_traced_surface(surface)
    }

    // Project one rows-only scalar load surface and classify shape mismatches.
    fn expect_scalar_rows_surface(
        surface: LoadExecutionSurface<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        match surface {
            LoadExecutionSurface::ScalarRows(rows) => Ok(rows),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar rows entrypoint must produce scalar rows surface",
            )),
        }
    }

    // Project one paged scalar load surface and classify shape mismatches.
    fn expect_scalar_page_surface(
        surface: LoadExecutionSurface<E>,
    ) -> Result<CursorPage<E>, InternalError> {
        match surface {
            LoadExecutionSurface::ScalarPage(page) => Ok(page),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar page entrypoint must produce scalar page surface",
            )),
        }
    }

    // Project one traced paged scalar load surface and classify shape mismatches.
    fn expect_scalar_traced_surface(
        surface: LoadExecutionSurface<E>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        match surface {
            LoadExecutionSurface::ScalarPageWithTrace(page, trace) => Ok((page, trace)),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar traced entrypoint must produce scalar traced page surface",
            )),
        }
    }
}
