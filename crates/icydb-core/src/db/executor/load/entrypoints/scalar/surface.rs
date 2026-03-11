//! Module: db::executor::load::entrypoints::scalar::surface
//! Responsibility: module-local ownership and contracts for db::executor::load::entrypoints::scalar::surface.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput,
            load::{
                CursorPage, LoadExecutor,
                entrypoints::{LoadExecutionMode, LoadExecutionSurface, LoadTracingMode},
            },
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
    pub(in crate::db::executor::load) fn execute_load_scalar_rows(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<EntityResponse<E>, InternalError> {
        let surface = self.execute_load(plan, cursor, LoadExecutionMode::scalar_unpaged_rows())?;
        match surface {
            LoadExecutionSurface::ScalarRows(rows) => Ok(rows),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar rows entrypoint must produce scalar rows surface",
            )),
        }
    }

    // Execute one paged scalar load and materialize page output.
    pub(in crate::db::executor::load) fn execute_load_scalar_page(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<CursorPage<E>, InternalError> {
        let surface = self.execute_load(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Disabled),
        )?;
        match surface {
            LoadExecutionSurface::ScalarPage(page) => Ok(page),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar page entrypoint must produce scalar page surface",
            )),
        }
    }

    // Execute one traced paged scalar load and materialize traced page output.
    pub(in crate::db::executor::load) fn execute_load_scalar_page_with_trace(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Enabled),
        )?;
        match surface {
            LoadExecutionSurface::ScalarPageWithTrace(page, trace) => Ok((page, trace)),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar traced entrypoint must produce scalar traced page surface",
            )),
        }
    }
}
