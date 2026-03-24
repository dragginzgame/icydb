//! Module: db::executor::pipeline::entrypoints::scalar::surface
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::scalar::surface.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        PersistedRow,
        executor::{
            ExecutionTrace, LoadCursorInput, PreparedLoadPlan,
            pipeline::contracts::{CursorPage, LoadExecutor},
            pipeline::entrypoints::{LoadExecutionMode, LoadTracingMode},
            pipeline::orchestrator::LoadExecutionSurface,
        },
    },
    error::InternalError,
    traits::EntityValue,
};

///
/// ScalarTracedSurfaceContract
///
/// ScalarTracedSurfaceContract owns the fixed scalar traced-page surface
/// mismatch invariant for the scalar load entrypoint boundary.
///

struct ScalarTracedSurfaceContract;

impl ScalarTracedSurfaceContract {
    // Build the canonical scalar traced-surface mismatch invariant.
    fn scalar_traced_surface_required() -> InternalError {
        InternalError::query_executor_invariant(
            "scalar traced entrypoint must produce scalar traced page surface",
        )
    }
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Execute one traced paged scalar load and materialize traced page output.
    pub(in crate::db::executor) fn execute_load_scalar_page_with_trace(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load_surface(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Enabled),
        )?;

        Self::expect_scalar_traced_surface(surface)
    }

    // Project one traced paged scalar load surface and classify shape mismatches.
    fn expect_scalar_traced_surface(
        surface: LoadExecutionSurface,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        match surface {
            LoadExecutionSurface::ScalarPageWithTrace(page, trace) => {
                Ok((page.into_cursor_page::<E>()?, trace))
            }
            LoadExecutionSurface::GroupedPageWithTrace(..) => {
                Err(ScalarTracedSurfaceContract::scalar_traced_surface_required())
            }
        }
    }
}
