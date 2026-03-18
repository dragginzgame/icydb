use crate::{
    db::executor::pipeline::{
        contracts::LoadExecutor,
        orchestrator::{LoadExecutionSurface, state::LoadPayloadState},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Materialize one finalized response surface from staged artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) fn materialize_surface(
        state: LoadPayloadState,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        let execution_mode = state.context.mode;
        if execution_mode.scalar_rows_mode() {
            let page = Self::expect_scalar_payload(
                state.payload,
                "rows load surface mode must carry scalar payload",
            )?;

            Ok(LoadExecutionSurface::ScalarRows(page.items))
        } else if execution_mode.scalar_page_mode() {
            let page = Self::expect_scalar_payload(
                state.payload,
                "scalar page load mode must carry scalar payload",
            )?;

            if execution_mode.tracing_enabled() {
                Ok(LoadExecutionSurface::ScalarPageWithTrace(page, state.trace))
            } else {
                Ok(LoadExecutionSurface::ScalarPage(page))
            }
        } else {
            debug_assert!(
                execution_mode.grouped_page_mode(),
                "surface materialization expects grouped mode for non-scalar load surfaces",
            );
            let page = Self::expect_grouped_payload(
                state.payload,
                "grouped page load mode must carry grouped payload",
            )?;

            Ok(LoadExecutionSurface::GroupedPageWithTrace(
                page,
                state.trace,
            ))
        }
    }
}
