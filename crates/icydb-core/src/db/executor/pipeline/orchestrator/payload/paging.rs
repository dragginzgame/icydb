use crate::{
    db::executor::pipeline::{
        contracts::LoadExecutor,
        orchestrator::state::{LoadExecutionPayload, LoadPayloadState},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Apply paging contracts over staged payload artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) fn apply_paging(
        mut state: LoadPayloadState<E>,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        let execution_mode = state.context.mode;
        let payload = if execution_mode.scalar_rows_mode() {
            let mut page = Self::expect_scalar_payload(
                state.payload,
                "unpaged load execution mode must carry scalar payload",
            )?;
            // Unpaged scalar execution intentionally suppresses continuation payload.
            page.next_cursor = None;
            LoadExecutionPayload::Scalar(page)
        } else if execution_mode.scalar_page_mode() {
            LoadExecutionPayload::Scalar(Self::expect_scalar_payload(
                state.payload,
                "scalar page load mode must carry scalar payload",
            )?)
        } else {
            debug_assert!(
                execution_mode.grouped_page_mode(),
                "payload paging expects grouped mode for non-scalar load surfaces",
            );
            LoadExecutionPayload::Grouped(Self::expect_grouped_payload(
                state.payload,
                "grouped page load mode must carry grouped payload",
            )?)
        };
        state.payload = payload;

        Ok(state)
    }

    // Apply tracing contracts as a post-processing layer over staged artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) const fn apply_tracing(
        mut state: LoadPayloadState<E>,
    ) -> LoadPayloadState<E> {
        if !state.context.mode.tracing_enabled() {
            state.trace = None;
        }

        state
    }
}
