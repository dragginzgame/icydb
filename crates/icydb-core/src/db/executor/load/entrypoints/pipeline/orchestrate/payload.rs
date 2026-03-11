//! Module: db::executor::load::entrypoints::pipeline::orchestrate::payload
//! Responsibility: module-local ownership and contracts for db::executor::load::entrypoints::pipeline::orchestrate::payload.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::load::{
        CursorPage, GroupedCursorPage, LoadExecutor,
        entrypoints::pipeline::{
            LoadExecutionSurface, LoadMode, LoadTracingMode,
            orchestrate::state::{LoadExecutionPayload, LoadPayloadState},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Apply paging contracts over staged payload artifacts.
    pub(super) fn apply_paging(
        mut state: LoadPayloadState<E>,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        let payload = match state.context.mode.mode {
            LoadMode::ScalarRows => {
                let mut page = Self::expect_scalar_payload(
                    state.payload,
                    "unpaged load execution mode must carry scalar payload",
                )?;
                // Unpaged scalar execution intentionally suppresses continuation payload.
                page.next_cursor = None;
                LoadExecutionPayload::Scalar(page)
            }
            LoadMode::ScalarPage => LoadExecutionPayload::Scalar(Self::expect_scalar_payload(
                state.payload,
                "scalar page load mode must carry scalar payload",
            )?),
            LoadMode::GroupedPage => LoadExecutionPayload::Grouped(Self::expect_grouped_payload(
                state.payload,
                "grouped page load mode must carry grouped payload",
            )?),
        };
        state.payload = payload;

        Ok(state)
    }

    // Apply tracing contracts as a post-processing layer over staged artifacts.
    pub(super) const fn apply_tracing(mut state: LoadPayloadState<E>) -> LoadPayloadState<E> {
        if matches!(state.context.mode.tracing, LoadTracingMode::Disabled) {
            state.trace = None;
        }

        state
    }

    // Materialize one finalized response surface from staged artifacts.
    pub(super) fn materialize_surface(
        state: LoadPayloadState<E>,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        match state.context.mode.mode {
            LoadMode::ScalarRows => {
                let page = Self::expect_scalar_payload(
                    state.payload,
                    "rows load surface mode must carry scalar payload",
                )?;

                Ok(LoadExecutionSurface::ScalarRows(page.items))
            }
            LoadMode::ScalarPage => {
                let page = Self::expect_scalar_payload(
                    state.payload,
                    "scalar page load mode must carry scalar payload",
                )?;

                if matches!(state.context.mode.tracing, LoadTracingMode::Enabled) {
                    Ok(LoadExecutionSurface::ScalarPageWithTrace(page, state.trace))
                } else {
                    Ok(LoadExecutionSurface::ScalarPage(page))
                }
            }
            LoadMode::GroupedPage => {
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

    // Extract scalar payload at one stage boundary and classify mismatches.
    fn expect_scalar_payload(
        payload: LoadExecutionPayload<E>,
        mismatch_message: &'static str,
    ) -> Result<CursorPage<E>, InternalError> {
        match payload {
            LoadExecutionPayload::Scalar(page) => Ok(page),
            LoadExecutionPayload::Grouped(_) => {
                Err(crate::db::error::query_executor_invariant(mismatch_message))
            }
        }
    }

    // Extract grouped payload at one stage boundary and classify mismatches.
    fn expect_grouped_payload(
        payload: LoadExecutionPayload<E>,
        mismatch_message: &'static str,
    ) -> Result<GroupedCursorPage, InternalError> {
        match payload {
            LoadExecutionPayload::Grouped(page) => Ok(page),
            LoadExecutionPayload::Scalar(_) => {
                Err(crate::db::error::query_executor_invariant(mismatch_message))
            }
        }
    }
}
