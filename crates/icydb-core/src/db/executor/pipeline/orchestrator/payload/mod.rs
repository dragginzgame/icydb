//! Module: db::executor::pipeline::orchestrator::payload
//! Responsibility: payload-stage helper seams for paging and surface materialization.
//! Does not own: stage dispatch mechanics or pre-access strategy normalization.
//! Boundary: exposes payload-stage helpers for orchestrator stage execution.

mod paging;
mod surface;

use crate::{
    db::executor::pipeline::{
        contracts::{CursorPage, GroupedCursorPage, LoadExecutor},
        orchestrator::state::LoadExecutionPayload,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Extract scalar payload at one stage boundary and classify mismatches.
    pub(in crate::db::executor::pipeline::orchestrator) fn expect_scalar_payload(
        payload: LoadExecutionPayload,
        mismatch_message: &'static str,
    ) -> Result<CursorPage<E>, InternalError> {
        match payload {
            LoadExecutionPayload::Scalar(page) => {
                page.into_typed::<CursorPage<E>>(mismatch_message)
            }
            LoadExecutionPayload::Grouped(_) => {
                Err(crate::db::error::query_executor_invariant(mismatch_message))
            }
        }
    }

    // Extract grouped payload at one stage boundary and classify mismatches.
    pub(in crate::db::executor::pipeline::orchestrator) fn expect_grouped_payload(
        payload: LoadExecutionPayload,
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
