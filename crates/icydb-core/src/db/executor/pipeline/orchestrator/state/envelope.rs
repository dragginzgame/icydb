#![allow(dead_code)]

use crate::{
    db::executor::{
        ExecutablePlan, LoadCursorInput,
        pipeline::orchestrator::{
            LoadExecutionMode, LoadExecutionSurface,
            state::{LoadAccessState, LoadPayloadState},
        },
    },
    error::InternalError,
    traits::EntityKind,
};

///
/// LoadPipelineState
///
/// Stage-local state envelope used by deterministic load stage execution.
/// Encodes typed stage artifacts so orchestration remains linear and explicit.
///

pub(in crate::db::executor::pipeline::orchestrator) enum LoadPipelineState<E: EntityKind> {
    Inputs {
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    },
    Access(LoadAccessState<E>),
    Payload(LoadPayloadState<E>),
    Surface(LoadExecutionSurface<E>),
}

impl<E> LoadPipelineState<E>
where
    E: EntityKind,
{
    // Build one access-stage envelope from one access-state payload.
    pub(in crate::db::executor::pipeline::orchestrator) const fn from_access(
        state: LoadAccessState<E>,
    ) -> Self {
        Self::Access(state)
    }

    // Build one payload-stage envelope from one payload-state payload.
    pub(in crate::db::executor::pipeline::orchestrator) const fn from_payload(
        state: LoadPayloadState<E>,
    ) -> Self {
        Self::Payload(state)
    }

    // Build one surface-stage envelope from one finalized load surface.
    pub(in crate::db::executor::pipeline::orchestrator) const fn from_surface(
        surface: LoadExecutionSurface<E>,
    ) -> Self {
        Self::Surface(surface)
    }

    // Extract pre-context inputs and reject stage-order drift with one invariant error.
    pub(in crate::db::executor::pipeline::orchestrator) fn expect_inputs(
        self,
        mismatch_message: &'static str,
    ) -> Result<(ExecutablePlan<E>, LoadCursorInput, LoadExecutionMode), InternalError> {
        match self {
            Self::Inputs {
                plan,
                cursor,
                execution_mode,
            } => Ok((plan, cursor, execution_mode)),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_message)),
        }
    }

    // Extract access-stage state and reject stage-order drift with one invariant error.
    pub(in crate::db::executor::pipeline::orchestrator) fn expect_access(
        self,
        mismatch_message: &'static str,
    ) -> Result<LoadAccessState<E>, InternalError> {
        match self {
            Self::Access(state) => Ok(state),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_message)),
        }
    }

    // Extract payload-stage state and reject stage-order drift with one invariant error.
    pub(in crate::db::executor::pipeline::orchestrator) fn expect_payload(
        self,
        mismatch_message: &'static str,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        match self {
            Self::Payload(state) => Ok(state),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_message)),
        }
    }

    // Consume final orchestration state into one terminal surface.
    pub(in crate::db::executor::pipeline::orchestrator) fn into_surface(
        self,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        match self {
            Self::Surface(surface) => Ok(surface),
            _ => Err(crate::db::error::query_executor_invariant(
                "load stage loop must terminate with a materialized surface",
            )),
        }
    }
}
