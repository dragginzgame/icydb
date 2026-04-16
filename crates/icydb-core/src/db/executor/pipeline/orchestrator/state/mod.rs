//! Module: db::executor::pipeline::orchestrator::state
//! Responsibility: owns context artifacts and payload state contracts for load
//! runtime orchestration.
//! Does not own: terminal materialization logic or strategy dispatch.
//! Boundary: exports the runtime state seams shared by the monomorphic load path.

use crate::db::executor::{
    ExecutionTrace,
    pipeline::contracts::{GroupedCursorPage, StructuralCursorPage},
    pipeline::orchestrator::LoadSurfaceMode,
    pipeline::orchestrator::strategy::ExecutionSpec,
};
use crate::error::InternalError;

///
/// LoadExecutionContext
///
/// Canonical execution artifacts normalized before staged orchestration.
/// Owns immutable entrypoint mode contracts consumed by pipeline stages.
///
pub(in crate::db::executor::pipeline::orchestrator) struct LoadExecutionContext {
    pub(in crate::db::executor::pipeline::orchestrator) mode: LoadSurfaceMode,
}

impl LoadExecutionContext {
    // Construct one immutable execution context from one normalized mode contract.
    pub(in crate::db::executor::pipeline::orchestrator) const fn new(
        mode: LoadSurfaceMode,
    ) -> Self {
        Self { mode }
    }
}

///
/// LoadAccessInputs
///
/// Access-stage payload extracted from execution context.
/// Carries normalized plan/cursor artifacts into grouping/projection stage.
///
pub(in crate::db::executor::pipeline::orchestrator) struct LoadAccessInputs {
    pub(in crate::db::executor::pipeline::orchestrator) execution_spec: ExecutionSpec,
}

///
/// LoadAccessState
///
/// Access-stage execution artifacts for one load orchestration pass.
/// Carries normalized context and one required access-stage payload.
///
pub(in crate::db::executor::pipeline::orchestrator) struct LoadAccessState {
    pub(in crate::db::executor::pipeline::orchestrator) context: LoadExecutionContext,
    pub(in crate::db::executor::pipeline::orchestrator) access_inputs: LoadAccessInputs,
}

///
/// LoadPayloadState
///
/// Payload-stage execution artifacts for one load orchestration pass.
/// Carries normalized context, one required payload, and optional trace output.
///
pub(in crate::db::executor::pipeline::orchestrator) struct LoadPayloadState {
    pub(in crate::db::executor::pipeline::orchestrator) context: LoadExecutionContext,
    pub(in crate::db::executor::pipeline::orchestrator) payload: LoadExecutionPayload,
    pub(in crate::db::executor::pipeline::orchestrator) trace: Option<ExecutionTrace>,
}

///
/// LoadExecutionPayload
///
/// Canonical payload envelope produced by one load orchestration pass.
///
pub(in crate::db::executor::pipeline::orchestrator) enum LoadExecutionPayload {
    Scalar(StructuralCursorPage),
    Grouped(GroupedCursorPage),
}

impl LoadExecutionPayload {
    // Validate that this payload shape matches the selected load-surface mode.
    pub(in crate::db::executor::pipeline::orchestrator) fn validate_for_mode(
        &self,
        execution_mode: LoadSurfaceMode,
    ) -> Result<(), InternalError> {
        if execution_mode.is_scalar_page() {
            return match self {
                Self::Scalar(_) => Ok(()),
                Self::Grouped(_) => Err(InternalError::load_runtime_scalar_payload_required()),
            };
        }

        debug_assert!(
            execution_mode.is_grouped_page(),
            "runtime payload validation expects grouped mode for non-scalar load surfaces",
        );
        match self {
            Self::Grouped(_) => Ok(()),
            Self::Scalar(_) => Err(InternalError::load_runtime_grouped_payload_required()),
        }
    }

    // Require one scalar cursor page after payload validation selected the
    // scalar load surface family.
    pub(in crate::db::executor::pipeline::orchestrator) fn into_scalar_page(
        self,
    ) -> Result<StructuralCursorPage, InternalError> {
        match self {
            Self::Scalar(page) => Ok(page),
            Self::Grouped(_) => Err(InternalError::load_runtime_scalar_surface_payload_required()),
        }
    }

    // Require one grouped cursor page after payload validation selected the
    // grouped load surface family.
    pub(in crate::db::executor::pipeline::orchestrator) fn into_grouped_page(
        self,
    ) -> Result<GroupedCursorPage, InternalError> {
        match self {
            Self::Grouped(page) => Ok(page),
            Self::Scalar(_) => Err(InternalError::load_runtime_grouped_surface_payload_required()),
        }
    }
}
