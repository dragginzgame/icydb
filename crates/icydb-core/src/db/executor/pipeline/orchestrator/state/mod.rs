//! Module: db::executor::pipeline::orchestrator::state
//! Responsibility: owns context artifacts and payload state contracts for load
//! runtime orchestration.
//! Does not own: terminal materialization logic or strategy dispatch.
//! Boundary: exports the runtime state seams shared by the monomorphic load path.

use crate::db::executor::{
    ExecutionTrace,
    pipeline::contracts::{GroupedCursorPage, StructuralCursorPage},
    pipeline::entrypoints::PreparedLoadRouteRuntime,
    pipeline::orchestrator::{LoadExecutionSurface, LoadSurfaceMode},
};
use crate::error::InternalError;

///
/// LoadExecutionContext
///
/// Canonical execution artifacts normalized before staged orchestration.
/// Owns immutable entrypoint mode contracts consumed by pipeline stages.
///
pub(in crate::db::executor::pipeline) struct LoadExecutionContext {
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
/// Carries one canonical prepared route runtime into payload execution stage.
///
pub(in crate::db::executor::pipeline::orchestrator) struct LoadAccessInputs {
    pub(in crate::db::executor::pipeline::orchestrator) prepared_runtime: PreparedLoadRouteRuntime,
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
pub(in crate::db::executor::pipeline) struct LoadPayloadState {
    pub(in crate::db::executor::pipeline::orchestrator) context: LoadExecutionContext,
    pub(in crate::db::executor::pipeline::orchestrator) payload: LoadExecutionPayload,
    pub(in crate::db::executor::pipeline::orchestrator) trace: Option<ExecutionTrace>,
}

impl LoadPayloadState {
    // Construct one payload-stage state from one prepared execution result.
    pub(in crate::db::executor::pipeline) const fn new(
        context: LoadExecutionContext,
        payload: LoadExecutionPayload,
        trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            context,
            payload,
            trace,
        }
    }

    // Validate that one staged payload shape matches the selected load-surface
    // mode before later paging, tracing, and surface-materialization phases
    // consume it.
    pub(in crate::db::executor::pipeline::orchestrator) fn apply_paging(
        self,
    ) -> Result<Self, InternalError> {
        self.payload.validate_for_mode(self.context.mode)?;

        Ok(self)
    }

    // Apply tracing contracts over generic-free runtime payload artifacts.
    pub(in crate::db::executor::pipeline::orchestrator) const fn apply_tracing(mut self) -> Self {
        if !self.context.mode.tracing_enabled() {
            self.trace = None;
        }

        self
    }

    // Materialize one finalized generic-free load surface from runtime payload
    // artifacts after paging and tracing have already run.
    pub(in crate::db::executor::pipeline::orchestrator) fn into_surface(
        self,
    ) -> Result<LoadExecutionSurface, InternalError> {
        let execution_mode = self.context.mode;
        self.payload.validate_for_mode(execution_mode)?;

        if execution_mode.is_scalar_page() {
            let page = self.payload.into_scalar_page()?;

            return Ok(LoadExecutionSurface::ScalarPageWithTrace(page, self.trace));
        }

        debug_assert!(
            execution_mode.is_grouped_page(),
            "runtime surface materialization expects grouped mode for non-scalar load surfaces",
        );
        let page = self.payload.into_grouped_page()?;

        Ok(LoadExecutionSurface::GroupedPageWithTrace(page, self.trace))
    }
}

///
/// LoadExecutionPayload
///
/// Canonical payload envelope produced by one load orchestration pass.
///
pub(in crate::db::executor::pipeline) enum LoadExecutionPayload {
    Scalar(StructuralCursorPage),
    Grouped(GroupedCursorPage),
}

impl LoadExecutionPayload {
    // Wrap one scalar page in the canonical load payload envelope.
    pub(in crate::db::executor::pipeline) const fn scalar(page: StructuralCursorPage) -> Self {
        Self::Scalar(page)
    }

    // Wrap one grouped page in the canonical load payload envelope.
    pub(in crate::db::executor::pipeline) const fn grouped(page: GroupedCursorPage) -> Self {
        Self::Grouped(page)
    }

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
