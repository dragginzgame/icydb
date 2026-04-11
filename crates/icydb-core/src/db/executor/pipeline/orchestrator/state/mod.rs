//! Module: db::executor::pipeline::orchestrator::state
//! Responsibility: owns context artifacts and payload state contracts for load
//! runtime orchestration.
//! Does not own: terminal materialization logic or strategy dispatch.
//! Boundary: exports the runtime state seams shared by the monomorphic load path.

use crate::db::executor::{
    ExecutionTrace,
    pipeline::contracts::{GroupedCursorPage, StructuralCursorPage},
    pipeline::orchestrator::LoadExecutionMode,
    pipeline::orchestrator::strategy::ExecutionSpec,
};

///
/// LoadExecutionContext
///
/// Canonical execution artifacts normalized before staged orchestration.
/// Owns immutable entrypoint mode contracts consumed by pipeline stages.
///
pub(in crate::db::executor::pipeline::orchestrator) struct LoadExecutionContext {
    pub(in crate::db::executor::pipeline::orchestrator) mode: LoadExecutionMode,
}

impl LoadExecutionContext {
    // Construct one immutable execution context from one normalized mode contract.
    pub(in crate::db::executor::pipeline::orchestrator) const fn new(
        mode: LoadExecutionMode,
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
