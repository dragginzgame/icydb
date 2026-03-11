//! Module: db::executor::pipeline::orchestrator::state
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::orchestrator::state.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        ExecutablePlan, ExecutionTrace, PreparedLoadCursor,
        pipeline::contracts::{CursorPage, GroupedCursorPage},
        pipeline::orchestrator::LoadExecutionMode,
    },
    traits::EntityKind,
};

///
/// LoadExecutionContext
///
/// Canonical execution artifacts normalized before staged orchestration.
/// Owns immutable entrypoint mode contracts consumed by pipeline stages.
///

pub(super) struct LoadExecutionContext {
    pub(super) mode: LoadExecutionMode,
}

impl LoadExecutionContext {
    // Construct one immutable execution context from one normalized mode contract.
    pub(super) const fn new(mode: LoadExecutionMode) -> Self {
        Self { mode }
    }
}

///
/// LoadAccessInputs
///
/// Access-stage payload extracted from execution context.
/// Carries normalized plan/cursor artifacts into grouping/projection stage.
///

pub(super) struct LoadAccessInputs<E: EntityKind> {
    pub(super) plan: ExecutablePlan<E>,
    pub(super) cursor: PreparedLoadCursor,
}

///
/// LoadAccessState
///
/// Access-stage execution artifacts for one load orchestration pass.
/// Carries normalized context and one required access-stage payload.
///

pub(super) struct LoadAccessState<E: EntityKind> {
    pub(super) context: LoadExecutionContext,
    pub(super) access_inputs: LoadAccessInputs<E>,
}

///
/// LoadPayloadState
///
/// Payload-stage execution artifacts for one load orchestration pass.
/// Carries normalized context, one required payload, and optional trace output.
///

pub(super) struct LoadPayloadState<E: EntityKind> {
    pub(super) context: LoadExecutionContext,
    pub(super) payload: LoadExecutionPayload<E>,
    pub(super) trace: Option<ExecutionTrace>,
}

///
/// LoadExecutionPayload
///
/// Canonical payload envelope produced by one load orchestration pass.
///

pub(super) enum LoadExecutionPayload<E: EntityKind> {
    Scalar(CursorPage<E>),
    Grouped(GroupedCursorPage),
}
