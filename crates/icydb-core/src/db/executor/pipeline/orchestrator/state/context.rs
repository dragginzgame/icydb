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

pub(in crate::db::executor::pipeline::orchestrator) struct LoadAccessInputs<E: EntityKind> {
    pub(in crate::db::executor::pipeline::orchestrator) plan: ExecutablePlan<E>,
    pub(in crate::db::executor::pipeline::orchestrator) cursor: PreparedLoadCursor,
}

///
/// LoadAccessState
///
/// Access-stage execution artifacts for one load orchestration pass.
/// Carries normalized context and one required access-stage payload.
///

pub(in crate::db::executor::pipeline::orchestrator) struct LoadAccessState<E: EntityKind> {
    pub(in crate::db::executor::pipeline::orchestrator) context: LoadExecutionContext,
    pub(in crate::db::executor::pipeline::orchestrator) access_inputs: LoadAccessInputs<E>,
}

///
/// LoadPayloadState
///
/// Payload-stage execution artifacts for one load orchestration pass.
/// Carries normalized context, one required payload, and optional trace output.
///

pub(in crate::db::executor::pipeline::orchestrator) struct LoadPayloadState<E: EntityKind> {
    pub(in crate::db::executor::pipeline::orchestrator) context: LoadExecutionContext,
    pub(in crate::db::executor::pipeline::orchestrator) payload: LoadExecutionPayload<E>,
    pub(in crate::db::executor::pipeline::orchestrator) trace: Option<ExecutionTrace>,
}

///
/// LoadExecutionPayload
///
/// Canonical payload envelope produced by one load orchestration pass.
///

pub(in crate::db::executor::pipeline::orchestrator) enum LoadExecutionPayload<E: EntityKind> {
    Scalar(CursorPage<E>),
    Grouped(GroupedCursorPage),
}
