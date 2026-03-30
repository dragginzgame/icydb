//! Module: db::executor::pipeline::orchestrator::contracts::surface
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::orchestrator::contracts::surface.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    ExecutionTrace,
    pipeline::contracts::{GroupedCursorPage, StructuralCursorPage},
};

///
/// LoadExecutionSurface
///
/// LoadExecutionSurface is the finalized generic-free load output contract for
/// entrypoint wrappers.
/// Scalar payloads remain structural all the way to the entrypoint edge, so the
/// orchestrator no longer boxes them behind `Any`.
///

pub(in crate::db::executor) enum LoadExecutionSurface {
    ScalarPageWithTrace(StructuralCursorPage, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
}
