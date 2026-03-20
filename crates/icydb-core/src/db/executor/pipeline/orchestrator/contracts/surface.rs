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
    ScalarPage(StructuralCursorPage),
    ScalarPageWithTrace(StructuralCursorPage, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
}
