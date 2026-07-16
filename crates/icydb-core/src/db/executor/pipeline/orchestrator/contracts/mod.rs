//! Module: executor::pipeline::orchestrator::contracts
//! Responsibility: canonical mode and erased-surface contracts for load orchestration.
//! Does not own: runtime orchestration mechanics.
//! Boundary: defines stable load-surface semantics consumed by entrypoints
//! and monomorphic runtime wiring.

use crate::{
    db::executor::{
        ExecutionTrace,
        pipeline::contracts::{GroupedCursorPage, StructuralCursorPage},
    },
    error::InternalError,
};

///
/// LoadSurfaceMode
///
/// Canonical load surface selected before staged execution starts.
/// This is the single scalar/grouped surface-selection boundary for load
/// orchestration.
///
#[derive(Clone, Copy)]
pub(in crate::db::executor) enum LoadSurfaceMode {
    ScalarPage,
    GroupedPage,
}

impl LoadSurfaceMode {
    // True when this surface mode materializes one paged scalar surface.
    pub(in crate::db::executor) const fn is_scalar_page(self) -> bool {
        matches!(self, Self::ScalarPage)
    }

    // True when this surface mode materializes one grouped paged surface.
    pub(in crate::db::executor) const fn is_grouped_page(self) -> bool {
        matches!(self, Self::GroupedPage)
    }

    // Fail closed when entrypoint-selected surface mode and projected groupedness disagree.
    pub(in crate::db::executor) fn validate_grouped_ordering(
        self,
        grouped_ordering: bool,
    ) -> Result<(), InternalError> {
        match (self.is_grouped_page(), grouped_ordering) {
            (false, false) | (true, true) => Ok(()),
            (false, true) | (true, false) => Err(InternalError::query_executor_invariant()),
        }
    }
}

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
