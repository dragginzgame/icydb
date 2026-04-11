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
/// LoadTracingMode
///
/// Trace emission contract for one load orchestration request.
///
#[derive(Clone, Copy)]
pub(in crate::db::executor) enum LoadTracingMode {
    Enabled,
}

///
/// LoadSurfaceKind
///
/// Canonical load surface kind selected before staged execution starts.
/// This is the single surface-selection boundary for load orchestration.
///
#[derive(Clone, Copy)]
pub(super) enum LoadSurfaceKind {
    ScalarPage,
    GroupedPage,
}

///
/// LoadSurfaceMode
///
/// Unified load entrypoint surface bundle used by `execute_load`.
/// Encodes one canonical surface kind plus tracing contract.
///
#[derive(Clone, Copy)]
pub(in crate::db::executor) struct LoadSurfaceMode {
    pub(super) kind: LoadSurfaceKind,
    pub(super) tracing: LoadTracingMode,
}

impl LoadSurfaceMode {
    // Build one scalar paged mode contract with configurable tracing.
    pub(in crate::db::executor) const fn scalar_paged(tracing: LoadTracingMode) -> Self {
        Self {
            kind: LoadSurfaceKind::ScalarPage,
            tracing,
        }
    }

    // Build one grouped paged mode contract with configurable tracing.
    pub(in crate::db::executor) const fn grouped_paged(tracing: LoadTracingMode) -> Self {
        Self {
            kind: LoadSurfaceKind::GroupedPage,
            tracing,
        }
    }

    // True when this surface mode materializes one paged scalar surface.
    pub(in crate::db::executor) const fn is_scalar_page(self) -> bool {
        matches!(self.kind, LoadSurfaceKind::ScalarPage)
    }

    // True when this surface mode materializes one grouped paged surface.
    pub(in crate::db::executor) const fn is_grouped_page(self) -> bool {
        matches!(self.kind, LoadSurfaceKind::GroupedPage)
    }

    // True when this surface mode should preserve execution trace output.
    pub(in crate::db::executor::pipeline::orchestrator) const fn tracing_enabled(self) -> bool {
        matches!(self.tracing, LoadTracingMode::Enabled)
    }

    // Fail closed when entrypoint-selected surface mode and projected groupedness disagree.
    pub(in crate::db::executor) fn validate_grouped_ordering(
        self,
        grouped_ordering: bool,
    ) -> Result<(), InternalError> {
        match (self.is_grouped_page(), grouped_ordering) {
            (false, false) | (true, true) => Ok(()),
            (false, true) | (true, false) => Err(self.logical_plan_invariant_error()),
        }
    }

    // Construct the canonical entrypoint/logical-plan mismatch invariant.
    pub(in crate::db::executor) fn logical_plan_invariant_error(self) -> InternalError {
        InternalError::query_executor_invariant(if self.is_scalar_page() {
            "grouped plans require grouped load execution mode"
        } else {
            "grouped load execution mode requires grouped logical plans"
        })
    }

    // Construct the canonical entrypoint/cursor-input mismatch invariant.
    pub(in crate::db::executor) fn cursor_input_invariant_error(self) -> InternalError {
        InternalError::query_executor_invariant(if self.is_scalar_page() {
            "scalar load execution mode requires scalar cursor input"
        } else {
            "grouped load execution mode requires grouped cursor input"
        })
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
