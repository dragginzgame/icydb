use crate::{db::executor::RequestedLoadExecutionShape, error::InternalError};

///
/// LoadTracingMode
///
/// Trace emission contract for one load orchestration request.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum LoadTracingMode {
    Disabled,
    Enabled,
}

///
/// LoadMode
///
/// Canonical load pipeline mode selected before staged execution starts.
/// This is the single mode-selection boundary for load orchestration.
///

#[derive(Clone, Copy)]
pub(super) enum LoadMode {
    ScalarRows,
    ScalarPage,
    GroupedPage,
}

///
/// LoadExecutionMode
///
/// Unified load entrypoint mode bundle used by `execute_load`.
/// Encodes one canonical load mode plus tracing contract.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct LoadExecutionMode {
    pub(super) mode: LoadMode,
    pub(super) tracing: LoadTracingMode,
}

impl LoadExecutionMode {
    // Build one scalar unpaged rows mode contract.
    pub(in crate::db::executor) const fn scalar_unpaged_rows() -> Self {
        Self {
            mode: LoadMode::ScalarRows,
            tracing: LoadTracingMode::Disabled,
        }
    }

    // Build one scalar paged mode contract with configurable tracing.
    pub(in crate::db::executor) const fn scalar_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::ScalarPage,
            tracing,
        }
    }

    // Build one grouped paged mode contract with configurable tracing.
    pub(in crate::db::executor) const fn grouped_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::GroupedPage,
            tracing,
        }
    }

    // Validate one mode tuple so wrappers cannot silently drift.
    pub(in crate::db::executor::pipeline::orchestrator) fn validate(
        self,
    ) -> Result<(), InternalError> {
        if matches!(
            (self.mode, self.tracing),
            (LoadMode::ScalarRows, LoadTracingMode::Enabled)
        ) {
            Err(crate::db::error::query_executor_invariant(
                "scalar rows load mode must not request tracing output",
            ))
        } else {
            Ok(())
        }
    }

    // Resolve entrypoint-selected mode into the requested scalar/grouped execution shape.
    pub(in crate::db::executor) const fn requested_shape(self) -> RequestedLoadExecutionShape {
        match self.mode {
            LoadMode::ScalarRows | LoadMode::ScalarPage => RequestedLoadExecutionShape::Scalar,
            LoadMode::GroupedPage => RequestedLoadExecutionShape::Grouped,
        }
    }

    // True when load mode materializes unpaged scalar rows.
    pub(in crate::db::executor::pipeline::orchestrator) const fn scalar_rows_mode(self) -> bool {
        matches!(self.mode, LoadMode::ScalarRows)
    }

    // True when load mode materializes one paged scalar surface.
    pub(in crate::db::executor::pipeline::orchestrator) const fn scalar_page_mode(self) -> bool {
        matches!(self.mode, LoadMode::ScalarPage)
    }

    // True when load mode materializes one grouped paged surface.
    pub(in crate::db::executor::pipeline::orchestrator) const fn grouped_page_mode(self) -> bool {
        matches!(self.mode, LoadMode::GroupedPage)
    }

    // True when load mode should preserve execution trace output.
    pub(in crate::db::executor::pipeline::orchestrator) const fn tracing_enabled(self) -> bool {
        matches!(self.tracing, LoadTracingMode::Enabled)
    }
}
