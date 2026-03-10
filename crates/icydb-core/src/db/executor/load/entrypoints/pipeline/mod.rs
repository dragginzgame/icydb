//! Module: executor::load::entrypoints::pipeline
//! Responsibility: load entrypoint mode/output contracts for staged orchestration.
//! Does not own: row materialization mechanics or continuation cursor resolution internals.
//! Boundary: defines stable entrypoint-facing load mode and response envelopes.

mod orchestrate;

use crate::{
    db::{
        executor::{
            ExecutionTrace, RequestedLoadExecutionShape,
            load::{CursorPage, GroupedCursorPage},
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityKind,
};

#[cfg(test)]
pub(in crate::db::executor) use orchestrate::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};

///
/// LoadTracingMode
///
/// Trace emission contract for one load orchestration request.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor::load) enum LoadTracingMode {
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
pub(in crate::db::executor::load) struct LoadExecutionMode {
    pub(super) mode: LoadMode,
    pub(super) tracing: LoadTracingMode,
}

impl LoadExecutionMode {
    // Build one scalar unpaged rows mode contract.
    pub(in crate::db::executor::load) const fn scalar_unpaged_rows() -> Self {
        Self {
            mode: LoadMode::ScalarRows,
            tracing: LoadTracingMode::Disabled,
        }
    }

    // Build one scalar paged mode contract with configurable tracing.
    pub(in crate::db::executor::load) const fn scalar_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::ScalarPage,
            tracing,
        }
    }

    // Build one grouped paged mode contract with configurable tracing.
    pub(in crate::db::executor::load) const fn grouped_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::GroupedPage,
            tracing,
        }
    }

    // Validate one mode tuple so wrappers cannot silently drift.
    pub(super) fn validate(self) -> Result<(), InternalError> {
        if matches!(
            (self.mode, self.tracing),
            (LoadMode::ScalarRows, LoadTracingMode::Enabled)
        ) {
            Err(InternalError::query_executor_invariant(
                "scalar rows load mode must not request tracing output",
            ))
        } else {
            Ok(())
        }
    }

    // Resolve entrypoint-selected mode into the requested scalar/grouped execution shape.
    pub(in crate::db::executor::load) const fn requested_shape(
        self,
    ) -> RequestedLoadExecutionShape {
        match self.mode {
            LoadMode::ScalarRows | LoadMode::ScalarPage => RequestedLoadExecutionShape::Scalar,
            LoadMode::GroupedPage => RequestedLoadExecutionShape::Grouped,
        }
    }
}

///
/// LoadExecutionSurface
///
/// Finalized load output surface for entrypoint wrappers.
/// Encodes one terminal response shape so wrapper adapters do not carry
/// payload/trace pairing branches.
///

pub(in crate::db::executor::load) enum LoadExecutionSurface<E: EntityKind> {
    ScalarRows(EntityResponse<E>),
    ScalarPage(CursorPage<E>),
    ScalarPageWithTrace(CursorPage<E>, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
}
