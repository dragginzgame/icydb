//! Module: db::executor::planning::continuation::engine
//! Defines continuation-window helpers used by executor runtime loops.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::planning::continuation::scalar::ScalarContinuationContext,
        executor::{PreparedLoadPlan, pipeline::orchestrator::LoadSurfaceMode},
        query::plan::ExecutionOrdering,
    },
    error::InternalError,
};

///
/// LoadCursorResolver
///
/// Executor-owned load-cursor resolution boundary.
/// This type owns only entrypoint load-cursor validation/revalidation against
/// prepared load plans. It no longer owns grouped token emission or general
/// continuation construction helpers.
///

pub(in crate::db::executor) struct LoadCursorResolver;

impl LoadCursorResolver {
    /// Resolve load surface/order compatibility and cursor revalidation contracts.
    pub(in crate::db::executor) fn resolve_load_cursor_context(
        plan: &PreparedLoadPlan,
        cursor: LoadCursorInput,
        execution_mode: LoadSurfaceMode,
    ) -> Result<PreparedLoadCursor, InternalError> {
        let ordering = plan.execution_ordering()?;
        execution_mode
            .validate_grouped_ordering(matches!(ordering, ExecutionOrdering::Grouped(_)))?;

        let cursor = match (execution_mode.is_scalar_page(), cursor) {
            (true, LoadCursorInput::Scalar(cursor)) => {
                let cursor = plan.revalidate_cursor(*cursor)?;
                let continuation_signature = plan.continuation_signature_for_runtime()?;
                PreparedLoadCursor::Scalar(Box::new(ScalarContinuationContext::for_runtime(
                    cursor,
                    continuation_signature,
                )))
            }
            (false, LoadCursorInput::Grouped(cursor)) => {
                PreparedLoadCursor::Grouped(plan.revalidate_grouped_cursor(cursor)?)
            }
            (true, LoadCursorInput::Grouped(_)) | (false, LoadCursorInput::Scalar(_)) => {
                return Err(execution_mode.cursor_input_invariant_error());
            }
        };

        Ok(cursor)
    }
}

///
/// LoadCursorInput
///
/// Load-entrypoint cursor input contract passed into continuation resolver
/// before runtime ordering/shape compatibility checks.
///

pub(in crate::db::executor) enum LoadCursorInput {
    Scalar(Box<PlannedCursor>),
    Grouped(GroupedPlannedCursor),
}

impl LoadCursorInput {
    /// Build scalar load cursor input.
    #[must_use]
    pub(in crate::db::executor) fn scalar(cursor: PlannedCursor) -> Self {
        Self::Scalar(Box::new(cursor))
    }

    /// Build grouped load cursor input.
    #[must_use]
    pub(in crate::db::executor) fn grouped(cursor: impl Into<GroupedPlannedCursor>) -> Self {
        Self::Grouped(cursor.into())
    }
}

///
/// PreparedLoadCursor
///
/// Revalidated load cursor contract returned by continuation resolver.
///

pub(in crate::db::executor) enum PreparedLoadCursor {
    Scalar(Box<ScalarContinuationContext>),
    Grouped(GroupedPlannedCursor),
}
