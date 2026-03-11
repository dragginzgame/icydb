//! Module: db::executor::continuation::engine
//! Responsibility: module-local ownership and contracts for db::executor::continuation::engine.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::{
            ContinuationSignature, GroupedContinuationToken, GroupedPlannedCursor, PlannedCursor,
        },
        direction::Direction,
        executor::{
            ExecutablePlan,
            continuation::scalar::{ResolvedScalarContinuationContext, ScalarContinuationContext},
        },
        query::plan::ExecutionOrdering,
    },
    error::InternalError,
    traits::EntityKind,
    value::Value,
};

///
/// ContinuationEngine
///
/// Executor-owned continuation protocol facade.
/// Centralizes scalar cursor runtime bindings and grouped cursor token emission
/// so executor load paths consume one boundary for runtime continuation payloads.
///

pub(in crate::db::executor) struct ContinuationEngine;

impl ContinuationEngine {
    /// Resolve load mode/order compatibility and cursor revalidation contracts.
    pub(in crate::db::executor) fn resolve_load_cursor_context<E: EntityKind>(
        plan: &ExecutablePlan<E>,
        cursor: LoadCursorInput,
        requested_shape: RequestedLoadExecutionShape,
    ) -> Result<ResolvedLoadCursorContext, InternalError> {
        let ordering = plan.execution_ordering()?;
        match (requested_shape, &ordering) {
            (
                RequestedLoadExecutionShape::Scalar,
                ExecutionOrdering::PrimaryKey | ExecutionOrdering::Explicit(_),
            )
            | (RequestedLoadExecutionShape::Grouped, ExecutionOrdering::Grouped(_)) => {}
            (RequestedLoadExecutionShape::Scalar, ExecutionOrdering::Grouped(_)) => {
                return Err(crate::db::error::query_executor_invariant(
                    "grouped plans require grouped load execution mode",
                ));
            }
            (
                RequestedLoadExecutionShape::Grouped,
                ExecutionOrdering::PrimaryKey | ExecutionOrdering::Explicit(_),
            ) => {
                return Err(crate::db::error::query_executor_invariant(
                    "grouped load execution mode requires grouped logical plans",
                ));
            }
        }

        let cursor = match (requested_shape, cursor) {
            (RequestedLoadExecutionShape::Scalar, LoadCursorInput::Scalar(cursor)) => {
                let cursor = plan.revalidate_cursor(*cursor)?;
                let continuation_signature = plan.continuation_signature_for_runtime()?;
                let resolved = Self::resolve_scalar_context(cursor, continuation_signature);
                PreparedLoadCursor::Scalar(Box::new(resolved))
            }
            (RequestedLoadExecutionShape::Grouped, LoadCursorInput::Grouped(cursor)) => {
                PreparedLoadCursor::Grouped(plan.revalidate_grouped_cursor(cursor)?)
            }
            (RequestedLoadExecutionShape::Scalar, LoadCursorInput::Grouped(_)) => {
                return Err(crate::db::error::query_executor_invariant(
                    "scalar load execution mode requires scalar cursor input",
                ));
            }
            (RequestedLoadExecutionShape::Grouped, LoadCursorInput::Scalar(_)) => {
                return Err(crate::db::error::query_executor_invariant(
                    "grouped load execution mode requires grouped cursor input",
                ));
            }
        };

        Ok(ResolvedLoadCursorContext::new(cursor))
    }

    /// Resolve scalar continuation runtime + signature into one contract object.
    #[must_use]
    pub(in crate::db::executor) fn resolve_scalar_context(
        cursor: PlannedCursor,
        continuation_signature: ContinuationSignature,
    ) -> ResolvedScalarContinuationContext {
        ResolvedScalarContinuationContext::new(
            ScalarContinuationContext::new(cursor),
            continuation_signature,
        )
    }

    /// Build one grouped continuation token for grouped page finalization.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_next_cursor_token(
        continuation_signature: ContinuationSignature,
        last_group_key: Vec<Value>,
        resume_initial_offset: u32,
    ) -> GroupedContinuationToken {
        GroupedContinuationToken::new_with_direction(
            continuation_signature,
            last_group_key,
            Direction::Asc,
            resume_initial_offset,
        )
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

///
/// RequestedLoadExecutionShape
///
/// Requested load execution shape at entrypoint selection time.
/// Used by continuation resolver to validate mode/order compatibility before
/// cursor revalidation occurs.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum RequestedLoadExecutionShape {
    Scalar,
    Grouped,
}

impl LoadCursorInput {
    /// Build scalar load cursor input.
    #[must_use]
    pub(in crate::db::executor) fn scalar(cursor: impl Into<PlannedCursor>) -> Self {
        Self::Scalar(Box::new(cursor.into()))
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
    Scalar(Box<ResolvedScalarContinuationContext>),
    Grouped(GroupedPlannedCursor),
}

///
/// ResolvedLoadCursorContext
///
/// Canonical load cursor resolution output.
/// Carries one revalidated cursor payload so load
/// entrypoint orchestration consumes one resolved contract boundary.
///

pub(in crate::db::executor) struct ResolvedLoadCursorContext {
    cursor: PreparedLoadCursor,
}

impl ResolvedLoadCursorContext {
    /// Construct one resolved load cursor context.
    #[must_use]
    const fn new(cursor: PreparedLoadCursor) -> Self {
        Self { cursor }
    }

    /// Consume context and return revalidated cursor payload.
    #[must_use]
    pub(in crate::db::executor) fn into_cursor(self) -> PreparedLoadCursor {
        self.cursor
    }
}
