//! Module: executor::pipeline::contracts
//! Responsibility: executor-owned scalar/grouped load contract helpers and pagination contracts.
//! Does not own: planner semantics, intent validation, or access-path selection policy.
//! Boundary: consumes planned query contracts and drives load execution helpers.

mod execution;
mod fast_stream;
pub(in crate::db::executor) mod grouped;
mod materialization;
mod post_access;
mod scan;

use crate::{
    db::{
        Db, RuntimeGroupedRow,
        cursor::{ContinuationToken, GroupedContinuationToken},
        direction::Direction,
        executor::{ExecutionOptimization, KeyOrderComparator, OrderedKeyStreamBox},
        response::EntityResponse,
    },
    traits::EntityKind,
};

pub(in crate::db) use execution::StructuralCursorPage;
pub(in crate::db) use execution::StructuralCursorPagePayload;
pub(in crate::db::executor) use execution::{
    CursorEmissionMode, ExecutionInputs, ExecutionOutcomeMetrics, ExecutionRuntimeAdapter,
    MaterializedExecutionAttempt, MaterializedExecutionPayload, PreparedExecutionInputParts,
    PreparedExecutionProjection, ProjectionMaterializationMode, ResolvedExecutionKeyStream,
    RowCollectorMaterializationRequest, RuntimePageMaterializationRequest,
};
pub(in crate::db::executor) use fast_stream::{FastStreamRouteKind, FastStreamRouteRequest};
pub(in crate::db::executor) use grouped::{
    GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage, IndexSpecBundle,
};
pub(in crate::db::executor) use materialization::{
    KernelPageMaterializationRequest, ScalarMaterializationCapabilities,
};
pub(in crate::db::executor) use post_access::PostAccessContract;
pub(in crate::db) use scan::AccessScanContinuationInput;
pub(in crate::db::executor) use scan::AccessStreamBindings;

///
/// PageCursor
///
/// Internal continuation cursor enum for scalar and grouped pagination.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PageCursor {
    Scalar(ContinuationToken),
    Grouped(GroupedContinuationToken),
}

impl PageCursor {
    /// Borrow scalar continuation token when this cursor is scalar-shaped.
    #[must_use]
    pub(in crate::db) const fn as_scalar(&self) -> Option<&ContinuationToken> {
        match self {
            Self::Scalar(token) => Some(token),
            Self::Grouped(_) => None,
        }
    }

    /// Borrow grouped continuation token when this cursor is grouped-shaped.
    #[must_use]
    pub(in crate::db) const fn as_grouped(&self) -> Option<&GroupedContinuationToken> {
        match self {
            Self::Scalar(_) => None,
            Self::Grouped(token) => Some(token),
        }
    }

    /// Encode one cursor into its canonical token bytes.
    #[cfg(test)]
    pub(in crate::db) fn encode(&self) -> Result<Vec<u8>, crate::db::cursor::TokenWireError> {
        match self {
            Self::Scalar(token) => token.encode(),
            Self::Grouped(token) => token.encode(),
        }
    }
}

impl From<ContinuationToken> for PageCursor {
    fn from(value: ContinuationToken) -> Self {
        Self::Scalar(value)
    }
}

impl From<GroupedContinuationToken> for PageCursor {
    fn from(value: GroupedContinuationToken) -> Self {
        Self::Grouped(value)
    }
}

///
/// CursorPage
///
/// Internal load page result with continuation cursor payload.
/// Returned by paged executor entrypoints.
///

#[derive(Debug)]
pub(in crate::db) struct CursorPage<E: EntityKind> {
    pub(in crate::db) items: EntityResponse<E>,
    pub(in crate::db) next_cursor: Option<PageCursor>,
}

///
/// GroupedCursorPage
///
/// Internal grouped page result with grouped rows and continuation cursor payload.
///

#[derive(Debug)]
pub(in crate::db) struct GroupedCursorPage {
    pub(in crate::db) rows: Vec<RuntimeGroupedRow>,
    pub(in crate::db) next_cursor: Option<PageCursor>,
}

/// Resolve key-stream comparator contract from runtime direction.
pub(in crate::db::executor) const fn key_stream_comparator_from_direction(
    direction: Direction,
) -> KeyOrderComparator {
    KeyOrderComparator::from_direction(direction)
}

///
/// FastPathKeyResult
///
/// Internal fast-path access result.
/// Carries ordered keys plus observability metadata for shared execution phases.
///

pub(in crate::db::executor) struct FastPathKeyResult {
    pub(in crate::db::executor) ordered_key_stream: OrderedKeyStreamBox,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) optimization: ExecutionOptimization,
}

///
/// LoadExecutor
///
/// Load-plan executor with canonical post-access semantics.
/// Coordinates fast paths, trace hooks, and pagination cursors.
///

#[derive(Clone)]
pub(in crate::db) struct LoadExecutor<E: EntityKind> {
    pub(in crate::db::executor) db: Db<E::Canister>,
    pub(in crate::db::executor) debug: bool,
}
