//! Module: executor::pipeline::contracts
//! Responsibility: executor-owned scalar/grouped load contract helpers and pagination contracts.
//! Does not own: planner semantics, intent validation, or access-path selection policy.
//! Boundary: consumes planned query contracts and drives load execution helpers.

mod execution;
pub(in crate::db::executor) mod grouped;
mod post_access;

use crate::{
    db::{
        Db, GroupedRow, PersistedRow,
        cursor::{ContinuationToken, GroupedContinuationToken},
        data::DataRow,
        direction::Direction,
        executor::{ExecutionOptimization, KeyOrderComparator, OrderedKeyStreamBox},
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

#[cfg(any(test, feature = "perf-attribution"))]
pub(in crate::db::executor) use execution::StructuralCursorPagePayload;
pub(in crate::db::executor) use execution::{
    CoveringComponentScanState, CursorEmissionMode, DirectCoveringScanMaterializationRequest,
    ExecutionInputs, ExecutionOutcomeMetrics, ExecutionRuntimeAdapter,
    MaterializedExecutionAttempt, MaterializedExecutionPayload, PreparedExecutionProjection,
    ProjectionMaterializationMode, ResolvedExecutionKeyStream, RowCollectorMaterializationRequest,
    RuntimePageMaterializationRequest, StructuralCursorPage,
};
pub(in crate::db::executor) use grouped::{
    GroupedFoldStage, GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage,
    GroupedStreamStage, IndexSpecBundle, RowView, StructuralGroupedRowRuntime,
};
pub(in crate::db::executor) use post_access::PostAccessContract;

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

impl<E> CursorPage<E>
where
    E: PersistedRow + EntityValue,
{
    /// Build one typed cursor page from structural rows plus cursor state.
    pub(in crate::db::executor) fn from_data_rows(
        data_rows: Vec<DataRow>,
        next_cursor: Option<PageCursor>,
    ) -> Result<Self, InternalError> {
        let items = EntityResponse::from_data_rows(data_rows)?;

        Ok(Self { items, next_cursor })
    }
}

impl StructuralCursorPage {
    /// Decode one structural scalar page into one typed entity response.
    pub(in crate::db::executor) fn into_entity_response<E>(
        self,
    ) -> Result<EntityResponse<E>, InternalError>
    where
        E: PersistedRow + EntityValue,
    {
        let (data_rows, _) = self.into_parts();

        EntityResponse::from_data_rows(data_rows)
    }

    /// Decode one structural scalar page into one typed cursor page.
    pub(in crate::db::executor) fn into_cursor_page<E>(self) -> Result<CursorPage<E>, InternalError>
    where
        E: PersistedRow + EntityValue,
    {
        let (data_rows, next_cursor) = self.into_parts();

        CursorPage::from_data_rows(data_rows, next_cursor)
    }
}

///
/// GroupedCursorPage
///
/// Internal grouped page result with grouped rows and continuation cursor payload.
///

#[derive(Debug)]
pub(in crate::db) struct GroupedCursorPage {
    pub(in crate::db) rows: Vec<GroupedRow>,
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
