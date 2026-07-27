//! Module: executor::pipeline::contracts
//! Responsibility: executor-owned scalar/grouped load contract helpers and pagination contracts.
//! Does not own: planner semantics, intent validation, or access-path selection policy.
//! Boundary: consumes planned query contracts and drives load execution helpers.

mod execution;
mod fast_stream;
pub(in crate::db::executor) mod grouped;
mod materialization;
mod scan;

#[cfg(feature = "sql")]
use crate::db::executor::saturating_u32_len;
use crate::db::{
    cursor::GroupedContinuationToken,
    direction::Direction,
    executor::{ExecutionOptimization, KeyOrderComparator, OrderedKeyStreamBox, RuntimeGroupedRow},
    schema::AcceptedValueCatalogHandle,
};

#[cfg(feature = "sql")]
pub(in crate::db::executor) use execution::KernelRowsExecutionAttempt;
pub(in crate::db) use execution::StructuralCursorPage;
pub(in crate::db::executor) use execution::{
    CursorEmissionMode, ExecutionInputs, ExecutionOutcomeMetrics, ExecutionRuntimeAdapter,
    MaterializedExecutionAttempt, PreparedExecutionInputContext, PreparedExecutionProjection,
    ProjectionMaterializationMode, ResolvedExecutionKeyStream, RowCollectorMaterializationRequest,
};
pub(in crate::db::executor) use fast_stream::{FastStreamRouteKind, FastStreamRouteRequest};
pub(in crate::db::executor) use grouped::{
    GroupedPlannerPayload, GroupedRouteStage, IndexSpecBundle,
};
pub(in crate::db::executor) use materialization::{
    KernelPageMaterializationRequest, ScalarMaterializationCapabilities,
};
pub(in crate::db::executor) use scan::{AccessScanContinuationInput, AccessStreamBindings};

///
/// GroupedCursorPage
///
/// Internal grouped page result with grouped rows and continuation cursor payload.
///

#[derive(Debug)]
pub(in crate::db::executor) struct GroupedCursorPage {
    pub(in crate::db::executor) rows: Vec<RuntimeGroupedRow>,
    pub(in crate::db::executor) next_cursor: Option<GroupedContinuationToken>,
}

///
/// StructuralGroupedProjectionResult
///
/// StructuralGroupedProjectionResult is the executor-owned transport wrapper
/// for grouped projection rows. It preserves grouped cursor-page internals
/// behind a narrow consumptive boundary for adapter-level DTO shaping.
///

#[derive(Debug)]
pub(in crate::db) struct StructuralGroupedProjectionResult {
    page: GroupedCursorPage,
    value_catalog: AcceptedValueCatalogHandle,
}

impl StructuralGroupedProjectionResult {
    /// Wrap one grouped cursor page behind the structural grouped boundary.
    #[must_use]
    pub(in crate::db::executor) const fn from_page(
        page: GroupedCursorPage,
        value_catalog: AcceptedValueCatalogHandle,
    ) -> Self {
        Self {
            page,
            value_catalog,
        }
    }

    /// Return the grouped row count computed at the executor boundary.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn row_count(&self) -> u32 {
        saturating_u32_len(self.page.rows.len())
    }

    /// Consume the structural grouped result into runtime rows plus the grouped
    /// continuation cursor carrier for session response finalization.
    #[must_use]
    pub(in crate::db) fn into_rows_and_cursor(
        self,
    ) -> (
        Vec<RuntimeGroupedRow>,
        Option<GroupedContinuationToken>,
        AcceptedValueCatalogHandle,
    ) {
        let Self {
            page,
            value_catalog,
        } = self;

        (page.rows, page.next_cursor, value_catalog)
    }
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
    pub(in crate::db::executor) rows_scanned: Option<usize>,
    pub(in crate::db::executor) optimization: ExecutionOptimization,
}
