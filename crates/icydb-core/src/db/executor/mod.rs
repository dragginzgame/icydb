//! Module: db::executor
//! Responsibility: runtime execution boundaries for validated query plans.
//! Does not own: logical query semantics or persistence encoding policy.
//! Boundary: consumes query/access/cursor contracts and drives load/delete/aggregate runtime.

mod aggregate;
mod authority;
mod covering;
pub(in crate::db) mod delete;
pub(in crate::db) mod diagnostics;
pub(in crate::db) mod explain;
pub(in crate::db) mod group;
mod kernel;
mod mutation;
mod order;
pub(in crate::db) mod pipeline;
mod plan_metrics;
pub(super) mod planning;
mod prepared_execution_plan;
mod profiling;
pub(in crate::db) mod projection;
pub(in crate::db) use planning::route;
mod runtime_context;
mod scan;
mod stream;
pub(in crate::db) mod terminal;
#[cfg(test)]
mod tests;
mod traversal;
mod util;
mod window;

use crate::db::access::{
    LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey, lower_access,
};

pub(in crate::db) use crate::db::access::{
    ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload,
};
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use aggregate::runtime::{
    GroupedCountFoldMetrics, with_grouped_count_fold_metrics,
};
pub(in crate::db) use aggregate::{
    ProjectedValueAggregateKind, ProjectedValueAggregateRequest, ScalarNumericFieldBoundaryRequest,
    ScalarProjectionBoundaryOutput, ScalarProjectionBoundaryRequest, ScalarTerminalBoundaryOutput,
    ScalarTerminalBoundaryRequest, execute_projected_value_aggregate,
};
pub use authority::EntityAuthority;
pub(in crate::db::executor) use covering::{
    CoveringProjectionComponentRows, covering_requires_row_presence_check,
};
pub(in crate::db) use covering::{
    covering_projection_scan_direction, decode_covering_projection_component,
    decode_covering_projection_pairs, decode_single_covering_projection_pairs,
    map_covering_projection_pairs, reorder_covering_projection_pairs,
    resolve_covering_projection_components_from_lowered_specs,
};
pub(super) use delete::DeleteExecutor;
pub(in crate::db) use diagnostics::ExecutionOptimization;
pub(in crate::db::executor) use diagnostics::ExecutionTrace;
#[cfg(test)]
pub(in crate::db) use explain::assemble_load_execution_node_descriptor;
pub(in crate::db) use explain::{
    assemble_aggregate_terminal_execution_descriptor,
    assemble_load_execution_node_descriptor_from_route_facts,
    assemble_load_execution_verbose_diagnostics_from_route_facts,
    freeze_load_execution_route_facts,
};
pub(in crate::db::executor) use kernel::ExecutionKernel;
pub use mutation::save::MutationMode;
pub(super) use mutation::save::SaveExecutor;
pub(in crate::db::executor) use order::{
    OrderReadableRow, apply_structural_order_window, apply_structural_order_window_to_data_rows,
    compare_orderable_row_with_boundary,
};
pub(in crate::db) use pipeline::contracts::AccessScanContinuationInput;
pub(in crate::db::executor) use pipeline::contracts::AccessStreamBindings;
pub(super) use pipeline::contracts::LoadExecutor;
pub(in crate::db) use pipeline::contracts::{CursorPage, GroupedCursorPage, PageCursor};
pub(in crate::db) use pipeline::contracts::{StructuralCursorPage, StructuralCursorPagePayload};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use pipeline::{
    GroupedCountAttribution, GroupedExecutePhaseAttribution, ScalarExecutePhaseAttribution,
};
pub(in crate::db::executor) use planning::continuation::{
    AccessWindow, ContinuationMode, GroupedContinuationContext, GroupedPaginationWindow,
    LoadCursorInput, LoadCursorResolver, PreparedLoadCursor, RouteContinuationPlan,
    ScalarContinuationContext,
};
pub(in crate::db::executor) use planning::preparation::ExecutionPreparation;
pub use planning::route::RouteExecutionMode;
pub use prepared_execution_plan::ExecutionFamily;
pub(in crate::db) use prepared_execution_plan::{BytesByProjectionMode, PreparedExecutionPlan};
pub(in crate::db::executor) use prepared_execution_plan::{
    PreparedAggregatePlan, PreparedLoadPlan, classify_bytes_by_projection_mode,
};
pub(in crate::db) use prepared_execution_plan::{
    SharedPreparedExecutionPlan, SharedPreparedProjectionRuntimeParts,
};
pub(in crate::db::executor) use profiling::{
    ExecutionProfileStats, measure_execution_stats_phase, record_aggregation,
    record_key_stream_micros, record_key_stream_yield, record_ordering, record_projection,
    record_rows_after_predicate, with_execution_stats_capture,
};
#[cfg(test)]
pub(in crate::db) use projection::PreparedProjectionPlan;
#[cfg(test)]
pub(in crate::db) use projection::projection_eval_data_row_for_materialize_tests;
#[cfg(test)]
pub(in crate::db) use projection::projection_eval_row_layout_for_materialize_tests;
pub(in crate::db) use runtime_context::{
    Context, StoreResolver, record_row_check_covering_candidate_seen,
    record_row_check_index_entry_scanned, record_row_check_index_membership_key_decoded,
    record_row_check_index_membership_multi_key_entry,
    record_row_check_index_membership_single_key_entry, record_row_check_row_emitted,
};
#[cfg(feature = "diagnostics")]
pub use runtime_context::{RowCheckMetrics, with_row_check_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use runtime_context::{RowCheckMetrics, with_row_check_metrics};
pub(in crate::db::executor) use runtime_context::{
    read_data_row_with_consistency_from_store, read_row_presence_with_consistency_from_data_store,
    sum_row_payload_bytes_from_ordered_key_stream_with_store,
    sum_row_payload_bytes_full_scan_window_with_store,
    sum_row_payload_bytes_key_range_window_with_store,
};
pub(in crate::db::executor) use stream::access::{
    ExecutableAccess, IndexScan, PrimaryScan, TraversalRuntime,
};
pub(in crate::db::executor) use stream::key::{
    KeyOrderComparator, KeyStreamLoopControl, OrderedKeyStream, OrderedKeyStreamBox,
    exact_output_key_count_hint, key_stream_budget_is_redundant,
    ordered_key_stream_from_materialized_keys,
};
#[cfg(feature = "sql")]
pub(in crate::db) use terminal::KernelRow;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use terminal::RetainedSlotLayout;
#[cfg(feature = "diagnostics")]
pub use terminal::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use terminal::{
    ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics,
};
pub(in crate::db::executor) use util::saturating_row_len;

///
/// ExecutionPlan
///
/// Canonical route-to-kernel execution contract for read execution.
/// This is route-owned policy output (mode, hints, fast-path ordering),
/// while `PreparedExecutionPlan` remains the validated query/lowered-spec container.
///

pub(in crate::db::executor) type ExecutionPlan = planning::route::ExecutionRoutePlan;

/// Validate plans at executor boundaries using structural entity authority.
pub(in crate::db::executor) fn validate_executor_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    authority.validate_executor_plan(plan)
}

// Design notes:
// - SchemaInfo is the planner-visible schema (relational attributes). Executors may see
//   additional tuple payload not represented in SchemaInfo.
// - Unsupported or opaque values are treated as incomparable; executor validation may
//   skip type checks for these values.
// - ORDER BY is stable; incomparable values preserve input order.
// - Corruption indicates invalid persisted bytes or store mismatches; invariant violations
//   indicate executor/planner contract breaches.

use crate::{
    db::{CompiledQuery, cursor::CursorPlanError, data::DataKey, query::plan::AccessPlannedQuery},
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use thiserror::Error as ThisError;

///
/// ExecutorPlanError
///
/// Executor-owned plan-surface failures produced during runtime cursor validation.
/// Mapped to query-owned plan errors only at query/session boundaries.
///

#[derive(Debug, ThisError)]
pub(in crate::db) enum ExecutorPlanError {
    #[error("{0}")]
    Cursor(Box<CursorPlanError>),
}

impl ExecutorPlanError {
    /// Construct one executor plan error from one cursor invariant violation.
    pub(in crate::db) fn continuation_cursor_invariant(message: impl Into<String>) -> Self {
        Self::from(CursorPlanError::continuation_cursor_invariant(message))
    }

    /// Construct one executor plan error for load-only continuation cursors.
    pub(in crate::db) fn continuation_cursor_requires_load_plan() -> Self {
        Self::continuation_cursor_invariant(
            "continuation cursors are only supported for load plans",
        )
    }

    /// Construct one executor plan error for grouped cursor preparation
    /// attempted against non-grouped logical plans.
    pub(in crate::db) fn grouped_cursor_preparation_requires_grouped_plan() -> Self {
        Self::continuation_cursor_invariant(
            "grouped cursor preparation requires grouped logical plans",
        )
    }

    /// Construct one executor plan error for grouped cursor revalidation
    /// attempted against non-grouped logical plans.
    pub(in crate::db) fn grouped_cursor_revalidation_requires_grouped_plan() -> Self {
        Self::continuation_cursor_invariant(
            "grouped cursor revalidation requires grouped logical plans",
        )
    }

    /// Construct one executor plan error for grouped boundary-arity access
    /// attempted against non-grouped logical plans.
    pub(in crate::db) fn grouped_cursor_boundary_arity_requires_grouped_plan() -> Self {
        Self::continuation_cursor_invariant(
            "grouped cursor boundary arity requires grouped logical plans",
        )
    }

    /// Construct one executor plan error for load-only continuation contracts.
    pub(in crate::db) fn continuation_contract_requires_load_plan() -> Self {
        Self::continuation_cursor_invariant(
            "continuation contracts are only supported for load plans",
        )
    }

    /// Construct one executor plan error for load execution descriptor access
    /// attempted against non-load prepared execution plans.
    pub(in crate::db) fn load_execution_descriptor_requires_load_plan() -> Self {
        Self::continuation_cursor_invariant(
            "load execution descriptor requires load-mode prepared execution plans",
        )
    }

    /// Construct one executor plan error for invalid lowered index-prefix specs.
    pub(in crate::db) fn lowered_index_prefix_spec_invalid() -> Self {
        Self::continuation_cursor_invariant(LoweredIndexPrefixSpec::invalid_reason())
    }

    /// Construct one executor plan error for invalid lowered index-range specs.
    pub(in crate::db) fn lowered_index_range_spec_invalid() -> Self {
        Self::continuation_cursor_invariant(LoweredIndexRangeSpec::invalid_reason())
    }

    /// Lift one executor plan error into the runtime internal taxonomy.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Cursor(err) => err.into_internal_error(),
        }
    }
}

impl From<CursorPlanError> for ExecutorPlanError {
    fn from(err: CursorPlanError) -> Self {
        Self::Cursor(Box::new(err))
    }
}

///
/// ExecutorError
///
/// Executor-owned runtime failure taxonomy for execution boundaries.
/// Keeps conflict vs corruption classification explicit for internal mapping.
/// User-shape validation failures remain plan-layer errors.
///

#[derive(Debug, ThisError)]
pub(in crate::db::executor) enum ExecutorError {
    #[error("corruption detected ({origin}): {message}")]
    Corruption {
        origin: ErrorOrigin,
        message: String,
    },

    #[error("data key exists: {0}")]
    KeyExists(DataKey),
}

impl ExecutorError {
    pub(in crate::db::executor) const fn class(&self) -> ErrorClass {
        match self {
            Self::KeyExists(_) => ErrorClass::Conflict,
            Self::Corruption { .. } => ErrorClass::Corruption,
        }
    }

    pub(in crate::db::executor) const fn origin(&self) -> ErrorOrigin {
        match self {
            Self::KeyExists(_) => ErrorOrigin::Store,
            Self::Corruption { origin, .. } => *origin,
        }
    }

    pub(in crate::db::executor) fn corruption(
        origin: ErrorOrigin,
        message: impl Into<String>,
    ) -> Self {
        Self::Corruption {
            origin,
            message: message.into(),
        }
    }

    // Construct a store-origin corruption error with canonical taxonomy.
    pub(in crate::db::executor) fn store_corruption(message: impl Into<String>) -> Self {
        Self::corruption(ErrorOrigin::Store, message)
    }

    // Construct the canonical missing-row store corruption error.
    pub(in crate::db::executor) fn missing_row(key: &DataKey) -> Self {
        Self::store_corruption(format!("missing row: {key}"))
    }

    // Construct the canonical persisted-row invariant-violation corruption error.
    pub(in crate::db::executor) fn persisted_row_invariant_violation(
        data_key: &DataKey,
        detail: impl AsRef<str>,
    ) -> Self {
        Self::store_corruption(format!(
            "persisted row invariant violation: {data_key} ({})",
            detail.as_ref(),
        ))
    }
}

impl From<ExecutorError> for InternalError {
    fn from(err: ExecutorError) -> Self {
        Self::classified(err.class(), err.origin(), err.to_string())
    }
}

impl<E> From<CompiledQuery<E>> for PreparedExecutionPlan<E>
where
    E: EntityKind,
{
    fn from(value: CompiledQuery<E>) -> Self {
        value.into_prepared_execution_plan()
    }
}
