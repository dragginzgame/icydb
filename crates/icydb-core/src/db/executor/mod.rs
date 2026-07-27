//! Module: db::executor
//! Responsibility: runtime execution boundaries for validated query plans.
//! Does not own: logical query semantics or persistence encoding policy.
//! Boundary: consumes query/access/cursor contracts and drives load/delete/aggregate runtime.

#[cfg(any(test, feature = "sql"))]
mod aggregate;
#[cfg(any(test, feature = "sql"))]
mod authority;
#[cfg(any(test, feature = "sql"))]
mod covering;
#[cfg(any(test, feature = "sql"))]
mod diagnostics;
#[cfg(any(test, feature = "sql-explain"))]
pub(in crate::db) mod explain;
#[cfg(any(test, feature = "sql"))]
mod group;
#[cfg(any(test, feature = "sql"))]
mod index_prefix_cardinality;
#[cfg(any(test, feature = "sql"))]
mod kernel;
mod mutation;
#[cfg(any(test, feature = "sql"))]
mod order;
#[cfg(any(test, feature = "sql"))]
mod pipeline;
#[cfg(any(test, feature = "sql"))]
mod plan_metrics;
#[cfg(any(test, feature = "sql"))]
pub(super) mod planning;
#[cfg(any(test, feature = "sql"))]
mod prepared_execution_plan;
#[cfg(any(test, feature = "sql"))]
mod profiling;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) mod projection;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use planning::route;
#[cfg(any(test, feature = "sql"))]
mod runtime_context;
#[cfg(any(test, feature = "sql"))]
mod scan;
#[cfg(any(test, feature = "sql"))]
mod stream;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) mod terminal;
#[cfg(any(test, feature = "sql"))]
mod traversal;
#[cfg(any(test, feature = "sql"))]
mod util;
#[cfg(any(test, feature = "sql"))]
mod window;

#[cfg(any(test, feature = "sql"))]
use crate::db::access::{
    LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract, LoweredKey,
};

#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use crate::db::access::{
    ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use aggregate::runtime::RuntimeGroupedRow;
#[cfg(all(feature = "diagnostics", any(test, feature = "sql")))]
pub(in crate::db::executor) use aggregate::runtime::{
    GroupedCountFoldMetrics, with_grouped_count_fold_metrics,
};
#[cfg(all(feature = "diagnostics", any(test, feature = "sql")))]
pub(in crate::db) use aggregate::{
    ScalarAggregateTerminalAttribution, with_scalar_aggregate_terminal_attribution,
};
#[cfg(feature = "sql")]
pub(in crate::db) use aggregate::{
    StructuralAggregateRequest, StructuralAggregateTerminal, StructuralAggregateTerminalKind,
};
#[cfg(feature = "sql")]
pub(in crate::db) use aggregate::{
    execute_direct_count_index_prefix_cardinality_for_canister,
    execute_structural_aggregate_rows_for_canister,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use authority::EntityAuthority;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use covering::resolve_covering_projection_components_from_lowered_specs;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use covering::{
    CoveringProjectionComponentRows, covering_projection_scan_direction,
    decode_single_covering_projection_pairs, reorder_covering_projection_pairs,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use covering::{
    decode_covering_projection_component, decode_covering_projection_pairs,
    map_covering_projection_pairs,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use diagnostics::ExecutionOptimization;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use diagnostics::ExecutionTrace;
#[cfg(feature = "sql-explain")]
pub(in crate::db) use explain::{
    assemble_load_execution_node_descriptor_from_route_facts,
    freeze_load_execution_route_facts_for_authority,
};
#[cfg(feature = "sql")]
pub(in crate::db) use index_prefix_cardinality::exact_count_cardinality_prefixes_for_plan;
#[cfg(feature = "sql")]
pub(in crate::db) use index_prefix_cardinality::lowered_index_prefix_cardinality_specs_from_plan;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use index_prefix_cardinality::{
    expand_index_prefix_family_with_exact_child_prefixes, lowered_index_prefix_liveness,
    lowered_index_prefix_liveness_at_generation,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use kernel::ExecutionKernel;
pub(in crate::db) use mutation::{
    commit_delete_row_ops_with_window_for_path,
    commit_structural_save_row_ops_with_window_for_path, validate_structural_accepted_after_image,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use order::{
    BoundedOrderWindow, OrderReadableRow, PendingOrderRows,
    apply_structural_order_window_to_data_rows, compare_orderable_row_with_boundary,
};
#[cfg(feature = "sql")]
pub(in crate::db) use pipeline::contracts::StructuralCursorPage;
#[cfg(feature = "sql")]
pub(in crate::db) use pipeline::contracts::StructuralGroupedProjectionResult;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use pipeline::contracts::{
    AccessScanContinuationInput, AccessStreamBindings,
};
#[cfg(feature = "sql")]
pub(in crate::db) use pipeline::execute_shared_grouped_plan_for_canister;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use pipeline::execute_shared_grouped_plan_for_canister_with_phase_attribution;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use pipeline::{
    GroupedCountAttribution, GroupedExecutePhaseAttribution, GroupedRuntimeAttribution,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use planning::continuation::{
    AccessWindow, ContinuationMode, GroupedContinuationContext, GroupedPaginationWindow,
    RouteContinuationPlan, ScalarContinuationContext,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use planning::preparation::ExecutionPreparation;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use planning::route::ExecutionRoutePlan;
#[cfg(any(test, feature = "sql"))]
pub use planning::route::RouteExecutionMode;
#[cfg(any(test, feature = "sql"))]
pub use prepared_execution_plan::ExecutionFamily;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use prepared_execution_plan::PreparedLoadPlan;
#[cfg(feature = "sql")]
pub(in crate::db) use prepared_execution_plan::SharedPreparedExecutionPlan;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use prepared_execution_plan::SharedPreparedProjectionRuntimeHandoff;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use prepared_execution_plan::{
    PreparedGroupedRuntimeResidents, PreparedScalarPlanCore, PreparedScalarRuntimeHandoff,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use profiling::{
    ExecutionProfileStats, record_aggregation, with_execution_stats_capture,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use profiling::{
    measure_execution_stats_phase, record_key_stream_micros, record_key_stream_yield,
    record_ordering, record_projection, record_rows_after_predicate,
};
#[cfg(feature = "sql")]
pub(in crate::db) use projection::CoveringProjectionMetricsRecorder;
#[cfg(feature = "sql")]
pub(in crate::db) use projection::ProjectionMaterializationMetricsRecorder;
#[cfg(feature = "sql")]
pub(in crate::db) use projection::{
    StructuralProjectionRequest, StructuralProjectionScanBudget,
    eval_compiled_filter_expr_with_required_slot_reader, execute_structural_projection_rows,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use projection::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
#[cfg(all(feature = "diagnostics", any(test, feature = "sql")))]
pub use runtime_context::{RowCheckMetrics, with_row_check_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use runtime_context::{RowCheckMetrics, with_row_check_metrics};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use runtime_context::{
    record_row_check_covering_candidate_seen, record_row_check_index_entry_scanned,
    record_row_check_index_key_owned_entry, record_row_check_index_row_identity_decoded,
    record_row_check_row_emitted, record_row_presence_probe,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use stream::access::PrimaryRangeKeyStream;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use stream::access::TraversalRuntime;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use stream::access::{
    ACCESS_SCAN_CHUNK_ENTRIES, AccessStreamExecutionPolicy, ExecutableAccess, IndexLeafOrderPolicy,
    IndexScan, PrimaryScan, active_lowered_index_prefix_specs, apply_index_scan_chunk_progress,
    branch_stream_chunk_entries, index_predicate_rejects_prefix_components,
    index_stream_chunk_entries_for_remaining, index_stream_output_limit_for_chunk,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use stream::key::{
    KeyOrderComparator, OrderedKeyStream, OrderedKeyStreamBox, exact_output_key_count_hint,
    key_stream_budget_is_redundant, ordered_key_stream_from_materialized_keys,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use stream::{
    FlatMergeOrderedChild, FlatMergeSiblingSet, FlatMergeStream, PrefixSetExecutionShape,
    PrefixSetMergeSafety,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use terminal::RetainedSlotLayout;
#[cfg(all(feature = "diagnostics", any(test, feature = "sql")))]
pub(in crate::db) use terminal::{DirectDataRowPhaseAttribution, KernelRowPhaseAttribution};
#[cfg(all(feature = "diagnostics", any(test, feature = "sql")))]
pub use terminal::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use terminal::{
    ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use terminal::{
    with_direct_data_row_phase_attribution, with_kernel_row_phase_attribution,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use util::apply_data_key_ordered_dedup_window;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use util::{apply_offset_limit_window, saturating_u32_len};

/// Validate plans at executor boundaries using structural entity authority.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn validate_executor_plan_for_authority(
    authority: &EntityAuthority,
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

#[cfg(any(test, feature = "sql"))]
use crate::db::{
    cursor::CursorPlanError, data::DecodedDataStoreKey, query::plan::AccessPlannedQuery,
};
use crate::error::{ErrorClass, ErrorOrigin, InternalError};

///
/// ExecutorPlanError
///
/// Executor-owned plan-surface failures produced during runtime cursor validation.
/// Mapped to query-owned plan errors only at query/session boundaries.
///

#[derive(Debug)]
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) enum ExecutorPlanError {
    Cursor(Box<CursorPlanError>),
}

#[cfg(any(test, feature = "sql"))]
impl ExecutorPlanError {
    /// Construct one executor plan error from one cursor invariant violation.
    pub(in crate::db::executor) fn continuation_cursor_invariant() -> Self {
        Self::from(CursorPlanError::continuation_cursor_invariant())
    }

    /// Construct one executor plan error for grouped cursor preparation
    /// attempted against non-grouped logical plans.
    pub(in crate::db::executor) fn grouped_cursor_preparation_requires_grouped_plan() -> Self {
        Self::continuation_cursor_invariant()
    }

    /// Construct one executor plan error for grouped boundary-arity access
    /// attempted against non-grouped logical plans.
    pub(in crate::db::executor) fn grouped_cursor_boundary_arity_requires_grouped_plan() -> Self {
        Self::continuation_cursor_invariant()
    }

    /// Construct one executor plan error for load-only continuation contracts.
    pub(in crate::db::executor) fn continuation_contract_requires_load_plan() -> Self {
        Self::continuation_cursor_invariant()
    }

    /// Lift one executor plan error into the runtime internal taxonomy.
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Cursor(err) => err.into_internal_error(),
        }
    }
}

#[cfg(any(test, feature = "sql"))]
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

#[derive(Debug)]
pub(in crate::db::executor) enum ExecutorError {
    #[cfg(any(test, feature = "sql"))]
    Corruption {
        origin: ErrorOrigin,
    },

    KeyExists,
}

impl ExecutorError {
    pub(in crate::db::executor) const fn class(&self) -> ErrorClass {
        match self {
            Self::KeyExists => ErrorClass::Conflict,
            #[cfg(any(test, feature = "sql"))]
            Self::Corruption { .. } => ErrorClass::Corruption,
        }
    }

    pub(in crate::db::executor) const fn origin(&self) -> ErrorOrigin {
        match self {
            Self::KeyExists => ErrorOrigin::Store,
            #[cfg(any(test, feature = "sql"))]
            Self::Corruption { origin } => *origin,
        }
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::executor) const fn corruption(origin: ErrorOrigin) -> Self {
        Self::Corruption { origin }
    }

    // Construct a store-origin corruption error with canonical taxonomy.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::executor) const fn store_corruption() -> Self {
        Self::corruption(ErrorOrigin::Store)
    }

    // Construct the canonical missing-row store corruption error.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::executor) const fn missing_row(_key: &DecodedDataStoreKey) -> Self {
        Self::store_corruption()
    }

    // Construct the canonical persisted-row invariant-violation corruption error.
}

/// Construct the canonical executor conflict for an occupied mutation key.
pub(in crate::db) fn mutation_key_exists_error() -> InternalError {
    ExecutorError::KeyExists.into()
}

impl From<ExecutorError> for InternalError {
    fn from(err: ExecutorError) -> Self {
        Self::classified(err.class(), err.origin())
    }
}
