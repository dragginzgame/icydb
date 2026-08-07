//! Module: db::executor
//! Responsibility: runtime execution boundaries for validated query plans.
//! Does not own: logical query semantics or persistence encoding policy.
//! Boundary: consumes query/access/cursor contracts and drives load/delete/aggregate runtime.

mod aggregate;
mod authority;
pub(in crate::db) mod budget;
mod covering;
mod diagnostics;
#[cfg(feature = "sql")]
pub(in crate::db) mod explain;
mod group;
mod index_prefix_cardinality;
mod kernel;
mod mutation;
mod order;
mod pipeline;
mod plan_metrics;
pub(super) mod planning;
mod prepared_execution_plan;
mod profiling;
pub(in crate::db) mod projection;
pub(in crate::db) use planning::route;
mod scan;
mod stream;
pub(in crate::db) mod terminal;
mod traversal;
mod util;
mod window;

use crate::db::access::{
    LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract, LoweredKey,
};

pub(in crate::db) use crate::db::access::{
    ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload,
};
pub(in crate::db) use aggregate::runtime::RuntimeGroupedRow;
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use aggregate::runtime::{
    GroupedCountFoldMetrics, with_grouped_count_fold_metrics,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
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
pub(in crate::db) use authority::EntityAuthority;
pub(in crate::db::executor) use covering::resolve_covering_projection_components_from_lowered_specs;
pub(in crate::db::executor) use covering::{
    covering_projection_scan_direction, decode_single_covering_projection_pairs,
    reorder_covering_projection_pairs,
};
pub(in crate::db::executor) use covering::{
    decode_covering_projection_component, decode_covering_projection_pairs,
    map_covering_projection_pairs,
};
pub(in crate::db) use diagnostics::ExecutionOptimization;
pub(in crate::db::executor) use diagnostics::ExecutionTrace;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use diagnostics::request_query_plan_evidence;
#[cfg(feature = "sql")]
pub(in crate::db) use explain::{
    assemble_load_execution_node_descriptor_from_route_facts,
    freeze_load_execution_route_facts_for_authority,
};
#[cfg(feature = "sql")]
pub(in crate::db) use index_prefix_cardinality::exact_count_cardinality_prefixes_for_plan;
#[cfg(feature = "sql")]
pub(in crate::db) use index_prefix_cardinality::user_index_prefix_cardinality_keys_from_plan;
pub(in crate::db::executor) use index_prefix_cardinality::{
    expand_index_prefix_family_with_exact_child_prefixes, lowered_index_prefix_exact_cardinality,
    lowered_index_prefix_liveness, lowered_index_prefix_liveness_at_generation,
};
pub(in crate::db::executor) use kernel::ExecutionKernel;
pub(in crate::db) use mutation::{
    AcceptedMutationConstraintScheduler, commit_structural_row_ops_with_window_for_path,
};
#[cfg(test)]
pub(in crate::db) use mutation::{
    MutationCommitInterruption, interrupt_next_mutation_commit_for_tests,
};
pub(in crate::db::executor) use order::{
    BoundedOrderWindow, DataRowOrderWindow, OrderReadableRow, PendingOrderRows,
    compare_orderable_row_with_boundary,
};
pub(in crate::db) use pipeline::contracts::StructuralCursorPage;
pub(in crate::db) use pipeline::contracts::StructuralGroupedProjectionResult;
pub(in crate::db::executor) use pipeline::contracts::{
    AccessScanContinuationInput, AccessStreamBindings,
};
pub(in crate::db) use pipeline::execute_shared_grouped_plan_for_canister;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use pipeline::execute_shared_grouped_plan_for_canister_with_phase_attribution;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use pipeline::{
    GroupedCountAttribution, GroupedExecutePhaseAttribution, GroupedRuntimeAttribution,
};
pub(in crate::db) use planning::continuation::ScalarContinuationContext;
pub(in crate::db::executor) use planning::continuation::{
    AccessWindow, ContinuationMode, GroupedContinuationContext, GroupedPaginationWindow,
    RouteContinuationPlan,
};
pub(in crate::db::executor) use planning::preparation::ExecutionPreparation;
pub(in crate::db::executor) use planning::route::ExecutionRoutePlan;
pub use planning::route::RouteExecutionMode;
pub use prepared_execution_plan::ExecutionFamily;
pub(in crate::db::executor) use prepared_execution_plan::PreparedLoadPlan;
pub(in crate::db) use prepared_execution_plan::SharedPreparedExecutionPlan;
pub(in crate::db::executor) use prepared_execution_plan::SharedPreparedProjectionRuntimeHandoff;
pub(in crate::db::executor) use prepared_execution_plan::{
    PreparedGroupedRuntimeResidents, PreparedScalarPlanCore, PreparedScalarRuntimeHandoff,
};
pub(in crate::db::executor) use profiling::{
    ExecutionProfileStats, record_aggregation, with_execution_stats_capture,
};
pub(in crate::db::executor) use profiling::{
    measure_execution_stats_phase, record_key_stream_micros, record_key_stream_yield,
    record_ordering, record_projection, record_rows_after_predicate,
};
pub(in crate::db) use projection::CoveringProjectionMetricsRecorder;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use projection::DistinctProjectionMetricsRecorder;
pub(in crate::db) use projection::ProjectionMaterializationMetricsRecorder;
pub(in crate::db) use projection::{
    StructuralProjectionRequest, execute_structural_projection_page,
};
#[cfg(feature = "sql")]
pub(in crate::db) use projection::{
    StructuralProjectionScanBudget, eval_compiled_filter_expr_with_required_slot_reader,
    execute_structural_projection_rows,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use projection::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
pub(in crate::db::executor) use stream::access::PrimaryRangeKeyStream;
pub(in crate::db::executor) use stream::access::TraversalRuntime;
pub(in crate::db::executor) use stream::access::{
    ACCESS_SCAN_CHUNK_ENTRIES, AccessStreamExecutionPolicy, ExecutableAccess, IndexComponentRow,
    IndexComponentRows, IndexComponentValues, IndexLeafOrderPolicy, IndexScan, PrimaryScan,
    active_lowered_index_prefix_specs, apply_index_scan_chunk_progress,
    branch_stream_chunk_entries, index_predicate_rejects_prefix_components,
    index_stream_chunk_entries_for_remaining, index_stream_output_limit_for_chunk,
};
pub(in crate::db::executor) use stream::key::{
    KeyOrderComparator, OrderedKeyStream, OrderedKeyStreamBox, exact_output_key_count_hint,
    key_stream_budget_is_redundant, ordered_key_stream_from_materialized_keys,
};
pub(in crate::db::executor) use stream::{
    FlatMergeOrderedChild, FlatMergeSiblingSet, FlatMergeStream, PrefixSetExecutionShape,
    PrefixSetMergeSafety,
};
pub(in crate::db) use terminal::PageWorkEnvelope;
pub(in crate::db::executor) use terminal::RetainedSlotLayout;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use terminal::{DirectDataRowPhaseAttribution, KernelRowPhaseAttribution};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use terminal::{
    with_direct_data_row_phase_attribution, with_kernel_row_phase_attribution,
};
pub(in crate::db::executor) use util::apply_data_key_ordered_dedup_window;
pub(in crate::db::executor) use util::{apply_offset_limit_window, saturating_u32_len};

/// Validate plans at executor boundaries using structural entity authority.
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
pub(in crate::db) enum ExecutorPlanError {
    Cursor(Box<CursorPlanError>),
}

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
    Corruption { origin: ErrorOrigin },

    KeyExists,
}

impl ExecutorError {
    pub(in crate::db::executor) const fn class(&self) -> ErrorClass {
        match self {
            Self::KeyExists => ErrorClass::Conflict,
            Self::Corruption { .. } => ErrorClass::Corruption,
        }
    }

    pub(in crate::db::executor) const fn origin(&self) -> ErrorOrigin {
        match self {
            Self::KeyExists => ErrorOrigin::Store,
            Self::Corruption { origin } => *origin,
        }
    }

    pub(in crate::db::executor) const fn corruption(origin: ErrorOrigin) -> Self {
        Self::Corruption { origin }
    }

    // Construct a store-origin corruption error with canonical taxonomy.
    pub(in crate::db::executor) const fn store_corruption() -> Self {
        Self::corruption(ErrorOrigin::Store)
    }

    // Construct the canonical missing-row store corruption error.
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
