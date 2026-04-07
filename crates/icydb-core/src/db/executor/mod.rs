//! Module: db::executor
//! Responsibility: runtime execution boundaries for validated query plans.
//! Does not own: logical query semantics or persistence encoding policy.
//! Boundary: consumes query/access/cursor contracts and drives load/delete/aggregate runtime.

mod aggregate;
mod authority;
mod continuation;
mod covering;
mod delete;
pub(in crate::db::executor) mod diagnostics;
mod executable_plan;
mod explain;
pub(in crate::db) mod group;
mod kernel;
mod mutation;
mod order;
mod pipeline;
mod plan_metrics;
mod plan_validate;
mod preparation;
mod projection;
pub(super) mod route;
mod runtime_context;
mod scan;
mod stream;
mod terminal;
#[cfg(test)]
mod tests;
mod traversal;
mod util;
mod window;

use crate::db::access::{
    LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey, lower_index_prefix_specs,
    lower_index_range_specs,
};

pub(in crate::db) use crate::db::access::{
    ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
};
pub(in crate::db) use aggregate::{
    ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryRequest,
    ScalarTerminalBoundaryRequest,
};
pub use authority::EntityAuthority;
pub(in crate::db::executor) use continuation::{
    AccessWindow, ContinuationCapabilities, ContinuationEngine, ContinuationMode,
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedPaginationWindow,
    LoadCursorInput, PreparedLoadCursor, RequestedLoadExecutionShape, ResolvedLoadCursorContext,
    ResolvedScalarContinuationContext, RouteContinuationPlan, ScalarContinuationBindings,
    ScalarRouteContinuationInvariantProjection,
};
pub(in crate::db::executor) use covering::{
    CoveringMembershipRows, CoveringProjectionComponentRows,
    SingleComponentCoveringProjectionOutcome, SingleComponentCoveringScanRequest,
    collect_single_component_covering_projection_from_lowered_specs,
    collect_single_component_covering_projection_values_from_lowered_specs,
    covering_projection_scan_direction, covering_requires_row_presence_check,
    decode_covering_projection_pairs, decode_single_covering_projection_pairs,
    map_covering_membership_pairs, map_covering_projection_pairs,
    reorder_covering_projection_pairs, resolve_covering_memberships_from_lowered_specs,
    resolve_covering_projection_component_from_lowered_specs,
    resolve_covering_projection_components_from_lowered_specs,
};
pub(super) use delete::DeleteExecutor;
#[cfg(feature = "sql")]
pub(in crate::db) use delete::execute_sql_delete_projection_for_canister;
pub(in crate::db::executor) use diagnostics::{ExecutionOptimization, ExecutionTrace};
pub(in crate::db) use executable_plan::{BytesByProjectionMode, ExecutablePlan, ExecutionStrategy};
pub(in crate::db::executor) use executable_plan::{PreparedAggregatePlan, PreparedLoadPlan};
pub(in crate::db) use explain::{
    assemble_aggregate_terminal_execution_descriptor_with_model,
    assemble_load_execution_node_descriptor_with_model,
    assemble_load_execution_node_descriptor_with_model_store_witness,
    assemble_load_execution_verbose_diagnostics_with_model,
};
pub(in crate::db::executor) use kernel::ExecutionKernel;
pub use mutation::save::MutationMode;
pub(super) use mutation::save::SaveExecutor;
pub(in crate::db::executor) use order::{
    OrderReadableRow, apply_structural_order_window, compare_orderable_row_with_boundary,
    mark_structural_order_slots, resolve_structural_order,
};
pub(super) use pipeline::contracts::LoadExecutor;
pub(in crate::db) use pipeline::contracts::{GroupedCursorPage, PageCursor};
pub(in crate::db::executor) use plan_validate::validate_executor_plan_for_authority;
pub(in crate::db::executor) use preparation::ExecutionPreparation;
pub(in crate::db::executor) use projection::mark_projection_referenced_slots;
#[cfg(all(feature = "sql", feature = "structural-read-metrics"))]
pub use projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "sql")]
pub(in crate::db) use projection::{
    execute_sql_projection_rows_for_canister, execute_sql_projection_text_rows_for_canister,
};
pub(in crate::db) use runtime_context::*;
#[cfg(feature = "structural-read-metrics")]
pub use runtime_context::{RowCheckMetrics, with_row_check_metrics};
#[cfg(all(test, not(feature = "structural-read-metrics")))]
pub(crate) use runtime_context::{RowCheckMetrics, with_row_check_metrics};
pub(super) use stream::access::*;
pub(in crate::db::executor) use stream::key::{
    BudgetedOrderedKeyStream, KeyOrderComparator, KeyStreamLoopControl, OrderedKeyStream,
    OrderedKeyStreamBox, drive_key_stream_with_control_flow, exact_output_key_count_hint,
    key_stream_budget_is_redundant, ordered_key_stream_from_materialized_keys,
};
#[cfg(feature = "sql")]
pub(in crate::db) use terminal::KernelRow;
pub(in crate::db::executor) use util::saturating_row_len;
pub(in crate::db) use window::compute_page_keep_count;

///
/// ExecutionPlan
///
/// Canonical route-to-kernel execution contract for read execution.
/// This is route-owned policy output (mode, hints, fast-path ordering),
/// while `ExecutablePlan` remains the validated query/lowered-spec container.
///

pub(in crate::db::executor) type ExecutionPlan = route::ExecutionRoutePlan;

// Design notes:
// - SchemaInfo is the planner-visible schema (relational attributes). Executors may see
//   additional tuple payload not represented in SchemaInfo.
// - Unsupported or opaque values are treated as incomparable; executor validation may
//   skip type checks for these values.
// - ORDER BY is stable; incomparable values preserve input order.
// - Corruption indicates invalid persisted bytes or store mismatches; invariant violations
//   indicate executor/planner contract breaches.

use crate::{
    db::{CompiledQuery, cursor::CursorPlanError, data::DataKey},
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
    /// attempted against non-load executable plans.
    pub(in crate::db) fn load_execution_descriptor_requires_load_plan() -> Self {
        Self::continuation_cursor_invariant(
            "load execution descriptor requires load-mode executable plans",
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

impl<E> From<CompiledQuery<E>> for ExecutablePlan<E>
where
    E: EntityKind,
{
    fn from(value: CompiledQuery<E>) -> Self {
        value.into_executable()
    }
}
