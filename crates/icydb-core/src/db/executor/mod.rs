//! Module: db::executor
//! Responsibility: runtime execution boundaries for validated query plans.
//! Does not own: logical query semantics or persistence encoding policy.
//! Boundary: consumes query/access/cursor contracts and drives load/delete/aggregate runtime.

mod aggregate;
mod authority;
mod continuation;
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
mod traversal;
mod util;
mod window;

use crate::db::access::{
    LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID, LoweredIndexPrefixSpec,
    LoweredIndexRangeSpec, LoweredKey, lower_index_prefix_specs, lower_index_range_specs,
};

pub(in crate::db) use crate::db::access::{
    ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
};
pub(in crate::db) use aggregate::{
    ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryRequest,
    ScalarTerminalBoundaryRequest,
};
pub(in crate::db::executor) use authority::EntityAuthority;
pub(in crate::db::executor) use continuation::{
    AccessWindow, ContinuationCapabilities, ContinuationEngine, ContinuationMode,
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedPaginationWindow,
    LoadCursorInput, PreparedLoadCursor, RequestedLoadExecutionShape, ResolvedLoadCursorContext,
    ResolvedScalarContinuationContext, RouteContinuationPlan, ScalarContinuationBindings,
    ScalarRouteContinuationInvariantProjection,
};
pub(super) use delete::DeleteExecutor;
pub(in crate::db::executor) use diagnostics::{ExecutionOptimization, ExecutionTrace};
pub(in crate::db) use executable_plan::{BytesByProjectionMode, ExecutablePlan, ExecutionStrategy};
pub(in crate::db::executor) use kernel::ExecutionKernel;
pub(super) use mutation::save::SaveExecutor;
pub(in crate::db::executor) use order::{
    OrderReadableRow, apply_structural_order, apply_structural_order_bounded,
    compare_orderable_row_with_boundary, resolve_structural_order,
};
pub(super) use pipeline::contracts::LoadExecutor;
pub(in crate::db::executor) use plan_validate::validate_executor_plan;
pub(in crate::db::executor) use plan_validate::validate_executor_plan_for_authority;
pub(in crate::db::executor) use preparation::ExecutionPreparation;
pub(in crate::db) use runtime_context::*;
pub(super) use stream::access::*;
pub(in crate::db::executor) use stream::key::{
    BudgetedOrderedKeyStream, KeyOrderComparator, KeyStreamLoopControl, OrderedKeyStream,
    OrderedKeyStreamBox, VecOrderedKeyStream, drive_key_stream_with_control_flow,
};
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
