//! Module: db::executor
//! Responsibility: runtime execution boundaries for validated query plans.
//! Does not own: logical query semantics or persistence encoding policy.
//! Boundary: consumes query/access/cursor contracts and drives load/delete/aggregate runtime.

pub(in crate::db) mod access_contract;
mod access_dispatcher;
mod aggregate;
mod context;
mod continuation;
mod delete;
mod executable_plan;
pub(in crate::db) mod group;
mod kernel;
pub(super) mod load;
mod mutation;
mod plan_metrics;
mod plan_validate;
mod preparation;
pub(super) mod route;
mod stream;
#[cfg(test)]
mod tests;
mod trace;
mod traversal;
mod window;

use crate::db::{
    access::{
        LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID,
        LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey, lower_index_prefix_specs,
        lower_index_range_specs,
    },
    cursor::{RangeToken, range_token_anchor_key, range_token_from_lowered_anchor},
};

pub(in crate::db) use access_contract::{
    ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan, ExecutionBounds,
    ExecutionDistinctMode, ExecutionMode, ExecutionOrdering, ExecutionPathKind,
    ExecutionPathPayload,
};
pub(in crate::db::executor) use access_dispatcher::{
    access_plan_metrics_kind, derive_access_capabilities, derive_access_path_capabilities,
    dispatch_access_plan_kind,
};
pub(super) use context::*;
pub(in crate::db::executor) use continuation::ContinuationEngine;
pub(super) use delete::DeleteExecutor;
pub(in crate::db) use executable_plan::ExecutablePlan;
pub(in crate::db::executor) use kernel::{ExecutionKernel, PlanRow};
pub(super) use load::LoadExecutor;
pub(super) use mutation::save::SaveExecutor;
pub(in crate::db::executor) use plan_validate::validate_executor_plan;
pub(in crate::db::executor) use preparation::ExecutionPreparation;
pub(super) use stream::{
    access::*,
    key::{
        BudgetedOrderedKeyStream, KeyOrderComparator, OrderedKeyStream, OrderedKeyStreamBox,
        VecOrderedKeyStream,
    },
};
pub use trace::{ExecutionAccessPathVariant, ExecutionOptimization, ExecutionTrace};
pub(in crate::db) use window::compute_page_window;

///
/// ExecutionPlan
///
/// Canonical route-to-kernel execution contract for read execution.
/// This is route-owned policy output (mode, hints, fast-path ordering),
/// while `ExecutablePlan` remains the validated query/lowered-spec container.
///

pub(in crate::db::executor) type ExecutionPlan = route::ExecutionRoutePlan;

impl<E: EntityKind> From<CompiledQuery<E>> for ExecutablePlan<E> {
    fn from(value: CompiledQuery<E>) -> Self {
        Self::new(value.into_inner())
    }
}

impl<E: EntityKind> CompiledQuery<E> {
    /// Convert one query-owned compiled intent into an executor-ready plan.
    #[must_use]
    pub(in crate::db) fn into_executable(self) -> ExecutablePlan<E> {
        ExecutablePlan::from(self)
    }
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
    db::{cursor::CursorPlanError, data::DataKey, query::intent::CompiledQuery},
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
pub(crate) enum ExecutorPlanError {
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

#[derive(Debug, ThisError)]
pub(crate) enum ExecutorError {
    #[error("corruption detected ({origin}): {message}")]
    Corruption {
        origin: ErrorOrigin,
        message: String,
    },

    #[error("data key exists: {0}")]
    KeyExists(DataKey),
}

impl ExecutorError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::KeyExists(_) => ErrorClass::Conflict,
            Self::Corruption { .. } => ErrorClass::Corruption,
        }
    }

    pub(crate) const fn origin(&self) -> ErrorOrigin {
        match self {
            Self::KeyExists(_) => ErrorOrigin::Store,
            Self::Corruption { origin, .. } => *origin,
        }
    }

    pub(crate) fn corruption(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        Self::Corruption {
            origin,
            message: message.into(),
        }
    }

    // Construct a store-origin corruption error with canonical taxonomy.
    pub(crate) fn store_corruption(message: impl Into<String>) -> Self {
        Self::corruption(ErrorOrigin::Store, message)
    }

    // Construct a serialize-origin corruption error with canonical taxonomy.
    pub(crate) fn serialize_corruption(message: impl Into<String>) -> Self {
        Self::corruption(ErrorOrigin::Serialize, message)
    }
}

impl From<ExecutorError> for InternalError {
    fn from(err: ExecutorError) -> Self {
        Self::classified(err.class(), err.origin(), err.to_string())
    }
}
