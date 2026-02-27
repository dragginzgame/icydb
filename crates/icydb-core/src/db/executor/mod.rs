mod aggregate;
mod commit_planner;
mod context;
mod delete;
mod executable_plan;
mod index_predicate;
mod kernel;
pub(super) mod load;
mod mutation;
mod physical_path;
mod plan_metrics;
mod plan_validate;
mod predicate_runtime;
mod preparation;
mod recovery;
pub(super) mod route;
mod stream;
#[cfg(test)]
mod tests;
mod window;

pub(in crate::db::executor) use crate::db::access::{
    LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID, lower_index_prefix_specs,
    lower_index_range_specs,
};
pub(in crate::db) use crate::db::access::{
    LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey,
};
pub(in crate::db::executor) use crate::db::cursor::{
    RangeToken, cursor_anchor_from_index_key, range_token_anchor_key,
    range_token_from_cursor_anchor, range_token_from_lowered_anchor,
};
pub(in crate::db) use commit_planner::prepare_row_commit_for_entity;
pub(super) use context::*;
pub(super) use delete::DeleteExecutor;
pub(in crate::db) use executable_plan::ExecutablePlan;
pub(in crate::db::executor) use index_predicate::{
    IndexPredicateCompileMode, compile_index_predicate_program_from_slots,
};
pub(in crate::db::executor) use kernel::{ExecutionKernel, PlanRow};
pub(super) use load::LoadExecutor;
pub use load::{ExecutionAccessPathVariant, ExecutionOptimization, ExecutionTrace};
pub(super) use mutation::save::SaveExecutor;
pub(in crate::db::executor) use plan_validate::validate_executor_plan;
#[cfg(test)]
pub(in crate::db) use predicate_runtime::eval_compare_values;
pub(in crate::db) use predicate_runtime::{PredicateFieldSlots, eval_with_slots};
pub(in crate::db::executor) use preparation::ExecutionPreparation;
pub(in crate::db) use recovery::{
    rebuild_secondary_indexes_from_rows, replay_commit_marker_row_ops,
};
pub(super) use stream::access::*;
pub(super) use stream::key::{
    BudgetedOrderedKeyStream, KeyOrderComparator, OrderedKeyStream, OrderedKeyStreamBox,
    VecOrderedKeyStream,
};
pub(in crate::db) use window::compute_page_window;

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
    db::{
        contracts::ValidateError,
        cursor::CursorPlanError,
        data::DataKey,
        query::plan::{OrderPlanError, PlanError},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use thiserror::Error as ThisError;

///
/// ExecutorPlanError
///
/// Executor-owned plan-surface failures produced during runtime cursor validation.
/// Mapped to `PlanError` only at query/session boundaries.
///

#[derive(Debug, ThisError)]
pub(crate) enum ExecutorPlanError {
    #[error("{0}")]
    Predicate(Box<ValidateError>),

    #[error("{0}")]
    Order(Box<OrderPlanError>),

    #[error("{0}")]
    Cursor(Box<CursorPlanError>),
}

impl ExecutorPlanError {
    /// Convert an executor-owned plan failure to query-owned `PlanError`.
    #[must_use]
    pub(crate) fn into_plan_error(self) -> PlanError {
        match self {
            Self::Predicate(err) => PlanError::from(*err),
            Self::Order(err) => PlanError::from(*err),
            Self::Cursor(err) => PlanError::from(*err),
        }
    }
}

impl From<ValidateError> for ExecutorPlanError {
    fn from(err: ValidateError) -> Self {
        Self::Predicate(Box::new(err))
    }
}

impl From<OrderPlanError> for ExecutorPlanError {
    fn from(err: OrderPlanError) -> Self {
        Self::Order(Box::new(err))
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

    // Construct a store-origin corruption error from displayable source context.
    pub(crate) fn store_corruption_from(source: impl std::fmt::Display) -> Self {
        Self::store_corruption(source.to_string())
    }
}

impl From<ExecutorError> for InternalError {
    fn from(err: ExecutorError) -> Self {
        Self::classified(err.class(), err.origin(), err.to_string())
    }
}
