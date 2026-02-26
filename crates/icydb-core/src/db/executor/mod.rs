pub(super) mod aggregate_model;
mod context;
mod cursor;
mod delete;
mod executable_plan;
mod execution_plan;
mod kernel;
pub(super) mod load;
mod mutation;
mod physical_path;
mod plan_metrics;
mod plan_validate;
mod preparation;
pub(super) mod route;
mod stream;
#[cfg(test)]
mod tests;
mod window;

pub(in crate::db) use crate::db::lowering::{LoweredIndexPrefixSpec, LoweredIndexRangeSpec};
pub(super) use context::*;
pub(in crate::db) use cursor::{
    PlannedCursor, decode_pk_cursor_boundary, decode_typed_primary_key_cursor_slot, prepare_cursor,
    revalidate_cursor, validate_index_range_anchor,
    validate_index_range_boundary_anchor_consistency,
};
pub(super) use delete::DeleteExecutor;
pub(in crate::db) use executable_plan::ExecutablePlan;
pub(in crate::db::executor) use execution_plan::ExecutionPlan;
pub(in crate::db::executor) use kernel::{
    ExecutionKernel, IndexPredicateCompileMode, PlanRow, PostAccessStats,
};
pub(super) use load::LoadExecutor;
pub use load::{ExecutionAccessPathVariant, ExecutionOptimization, ExecutionTrace};
pub(super) use mutation::save::SaveExecutor;
pub(in crate::db::executor) use plan_validate::validate_executor_plan;
pub(in crate::db::executor) use preparation::ExecutionPreparation;
pub(super) use stream::access::*;
pub(super) use stream::key::{
    BudgetedOrderedKeyStream, KeyOrderComparator, OrderedKeyStream, OrderedKeyStreamBox,
    VecOrderedKeyStream,
};
pub(in crate::db) use window::compute_page_window;

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
        cursor::CursorPlanError,
        data::DataKey,
        query::{
            plan::{OrderPlanError, PlanError},
            predicate::ValidateError,
        },
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
