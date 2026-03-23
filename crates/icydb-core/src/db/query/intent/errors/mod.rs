//! Module: query::intent::errors
//! Responsibility: query-intent-facing typed error taxonomy and domain conversions.
//! Does not own: planner rule evaluation or runtime execution policy decisions.
//! Boundary: unifies intent/planner/cursor/resource errors into query API error classes.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::{
    db::{
        cursor::CursorPlanError,
        query::plan::{
            CursorPagingPolicyError, FluentLoadPolicyViolation, IntentKeyAccessPolicyViolation,
            PlanError, PlannerError, PolicyPlanError,
        },
        response::ResponseError,
        schema::ValidateError,
    },
    error::{ErrorClass, InternalError},
};
use thiserror::Error as ThisError;

///
/// QueryError
///

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("{0}")]
    Validate(#[from] ValidateError),

    #[error("{0}")]
    Plan(Box<PlanError>),

    #[error("{0}")]
    Intent(#[from] IntentError),

    #[error("{0}")]
    Response(#[from] ResponseError),

    #[error("{0}")]
    Execute(#[from] QueryExecutionError),
}

impl QueryError {
    /// Construct an execution-domain query error from one classified runtime error.
    pub(crate) fn execute(err: InternalError) -> Self {
        Self::Execute(QueryExecutionError::from(err))
    }

    /// Construct one query-origin invariant-violation execution error.
    pub(crate) fn invariant(message: impl Into<String>) -> Self {
        Self::execute(crate::db::error::query_executor_invariant(message))
    }

    /// Construct one intent-domain query error.
    pub(crate) const fn intent(err: IntentError) -> Self {
        Self::Intent(err)
    }

    /// Construct one grouped-query intent error for scalar/query-only surfaces.
    pub(crate) const fn grouped_requires_execute_grouped() -> Self {
        Self::Intent(IntentError::GroupedRequiresExecuteGrouped)
    }

    /// Construct one query-origin unsupported execution error.
    pub(crate) fn unsupported_query(message: impl Into<String>) -> Self {
        Self::execute(InternalError::query_unsupported(message))
    }

    /// Construct one serialize-origin internal execution error.
    pub(crate) fn serialize_internal(message: impl Into<String>) -> Self {
        Self::execute(InternalError::serialize_internal(message))
    }

    /// Construct one query-origin unsupported SQL-feature execution error.
    #[cfg(feature = "sql")]
    pub(crate) fn unsupported_sql_feature(feature: &'static str) -> Self {
        Self::execute(InternalError::query_unsupported_sql_feature(feature))
    }

    /// Construct one invariant violation for scalar pagination emitting the wrong cursor kind.
    pub(crate) fn scalar_paged_emitted_grouped_continuation() -> Self {
        Self::invariant("scalar load pagination emitted grouped continuation token")
    }

    /// Construct one invariant violation for grouped pagination emitting the wrong cursor kind.
    pub(crate) fn grouped_paged_emitted_scalar_continuation() -> Self {
        Self::invariant("grouped pagination emitted scalar continuation token")
    }
}

///
/// QueryExecutionError
///

#[derive(Debug, ThisError)]
pub enum QueryExecutionError {
    #[error("{0}")]
    Corruption(InternalError),

    #[error("{0}")]
    IncompatiblePersistedFormat(InternalError),

    #[error("{0}")]
    InvariantViolation(InternalError),

    #[error("{0}")]
    Conflict(InternalError),

    #[error("{0}")]
    NotFound(InternalError),

    #[error("{0}")]
    Unsupported(InternalError),

    #[error("{0}")]
    Internal(InternalError),
}

impl QueryExecutionError {
    /// Borrow the wrapped classified runtime error.
    #[must_use]
    pub const fn as_internal(&self) -> &InternalError {
        match self {
            Self::Corruption(err)
            | Self::IncompatiblePersistedFormat(err)
            | Self::InvariantViolation(err)
            | Self::Conflict(err)
            | Self::NotFound(err)
            | Self::Unsupported(err)
            | Self::Internal(err) => err,
        }
    }
}

impl From<InternalError> for QueryExecutionError {
    fn from(err: InternalError) -> Self {
        match err.class {
            ErrorClass::Corruption => Self::Corruption(err),
            ErrorClass::IncompatiblePersistedFormat => Self::IncompatiblePersistedFormat(err),
            ErrorClass::InvariantViolation => Self::InvariantViolation(err),
            ErrorClass::Conflict => Self::Conflict(err),
            ErrorClass::NotFound => Self::NotFound(err),
            ErrorClass::Unsupported => Self::Unsupported(err),
            ErrorClass::Internal => Self::Internal(err),
        }
    }
}

impl From<PlannerError> for QueryError {
    fn from(err: PlannerError) -> Self {
        match err {
            PlannerError::Plan(err) => Self::from(*err),
            PlannerError::Internal(err) => Self::execute(*err),
        }
    }
}

impl From<PlanError> for QueryError {
    fn from(err: PlanError) -> Self {
        Self::Plan(Box::new(err))
    }
}

///
/// IntentError
///

#[derive(Clone, Copy, Debug, ThisError)]
pub enum IntentError {
    #[error("{0}")]
    PlanShape(#[from] PolicyPlanError),

    #[error("by_ids() cannot be combined with predicates")]
    ByIdsWithPredicate,

    #[error("only() cannot be combined with predicates")]
    OnlyWithPredicate,

    #[error("multiple key access methods were used on the same query")]
    KeyAccessConflict,

    #[error("{0}")]
    InvalidPagingShape(#[from] PagingIntentError),

    #[error("grouped queries require execute_grouped(...)")]
    GroupedRequiresExecuteGrouped,

    #[error("HAVING requires GROUP BY")]
    HavingRequiresGroupBy,
}

///
/// PagingIntentError
///
/// Canonical intent-level paging contract failures shared by planner and
/// fluent/execution boundary gates.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
#[expect(clippy::enum_variant_names)]
pub enum PagingIntentError {
    #[error(
        "{message}",
        message = CursorPlanError::cursor_requires_order_message()
    )]
    CursorRequiresOrder,

    #[error(
        "{message}",
        message = CursorPlanError::cursor_requires_limit_message()
    )]
    CursorRequiresLimit,

    #[error("cursor tokens can only be used with .page().execute()")]
    CursorRequiresPagedExecution,
}

impl From<CursorPagingPolicyError> for PagingIntentError {
    fn from(err: CursorPagingPolicyError) -> Self {
        match err {
            CursorPagingPolicyError::CursorRequiresOrder => Self::CursorRequiresOrder,
            CursorPagingPolicyError::CursorRequiresLimit => Self::CursorRequiresLimit,
        }
    }
}

impl From<CursorPagingPolicyError> for IntentError {
    fn from(err: CursorPagingPolicyError) -> Self {
        Self::InvalidPagingShape(PagingIntentError::from(err))
    }
}

impl From<IntentKeyAccessPolicyViolation> for IntentError {
    fn from(err: IntentKeyAccessPolicyViolation) -> Self {
        match err {
            IntentKeyAccessPolicyViolation::KeyAccessConflict => Self::KeyAccessConflict,
            IntentKeyAccessPolicyViolation::ByIdsWithPredicate => Self::ByIdsWithPredicate,
            IntentKeyAccessPolicyViolation::OnlyWithPredicate => Self::OnlyWithPredicate,
        }
    }
}

impl From<FluentLoadPolicyViolation> for IntentError {
    fn from(err: FluentLoadPolicyViolation) -> Self {
        match err {
            FluentLoadPolicyViolation::CursorRequiresPagedExecution => {
                Self::InvalidPagingShape(PagingIntentError::CursorRequiresPagedExecution)
            }
            FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped => {
                Self::GroupedRequiresExecuteGrouped
            }
            FluentLoadPolicyViolation::CursorRequiresOrder => {
                Self::InvalidPagingShape(PagingIntentError::CursorRequiresOrder)
            }
            FluentLoadPolicyViolation::CursorRequiresLimit => {
                Self::InvalidPagingShape(PagingIntentError::CursorRequiresLimit)
            }
        }
    }
}
