//! Module: query::intent::errors
//! Responsibility: query-intent-facing typed error taxonomy and domain conversions.
//! Does not own: planner rule evaluation or runtime execution policy decisions.
//! Boundary: unifies intent/planner/cursor/resource errors into query API error classes.

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(feature = "sql")]
use crate::db::sql::{lowering::SqlLoweringError, parser::SqlParseError};
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
    Validate(Box<ValidateError>),

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
    /// Construct one validation-domain query error.
    pub(crate) fn validate(err: ValidateError) -> Self {
        Self::Validate(Box::new(err))
    }

    /// Construct an execution-domain query error from one classified runtime error.
    pub(crate) fn execute(err: InternalError) -> Self {
        Self::Execute(QueryExecutionError::from(err))
    }

    /// Construct one query-origin invariant-violation execution error.
    pub(crate) fn invariant(message: impl Into<String>) -> Self {
        Self::execute(InternalError::query_executor_invariant(message))
    }

    /// Construct one invariant for prepared SELECT lowering that lost SELECT shape.
    #[cfg(feature = "sql")]
    pub(crate) fn prepared_sql_select_lane_mismatch() -> Self {
        Self::invariant("compiled SQL SELECT lane must lower to lowered SQL SELECT")
    }

    /// Construct one invariant for prepared DELETE lowering that lost DELETE shape.
    #[cfg(feature = "sql")]
    pub(crate) fn prepared_sql_delete_lane_mismatch() -> Self {
        Self::invariant("compiled SQL DELETE lane must lower to lowered SQL DELETE")
    }

    /// Construct one invariant for prepared INSERT extraction that lost INSERT shape.
    #[cfg(feature = "sql")]
    pub(crate) fn prepared_sql_insert_lane_mismatch() -> Self {
        Self::invariant("prepared SQL INSERT compilation must preserve INSERT statement ownership")
    }

    /// Construct one invariant for prepared INSERT SELECT extraction that lost SELECT source shape.
    #[cfg(feature = "sql")]
    pub(crate) fn prepared_sql_insert_select_source_mismatch() -> Self {
        Self::invariant(
            "prepared SQL INSERT SELECT compilation must preserve SELECT source ownership",
        )
    }

    /// Construct one invariant for prepared UPDATE extraction that lost UPDATE shape.
    #[cfg(feature = "sql")]
    pub(crate) fn prepared_sql_update_lane_mismatch() -> Self {
        Self::invariant("prepared SQL UPDATE compilation must preserve UPDATE statement ownership")
    }

    /// Construct one intent-domain query error.
    pub(crate) const fn intent(err: IntentError) -> Self {
        Self::Intent(err)
    }

    /// Construct one query-origin unsupported execution error.
    pub(crate) fn unsupported_query(message: impl Into<String>) -> Self {
        Self::execute(InternalError::query_unsupported(message))
    }

    /// Construct one serialize-origin internal execution error.
    pub(crate) fn serialize_internal(message: impl Into<String>) -> Self {
        Self::execute(InternalError::serialize_internal(message))
    }

    /// Construct one query error from one cursor plan-surface failure.
    pub(in crate::db) fn from_cursor_plan_error(err: CursorPlanError) -> Self {
        Self::from(PlanError::from(err))
    }

    /// Construct one query-origin unsupported SQL-feature execution error.
    #[cfg(feature = "sql")]
    pub(crate) fn unsupported_sql_feature(feature: &'static str) -> Self {
        Self::execute(InternalError::query_unsupported_sql_feature(feature))
    }

    /// Construct one query error from one SQL lowering failure.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_sql_lowering_error(err: SqlLoweringError) -> Self {
        match err {
            SqlLoweringError::Query(err) => *err,
            SqlLoweringError::Parse(SqlParseError::UnsupportedFeature { feature }) => {
                Self::unsupported_sql_feature(feature)
            }
            SqlLoweringError::UnexpectedQueryLaneStatement => {
                Self::unsupported_query_lane_sql_statement()
            }
            other => Self::unsupported_query(format!(
                "SQL query is not executable in this release: {other}"
            )),
        }
    }

    /// Construct one query error from one reduced SQL parse failure.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_sql_parse_error(err: SqlParseError) -> Self {
        Self::from_sql_lowering_error(SqlLoweringError::Parse(err))
    }

    /// Construct one unsupported query-lane SQL statement error.
    #[cfg(feature = "sql")]
    pub(crate) fn unsupported_query_lane_sql_statement() -> Self {
        Self::unsupported_query(
            "query-lane SQL execution only accepts SELECT, DELETE, and EXPLAIN statements",
        )
    }

    /// Construct one unsupported aggregate target-field query error.
    pub(crate) fn unknown_aggregate_target_field(field: &str) -> Self {
        Self::unsupported_query(format!("unknown aggregate target field: {field}"))
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

impl From<ValidateError> for QueryError {
    fn from(err: ValidateError) -> Self {
        Self::validate(err)
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

    #[error("grouped queries execute via execute()")]
    GroupedRequiresDirectExecute,

    #[error("HAVING requires GROUP BY")]
    HavingRequiresGroupBy,

    #[error("HAVING references an unknown grouped aggregate output")]
    HavingReferencesUnknownAggregate,
}

impl IntentError {
    /// Construct one by-ids-with-predicate intent error.
    pub(crate) const fn by_ids_with_predicate() -> Self {
        Self::ByIdsWithPredicate
    }

    /// Construct one only-with-predicate intent error.
    pub(crate) const fn only_with_predicate() -> Self {
        Self::OnlyWithPredicate
    }

    /// Construct one key-access-conflict intent error.
    pub(crate) const fn key_access_conflict() -> Self {
        Self::KeyAccessConflict
    }

    /// Construct one invalid-paging-shape intent error.
    pub(crate) const fn invalid_paging_shape(err: PagingIntentError) -> Self {
        Self::InvalidPagingShape(err)
    }

    /// Construct one cursor-requires-order intent error.
    pub(crate) const fn cursor_requires_order() -> Self {
        Self::invalid_paging_shape(PagingIntentError::cursor_requires_order())
    }

    /// Construct one cursor-requires-limit intent error.
    pub(crate) const fn cursor_requires_limit() -> Self {
        Self::invalid_paging_shape(PagingIntentError::cursor_requires_limit())
    }

    /// Construct one cursor-requires-paged-execution intent error.
    pub(crate) const fn cursor_requires_paged_execution() -> Self {
        Self::invalid_paging_shape(PagingIntentError::cursor_requires_paged_execution())
    }

    /// Construct one grouped-requires-direct-execute intent error.
    pub(crate) const fn grouped_requires_direct_execute() -> Self {
        Self::GroupedRequiresDirectExecute
    }

    /// Construct one HAVING-requires-GROUP-BY intent error.
    pub(crate) const fn having_requires_group_by() -> Self {
        Self::HavingRequiresGroupBy
    }

    /// Construct one unknown-grouped-aggregate HAVING intent error.
    pub(crate) const fn having_references_unknown_aggregate() -> Self {
        Self::HavingReferencesUnknownAggregate
    }
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

impl PagingIntentError {
    /// Construct one cursor-requires-order paging intent error.
    pub(crate) const fn cursor_requires_order() -> Self {
        Self::CursorRequiresOrder
    }

    /// Construct one cursor-requires-limit paging intent error.
    pub(crate) const fn cursor_requires_limit() -> Self {
        Self::CursorRequiresLimit
    }

    /// Construct one cursor-requires-paged-execution paging intent error.
    pub(crate) const fn cursor_requires_paged_execution() -> Self {
        Self::CursorRequiresPagedExecution
    }
}

impl From<CursorPagingPolicyError> for PagingIntentError {
    fn from(err: CursorPagingPolicyError) -> Self {
        match err {
            CursorPagingPolicyError::CursorRequiresOrder => Self::cursor_requires_order(),
            CursorPagingPolicyError::CursorRequiresLimit => Self::cursor_requires_limit(),
        }
    }
}

impl From<CursorPagingPolicyError> for IntentError {
    fn from(err: CursorPagingPolicyError) -> Self {
        match err {
            CursorPagingPolicyError::CursorRequiresOrder => Self::cursor_requires_order(),
            CursorPagingPolicyError::CursorRequiresLimit => Self::cursor_requires_limit(),
        }
    }
}

impl From<IntentKeyAccessPolicyViolation> for IntentError {
    fn from(err: IntentKeyAccessPolicyViolation) -> Self {
        match err {
            IntentKeyAccessPolicyViolation::KeyAccessConflict => Self::key_access_conflict(),
            IntentKeyAccessPolicyViolation::ByIdsWithPredicate => Self::by_ids_with_predicate(),
            IntentKeyAccessPolicyViolation::OnlyWithPredicate => Self::only_with_predicate(),
        }
    }
}

impl From<FluentLoadPolicyViolation> for IntentError {
    fn from(err: FluentLoadPolicyViolation) -> Self {
        match err {
            FluentLoadPolicyViolation::CursorRequiresPagedExecution => {
                Self::cursor_requires_paged_execution()
            }
            FluentLoadPolicyViolation::GroupedRequiresDirectExecute => {
                Self::grouped_requires_direct_execute()
            }
            FluentLoadPolicyViolation::CursorRequiresOrder => Self::cursor_requires_order(),
            FluentLoadPolicyViolation::CursorRequiresLimit => Self::cursor_requires_limit(),
        }
    }
}
