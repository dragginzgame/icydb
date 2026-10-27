//! Module: query::intent::errors
//! Responsibility: query-intent-facing typed error taxonomy and domain conversions.
//! Does not own: planner rule evaluation or runtime execution policy decisions.
//! Boundary: unifies intent/planner/cursor/resource errors into query API error classes.

use crate::{
    db::{
        cursor::CursorPlanError,
        query::plan::{PlanError, PlannerError, PolicyPlanError},
        schema::ValidateError,
    },
    error::{COMPACT_QUERY_DIAGNOSTIC_MESSAGE, ErrorClass, InternalError},
};
use icydb_diagnostic_code as diagnostic_code;

///
/// QueryError
///

#[derive(Debug, thiserror::Error)]
#[error("{COMPACT_QUERY_DIAGNOSTIC_MESSAGE}")]
pub enum QueryError {
    Validate(Box<ValidateError>),

    Plan(Box<PlanError>),

    Intent(IntentError),

    Execute(QueryExecutionError),
}

impl QueryError {
    /// Attach planner field context without reclassifying resource failures.
    pub(in crate::db) fn attach_query_field(self, role: diagnostic_code::QueryFieldRole) -> Self {
        match self {
            Self::Plan(mut error) => {
                *error = (*error).attach_query_field(role);
                Self::Plan(error)
            }
            error => error,
        }
    }

    /// Construct one validation-domain query error.
    pub(in crate::db) fn validate(err: ValidateError) -> Self {
        Self::Validate(Box::new(err))
    }

    /// Construct an execution-domain query error from one classified runtime error.
    pub(in crate::db) fn execute(err: InternalError) -> Self {
        Self::Execute(QueryExecutionError::from(err))
    }

    /// Return compact diagnostic identity for this query error.
    #[must_use]
    pub fn diagnostic(&self) -> diagnostic_code::Diagnostic {
        if let Self::Execute(error) = self {
            return error.diagnostic();
        }

        diagnostic_code::Diagnostic::new(
            self.diagnostic_code(),
            self.diagnostic_origin(),
            self.diagnostic_detail(),
        )
    }

    /// Project typed query context into canonical public numeric facts.
    #[must_use]
    pub fn diagnostic_facts(&self) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        match self {
            Self::Plan(error) => error.diagnostic_facts(),
            Self::Execute(error) => error.as_internal().diagnostic_facts(),
            Self::Validate(_) | Self::Intent(_) => Vec::new(),
        }
    }

    /// Borrow the planner-owned rejected-field context, when available.
    #[must_use]
    pub fn query_field_context(&self) -> Option<(diagnostic_code::QueryFieldRole, &str)> {
        match self {
            Self::Plan(error) => error.query_field_context(),
            Self::Validate(_) | Self::Intent(_) | Self::Execute(_) => None,
        }
    }

    /// Return the compact diagnostic code for this query error.
    #[must_use]
    pub fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::Validate(_) => diagnostic_code::DiagnosticCode::QueryValidate,
            Self::Plan(error) if error.is_invalid_continuation_cursor() => {
                diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor
            }
            Self::Plan(error) if error.is_unordered_pagination() => {
                diagnostic_code::DiagnosticCode::QueryUnorderedPagination
            }
            Self::Plan(_) => diagnostic_code::DiagnosticCode::QueryPlan,
            Self::Intent(_) => diagnostic_code::DiagnosticCode::QueryIntent,
            Self::Execute(error) => error.diagnostic_code(),
        }
    }

    fn diagnostic_origin(&self) -> diagnostic_code::ErrorOrigin {
        match self {
            Self::Execute(error) => error.as_internal().origin().diagnostic_origin(),
            Self::Plan(error) if error.is_invalid_continuation_cursor() => {
                diagnostic_code::ErrorOrigin::Cursor
            }
            Self::Validate(_) | Self::Plan(_) | Self::Intent(_) => {
                diagnostic_code::ErrorOrigin::Query
            }
        }
    }

    fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        let kind = match self {
            Self::Validate(_) => diagnostic_code::QueryErrorKind::Validate,
            Self::Plan(error) if error.is_invalid_continuation_cursor() => {
                diagnostic_code::QueryErrorKind::InvalidContinuationCursor
            }
            Self::Plan(error) if error.is_unordered_pagination() => {
                diagnostic_code::QueryErrorKind::UnorderedPagination
            }
            Self::Plan(_) => diagnostic_code::QueryErrorKind::Plan,
            Self::Intent(_) => diagnostic_code::QueryErrorKind::Intent,
            Self::Execute(_) => return None,
        };

        Some(diagnostic_code::DiagnosticDetail::QueryKind { kind })
    }

    /// Construct one query-origin invariant-violation execution error.
    pub(in crate::db) fn invariant() -> Self {
        Self::execute(InternalError::query_executor_invariant())
    }

    /// Construct one intent-domain query error.
    pub(in crate::db) const fn intent(err: IntentError) -> Self {
        Self::Intent(err)
    }

    /// Construct one query-origin unsupported execution error.
    pub(in crate::db) fn unsupported_query() -> Self {
        Self::execute(InternalError::query_unsupported())
    }

    /// Reject an admitted exact count whose metadata is unavailable, without scanning.
    pub(in crate::db) fn exact_count_metadata_unavailable() -> Self {
        Self::execute(InternalError::query_exact_count_metadata_unavailable())
    }

    /// Construct one query error from one cursor plan-surface failure.
    pub(in crate::db) fn from_cursor_plan_error(err: CursorPlanError) -> Self {
        // Internal cursor failures are not caller planning errors. Keep
        // external rejections in PlanError so their numeric facts survive.
        if matches!(err, CursorPlanError::ContinuationCursorInvariantViolation) {
            Self::execute(err.into_internal_error())
        } else {
            Self::from(PlanError::from(err))
        }
    }

    /// Construct one query-origin unsupported projection error.
    pub(in crate::db) fn unsupported_projection(
        reason: diagnostic_code::QueryProjectionCode,
    ) -> Self {
        Self::execute(InternalError::query_unsupported_projection(reason))
    }
}

impl From<IntentError> for QueryError {
    fn from(err: IntentError) -> Self {
        Self::Intent(err)
    }
}

impl From<QueryExecutionError> for QueryError {
    fn from(err: QueryExecutionError) -> Self {
        Self::Execute(err)
    }
}

impl From<diagnostic_code::QueryReadAdmissionCode> for QueryError {
    fn from(reason: diagnostic_code::QueryReadAdmissionCode) -> Self {
        Self::execute(InternalError::from(reason))
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

#[derive(Debug, thiserror::Error)]
#[error("{COMPACT_QUERY_DIAGNOSTIC_MESSAGE}")]
pub enum QueryExecutionError {
    Corruption(InternalError),

    IncompatiblePersistedFormat(InternalError),

    InvariantViolation(InternalError),

    Conflict(InternalError),

    NotFound(InternalError),

    Unsupported(InternalError),

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

    /// Return compact diagnostic identity for this execution error.
    #[must_use]
    pub fn diagnostic(&self) -> diagnostic_code::Diagnostic {
        self.as_internal().diagnostic()
    }

    /// Return the compact diagnostic code for this execution error.
    #[must_use]
    pub fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        self.as_internal().diagnostic_code()
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

#[derive(Clone, Copy, Debug)]
pub enum IntentError {
    PlanShape(PolicyPlanError),

    HavingRequiresGroupBy,

    ScalarTerminalRequiresScalarQuery,

    GroupedTerminalRequiresGroupedQuery,

    GroupedOutputDefinedByGroupAndAggregates,
}

impl From<PolicyPlanError> for IntentError {
    fn from(err: PolicyPlanError) -> Self {
        Self::PlanShape(err)
    }
}

impl IntentError {
    /// Construct one HAVING-requires-GROUP-BY intent error.
    pub(in crate::db::query) const fn having_requires_group_by() -> Self {
        Self::HavingRequiresGroupBy
    }

    /// Construct one scalar-terminal-on-grouped-intent error.
    pub(in crate::db) const fn scalar_terminal_requires_scalar_query() -> Self {
        Self::ScalarTerminalRequiresScalarQuery
    }

    /// Construct one grouped-terminal-on-scalar-intent error.
    pub(in crate::db) const fn grouped_terminal_requires_grouped_query() -> Self {
        Self::GroupedTerminalRequiresGroupedQuery
    }

    /// Construct one grouped-output-with-scalar-selection error.
    pub(in crate::db) const fn grouped_output_defined_by_group_and_aggregates() -> Self {
        Self::GroupedOutputDefinedByGroupAndAggregates
    }
}
