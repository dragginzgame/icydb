//! Module: query::intent::errors
//! Responsibility: query-intent-facing typed error taxonomy and domain conversions.
//! Does not own: planner rule evaluation or runtime execution policy decisions.
//! Boundary: unifies intent/planner/cursor/resource errors into query API error classes.

#[cfg(feature = "sql")]
use crate::db::query::plan::validate::ExprPlanError;
#[cfg(feature = "sql")]
use crate::db::sql::{ddl::SqlDdlPrepareError, lowering::SqlLoweringError, parser::SqlParseError};
#[cfg(feature = "sql")]
use crate::error::{ErrorDetail, SchemaDdlAdmissionError, StoreError};
use crate::{
    db::{
        cursor::CursorPlanError,
        numeric::NumericEvalError,
        query::plan::{PlanError, PlannerError, PolicyPlanError},
        schema::ValidateError,
    },
    error::{COMPACT_QUERY_DIAGNOSTIC_MESSAGE, ErrorClass, InternalError},
};
use icydb_diagnostic_code as diagnostic_code;
use std::fmt;

///
/// QueryError
///

#[derive(Debug)]
pub enum QueryError {
    Validate(Box<ValidateError>),

    Plan(Box<PlanError>),

    Intent(IntentError),

    Execute(QueryExecutionError),
}

impl QueryError {
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

    /// Construct one invariant for prepared SELECT lowering that lost SELECT shape.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn prepared_sql_select_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one invariant for prepared DELETE lowering that lost DELETE shape.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn prepared_sql_delete_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one invariant for prepared INSERT extraction that lost INSERT shape.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn prepared_sql_insert_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one invariant for prepared UPDATE extraction that lost UPDATE shape.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn prepared_sql_update_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one intent-domain query error.
    pub(in crate::db) const fn intent(err: IntentError) -> Self {
        Self::Intent(err)
    }

    /// Construct one query-origin unsupported execution error.
    pub(in crate::db) fn unsupported_query() -> Self {
        Self::execute(InternalError::query_unsupported())
    }

    /// Construct one query-origin unsupported SQL write boundary error.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn sql_write_boundary(
        boundary: diagnostic_code::SqlWriteBoundaryCode,
    ) -> Self {
        Self::execute(InternalError::query_sql_write_boundary(boundary))
    }

    /// Construct one query-origin SQL write-boundary error with numeric context.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn sql_write_boundary_with_facts(
        boundary: diagnostic_code::SqlWriteBoundaryCode,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        if facts.is_empty() {
            return Self::sql_write_boundary(boundary);
        }

        Self::execute(InternalError::query_sql_write_boundary_with_facts(
            boundary, facts,
        ))
    }

    /// Construct one query execution error from a checked numeric evaluation failure.
    pub(in crate::db) fn from_numeric_eval_error(err: NumericEvalError) -> Self {
        Self::execute(err.into_internal_error())
    }

    /// Construct one serialize-origin internal execution error.
    pub(in crate::db) fn serialize_internal() -> Self {
        Self::execute(InternalError::serialize_internal())
    }

    /// Construct one query error from one cursor plan-surface failure.
    pub(in crate::db) fn from_cursor_plan_error(err: CursorPlanError) -> Self {
        Self::from(PlanError::from(err))
    }

    /// Construct one query-origin unsupported SQL-feature execution error.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn unsupported_sql_feature(feature: diagnostic_code::SqlFeatureCode) -> Self {
        Self::execute(InternalError::query_unsupported_sql_feature(feature))
    }

    /// Construct one query-origin unsupported SQL lowering execution error.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn sql_lowering(reason: diagnostic_code::SqlLoweringCode) -> Self {
        Self::execute(InternalError::query_sql_lowering(reason))
    }

    /// Construct one query-origin SQL lowering error with numeric context.
    #[cfg(feature = "sql")]
    fn sql_lowering_with_facts(
        reason: diagnostic_code::SqlLoweringCode,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        if facts.is_empty() {
            return Self::sql_lowering(reason);
        }

        Self::execute(InternalError::query_sql_lowering_with_facts(reason, facts))
    }

    /// Construct one query-origin unsupported projection error.
    pub(in crate::db) fn unsupported_projection(
        reason: diagnostic_code::QueryProjectionCode,
    ) -> Self {
        Self::execute(InternalError::query_unsupported_projection(reason))
    }

    /// Construct one query-origin unsupported SQL endpoint surface mismatch.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn sql_surface_mismatch(
        mismatch: diagnostic_code::SqlSurfaceMismatchCode,
    ) -> Self {
        Self::execute(InternalError::query_sql_surface_mismatch(mismatch))
    }

    /// Construct one query error from one SQL lowering failure.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_sql_lowering_error(err: SqlLoweringError) -> Self {
        match err {
            SqlLoweringError::Query(err) => *err,
            SqlLoweringError::Parse(SqlParseError::UnsupportedFeature { feature }) => {
                Self::unsupported_sql_feature(feature)
            }
            #[cfg(feature = "sql")]
            SqlLoweringError::UnexpectedQueryLaneStatement => {
                Self::unsupported_query_lane_sql_statement()
            }
            SqlLoweringError::UnknownField { field } => {
                Self::from(PlanError::from(ExprPlanError::unknown_field(field)))
            }
            err if let Some(reason) = err.compact_diagnostic_code() => {
                let facts = err.diagnostic_facts();
                Self::sql_lowering_with_facts(reason, facts)
            }
            _ => Self::unsupported_query(),
        }
    }

    /// Construct one query error from one reduced SQL parse failure.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_sql_parse_error(err: SqlParseError) -> Self {
        Self::from_sql_lowering_error(SqlLoweringError::Parse(err))
    }

    /// Construct one query-origin unsupported SQL DDL admission error.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_sql_ddl_prepare_error(err: SqlDdlPrepareError) -> Self {
        let reason = err.admission_error();

        Self::execute(InternalError::query_schema_ddl_admission(reason))
    }

    /// Construct one query error from one SQL DDL execution failure.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_sql_ddl_execution_error(err: InternalError) -> Self {
        if matches!(
            err.detail(),
            Some(ErrorDetail::Store(StoreError::SchemaDdlPublicationRaceLost))
        ) {
            return Self::execute(InternalError::query_schema_ddl_admission(
                SchemaDdlAdmissionError::PublicationRaceLost,
            ));
        }

        if matches!(
            err.detail(),
            Some(ErrorDetail::Store(
                StoreError::SchemaDdlRewriteRequiresMigration
            ))
        ) {
            return Self::execute(InternalError::query_schema_ddl_admission(
                SchemaDdlAdmissionError::SchemaRewriteRequiresMigration,
            ));
        }

        if let Some(ErrorDetail::Store(StoreError::SchemaTransitionBudgetExceeded { resource })) =
            err.detail()
        {
            return Self::execute(InternalError::query_schema_ddl_admission(
                SchemaDdlAdmissionError::SchemaTransitionBudgetExceeded {
                    resource: *resource,
                },
            ));
        }

        Self::execute(err)
    }

    /// Construct one unsupported query-lane SQL statement error.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn unsupported_query_lane_sql_statement() -> Self {
        Self::unsupported_query()
    }

    /// Construct one unsupported aggregate target-field query error.
    pub(in crate::db) fn unknown_aggregate_target_field(_field: &str) -> Self {
        Self::execute(InternalError::query_unknown_aggregate_target_field())
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

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }
}

impl std::error::Error for QueryError {}

///
/// QueryExecutionError
///

#[derive(Debug)]
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

impl fmt::Display for QueryExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }
}

impl std::error::Error for QueryExecutionError {}

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
