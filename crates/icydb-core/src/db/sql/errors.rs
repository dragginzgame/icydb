//! Module: db::sql::errors
//! Responsibility: project SQL frontend failures into the maintained query diagnostic contract.
//! Does not own: query error taxonomy, parser admission or schema publication semantics.
//! Boundary: typed parser/lowering/DDL facts -> unchanged query errors and numeric context.

use crate::{
    db::{
        QueryError,
        query::plan::{PlanError, validate::ExprPlanError},
        sql::{ddl::SqlDdlPrepareError, lowering::SqlLoweringError, parser::SqlParseError},
    },
    error::{ErrorDetail, InternalError, SchemaDdlAdmissionError, StoreError},
};
use icydb_diagnostic_code::{self as diagnostic_code, QueryFieldRole};

impl QueryError {
    /// Construct one invariant for prepared SELECT lowering that lost SELECT shape.
    pub(in crate::db) fn prepared_sql_select_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one invariant for prepared DELETE lowering that lost DELETE shape.
    pub(in crate::db) fn prepared_sql_delete_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one invariant for prepared INSERT extraction that lost INSERT shape.
    pub(in crate::db) fn prepared_sql_insert_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one invariant for prepared UPDATE extraction that lost UPDATE shape.
    pub(in crate::db) fn prepared_sql_update_lane_mismatch() -> Self {
        Self::invariant()
    }

    /// Construct one query-origin unsupported SQL write boundary error.
    pub(in crate::db) fn sql_write_boundary(
        boundary: diagnostic_code::SqlWriteBoundaryCode,
    ) -> Self {
        Self::execute(InternalError::query_sql_write_boundary(boundary))
    }

    /// Construct one query-origin SQL write-boundary error with numeric context.
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

    /// Construct one query-origin unsupported SQL-feature execution error.
    pub(in crate::db) fn unsupported_sql_feature(feature: diagnostic_code::SqlFeatureCode) -> Self {
        Self::execute(InternalError::query_unsupported_sql_feature(feature))
    }

    /// Construct one query-origin unsupported SQL lowering execution error.
    pub(in crate::db) fn sql_lowering(reason: diagnostic_code::SqlLoweringCode) -> Self {
        Self::execute(InternalError::query_sql_lowering(reason))
    }

    /// Construct one query-origin SQL lowering error with numeric context.
    fn sql_lowering_with_facts(
        reason: diagnostic_code::SqlLoweringCode,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        if facts.is_empty() {
            return Self::sql_lowering(reason);
        }

        Self::execute(InternalError::query_sql_lowering_with_facts(reason, facts))
    }

    /// Construct one query-origin unsupported SQL endpoint surface mismatch.
    pub(in crate::db) fn sql_surface_mismatch(
        mismatch: diagnostic_code::SqlSurfaceMismatchCode,
    ) -> Self {
        Self::execute(InternalError::query_sql_surface_mismatch(mismatch))
    }

    /// Construct one query error from one SQL lowering failure.
    pub(in crate::db) fn from_sql_lowering_error(err: SqlLoweringError) -> Self {
        match err {
            SqlLoweringError::Query(err) => *err,
            SqlLoweringError::Parse(SqlParseError::InputAdmission { reason }) => reason.into(),
            SqlLoweringError::Parse(SqlParseError::UnsupportedFeature { feature }) => {
                Self::unsupported_sql_feature(feature)
            }
            SqlLoweringError::UnexpectedQueryLaneStatement => {
                Self::unsupported_query_lane_sql_statement()
            }
            SqlLoweringError::UnknownField { role, field } => {
                PlanError::from_sql_unknown_field(role, field)
                    .map_or_else(Self::invariant, Self::from)
            }
            err => {
                if let Some(reason) = err.compact_diagnostic_code() {
                    let facts = err.diagnostic_facts();

                    Self::sql_lowering_with_facts(reason, facts)
                } else {
                    Self::unsupported_query()
                }
            }
        }
    }

    /// Construct one query error from one reduced SQL parse failure.
    pub(in crate::db) fn from_sql_parse_error(err: SqlParseError) -> Self {
        Self::from_sql_lowering_error(SqlLoweringError::Parse(err))
    }

    /// Construct one query-origin unsupported SQL DDL admission error.
    pub(in crate::db) fn from_sql_ddl_prepare_error(err: SqlDdlPrepareError) -> Self {
        let reason = err.admission_error();

        Self::execute(InternalError::query_schema_ddl_admission(reason))
    }

    /// Construct one query error from one SQL DDL execution failure.
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
    pub(in crate::db) fn unsupported_query_lane_sql_statement() -> Self {
        Self::unsupported_query()
    }
}

impl PlanError {
    pub(in crate::db) fn from_sql_unknown_field(
        role: QueryFieldRole,
        field: String,
    ) -> Option<Self> {
        if !matches!(
            role,
            QueryFieldRole::Predicate
                | QueryFieldRole::Projection
                | QueryFieldRole::GroupBy
                | QueryFieldRole::AggregateTarget
        ) {
            return None;
        }

        Some(Self::from(ExprPlanError::UnknownField { field }).attach_query_field(role))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_unknown_field_attachment_accepts_only_lowering_roles() {
        for role in [
            QueryFieldRole::Predicate,
            QueryFieldRole::Projection,
            QueryFieldRole::GroupBy,
            QueryFieldRole::AggregateTarget,
        ] {
            let error = PlanError::from_sql_unknown_field(role, "missing".to_string())
                .expect("lowering role should be admitted");
            assert_eq!(error.query_field_context(), Some((role, "missing")));
        }
        for role in [QueryFieldRole::Having, QueryFieldRole::OrderBy] {
            assert!(
                PlanError::from_sql_unknown_field(role, "missing".to_string()).is_none(),
                "non-lowering role {role:?} must fail closed"
            );
        }
    }
}
