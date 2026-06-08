//! Module: diagnostic rendering.
//! Responsibility: render compact IcyDB diagnostic payloads for host/CLI users.
//! Does not own: canister wire shape, core error classification, or recovery policy.
//! Boundary: keeps rich diagnostic prose out of production canister crates.

use icydb::diagnostic::{
    DiagnosticCode, DiagnosticDetail, QueryErrorKind, RuntimeBoundaryCode, RuntimeErrorKind,
    SchemaDdlAdmissionCode, SqlFeatureCode, SqlSurfaceMismatchCode,
};

/// Render one compact public IcyDB error for CLI output.
pub(crate) fn render_error(err: &icydb::Error) -> String {
    let diagnostic = err.diagnostic();
    let code = diagnostic.code();
    let detail = diagnostic
        .detail()
        .copied()
        .map_or_else(|| code_text(code).to_string(), diagnostic_detail_text);

    format!("{}: {detail}", code_label(code))
}

fn diagnostic_detail_text(detail: DiagnosticDetail) -> String {
    match detail {
        DiagnosticDetail::QueryKind { kind } => query_kind_text(kind).to_string(),
        DiagnosticDetail::RuntimeKind { kind } => runtime_kind_text(kind).to_string(),
        DiagnosticDetail::RuntimeBoundary { boundary } => {
            runtime_boundary_text(boundary).to_string()
        }
        DiagnosticDetail::SchemaDdlAdmission { reason } => {
            format!("SQL DDL admission rejected: {}", schema_ddl_text(reason))
        }
        DiagnosticDetail::UnsupportedSqlFeature { feature } => {
            format!("unsupported SQL feature: {}", sql_feature_text(feature))
        }
        DiagnosticDetail::SqlSurfaceMismatch { mismatch } => {
            sql_surface_mismatch_text(mismatch).to_string()
        }
    }
}

const fn code_label(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "E_QUERY_VALIDATE",
        DiagnosticCode::QueryIntent => "E_QUERY_INTENT",
        DiagnosticCode::QueryPlan => "E_QUERY_PLAN",
        DiagnosticCode::QueryAccessRequirement => "E_QUERY_ACCESS_REQUIREMENT",
        DiagnosticCode::QueryUnorderedPagination => "E_QUERY_UNORDERED_PAGINATION",
        DiagnosticCode::QueryInvalidContinuationCursor => "E_QUERY_INVALID_CONTINUATION_CURSOR",
        DiagnosticCode::QueryNotFound => "E_QUERY_NOT_FOUND",
        DiagnosticCode::QueryNotUnique => "E_QUERY_NOT_UNIQUE",
        DiagnosticCode::QueryNumericOverflow => "E_QUERY_NUMERIC_OVERFLOW",
        DiagnosticCode::QueryNumericNotRepresentable => "E_QUERY_NUMERIC_NOT_REPRESENTABLE",
        DiagnosticCode::QueryUnsupportedSqlFeature => "E_QUERY_UNSUPPORTED_SQL_FEATURE",
        DiagnosticCode::QuerySqlSurfaceMismatch => "E_QUERY_SQL_SURFACE_MISMATCH",
        DiagnosticCode::SchemaDdlAdmission => "E_SCHEMA_DDL_ADMISSION",
        DiagnosticCode::StoreNotFound => "E_STORE_NOT_FOUND",
        DiagnosticCode::StoreCorruption => "E_STORE_CORRUPTION",
        DiagnosticCode::StoreInvariantViolation => "E_STORE_INVARIANT_VIOLATION",
        DiagnosticCode::RuntimeCorruption => "E_RUNTIME_CORRUPTION",
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => {
            "E_RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT"
        }
        DiagnosticCode::RuntimeInvariantViolation => "E_RUNTIME_INVARIANT_VIOLATION",
        DiagnosticCode::RuntimeConflict => "E_RUNTIME_CONFLICT",
        DiagnosticCode::RuntimeNotFound => "E_RUNTIME_NOT_FOUND",
        DiagnosticCode::RuntimeUnsupported => "E_RUNTIME_UNSUPPORTED",
        DiagnosticCode::RuntimeInternal => "E_RUNTIME_INTERNAL",
    }
}

const fn code_text(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "query validation failed",
        DiagnosticCode::QueryIntent => "query intent is invalid",
        DiagnosticCode::QueryPlan => "query planning failed",
        DiagnosticCode::QueryAccessRequirement => "query access requirement was not met",
        DiagnosticCode::QueryUnorderedPagination => "pagination requires deterministic ordering",
        DiagnosticCode::QueryInvalidContinuationCursor => "continuation cursor is invalid",
        DiagnosticCode::QueryNotFound => "query expected one row but found none",
        DiagnosticCode::QueryNotUnique => "query expected one row but found multiple rows",
        DiagnosticCode::QueryNumericOverflow => "numeric operation overflowed",
        DiagnosticCode::QueryNumericNotRepresentable => "numeric result is not representable",
        DiagnosticCode::QueryUnsupportedSqlFeature => "SQL feature is not supported",
        DiagnosticCode::QuerySqlSurfaceMismatch => "SQL statement used the wrong endpoint surface",
        DiagnosticCode::SchemaDdlAdmission => "SQL DDL admission rejected",
        DiagnosticCode::StoreNotFound => "store key was not found",
        DiagnosticCode::StoreCorruption => "store corruption detected",
        DiagnosticCode::StoreInvariantViolation => "store invariant was violated",
        DiagnosticCode::RuntimeCorruption => "runtime corruption detected",
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => {
            "persisted data format is incompatible"
        }
        DiagnosticCode::RuntimeInvariantViolation => "runtime invariant was violated",
        DiagnosticCode::RuntimeConflict => "runtime conflict detected",
        DiagnosticCode::RuntimeNotFound => "runtime item was not found",
        DiagnosticCode::RuntimeUnsupported => "operation is not supported",
        DiagnosticCode::RuntimeInternal => "internal runtime failure",
    }
}

const fn query_kind_text(kind: QueryErrorKind) -> &'static str {
    match kind {
        QueryErrorKind::Validate => "query validation failed",
        QueryErrorKind::Intent => "query intent is invalid",
        QueryErrorKind::Plan => "query planning failed",
        QueryErrorKind::AccessRequirement => "query access requirement was not met",
        QueryErrorKind::UnorderedPagination => "pagination requires deterministic ordering",
        QueryErrorKind::InvalidContinuationCursor => "continuation cursor is invalid",
        QueryErrorKind::NotFound => "query expected one row but found none",
        QueryErrorKind::NotUnique => "query expected one row but found multiple rows",
    }
}

const fn runtime_kind_text(kind: RuntimeErrorKind) -> &'static str {
    match kind {
        RuntimeErrorKind::Corruption => "runtime corruption detected",
        RuntimeErrorKind::IncompatiblePersistedFormat => "persisted data format is incompatible",
        RuntimeErrorKind::InvariantViolation => "runtime invariant was violated",
        RuntimeErrorKind::Conflict => "runtime conflict detected",
        RuntimeErrorKind::NotFound => "runtime item was not found",
        RuntimeErrorKind::Unsupported => "operation is not supported",
        RuntimeErrorKind::Internal => "internal runtime failure",
    }
}

const fn runtime_boundary_text(boundary: RuntimeBoundaryCode) -> &'static str {
    match boundary {
        RuntimeBoundaryCode::SqlSurfaceControllerRequired => {
            "SQL endpoint requires controller access"
        }
        RuntimeBoundaryCode::SchemaSurfaceControllerRequired => {
            "schema endpoint requires controller access"
        }
        RuntimeBoundaryCode::SqlQueryNoConfiguredEntities => {
            "SQL query endpoint has no configured entities"
        }
        RuntimeBoundaryCode::SqlQueryEntityNotConfigured => {
            "SQL query target entity is not configured for this canister"
        }
        RuntimeBoundaryCode::SqlDdlTargetRequired => "SQL DDL requires one target entity",
        RuntimeBoundaryCode::SqlDdlEntityNotConfigured => {
            "SQL DDL target entity is not configured for this canister"
        }
        RuntimeBoundaryCode::QueryResponseRowsRequired => "query response contains grouped rows",
        RuntimeBoundaryCode::QueryResponseGroupedRowsRequired => {
            "query response contains scalar rows"
        }
        RuntimeBoundaryCode::MutationResultEntityRequired => {
            "mutation result contains a count, not one entity"
        }
        RuntimeBoundaryCode::MutationResultEntitiesRequired => {
            "mutation result contains a count, not entity rows"
        }
        RuntimeBoundaryCode::MutationResultIdRequired => {
            "mutation result contains a count, not one entity id"
        }
        RuntimeBoundaryCode::MutationResultIdsRequired => {
            "mutation result contains a count, not entity ids"
        }
        RuntimeBoundaryCode::RowProjectionFieldNotConfigured => {
            "requested projection field is not configured for this entity"
        }
    }
}

const fn schema_ddl_text(reason: SchemaDdlAdmissionCode) -> &'static str {
    match reason {
        SchemaDdlAdmissionCode::MissingExpectedSchemaVersion => "missing EXPECT SCHEMA VERSION",
        SchemaDdlAdmissionCode::MissingNextSchemaVersion => "missing SET SCHEMA VERSION",
        SchemaDdlAdmissionCode::StaleExpectedSchemaVersion => "expected schema version is stale",
        SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion => {
            "expected schema version is invalid"
        }
        SchemaDdlAdmissionCode::InvalidNextSchemaVersion => "next schema version is invalid",
        SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump => {
            "accepted schema changed without a version bump"
        }
        SchemaDdlAdmissionCode::EmptyVersionBump => "schema version bump has no schema change",
        SchemaDdlAdmissionCode::VersionGap => "schema version gap is not allowed",
        SchemaDdlAdmissionCode::VersionRollback => "schema version rollback is not allowed",
        SchemaDdlAdmissionCode::FingerprintMethodMismatch => {
            "schema fingerprint method versions do not match"
        }
        SchemaDdlAdmissionCode::UnsupportedTransitionClass => {
            "DDL transition class is not supported"
        }
        SchemaDdlAdmissionCode::PhysicalRunnerMissing => {
            "required physical runner capability is missing"
        }
        SchemaDdlAdmissionCode::ValidationFailed => "candidate schema validation failed",
        SchemaDdlAdmissionCode::PublicationRaceLost => "accepted schema changed after DDL binding",
    }
}

const fn sql_surface_mismatch_text(mismatch: SqlSurfaceMismatchCode) -> &'static str {
    match mismatch {
        SqlSurfaceMismatchCode::QueryRejectsInsert => {
            "execute_sql_query rejects INSERT; use execute_sql_update::<E>()"
        }
        SqlSurfaceMismatchCode::QueryRejectsUpdate => {
            "execute_sql_query rejects UPDATE; use execute_sql_update::<E>()"
        }
        SqlSurfaceMismatchCode::QueryRejectsDelete => {
            "execute_sql_query rejects DELETE; use execute_sql_update::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsSelect => {
            "execute_sql_update rejects SELECT; use execute_sql_query::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsExplain => {
            "execute_sql_update rejects EXPLAIN; use execute_sql_query::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsDescribe => {
            "execute_sql_update rejects DESCRIBE; use execute_sql_query::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsShowIndexes => {
            "execute_sql_update rejects SHOW INDEXES; use execute_sql_query::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsShowColumns => {
            "execute_sql_update rejects SHOW COLUMNS; use execute_sql_query::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsShowEntities => {
            "execute_sql_update rejects SHOW ENTITIES; use execute_sql_query::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsShowStores => {
            "execute_sql_update rejects SHOW STORES; use execute_sql_query::<E>()"
        }
        SqlSurfaceMismatchCode::UpdateRejectsShowMemory => {
            "execute_sql_update rejects SHOW MEMORY; use execute_sql_query::<E>()"
        }
    }
}

const fn sql_feature_text(feature: SqlFeatureCode) -> &'static str {
    match feature {
        SqlFeatureCode::AggregateFilterClause => "aggregate FILTER clauses",
        SqlFeatureCode::AlterStatementBeyondAlterTable
        | SqlFeatureCode::AlterTableAddColumnDuplicateDefault
        | SqlFeatureCode::AlterTableAddColumnModifiers
        | SqlFeatureCode::AlterTableAddStatementBeyondAddColumn
        | SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction
        | SqlFeatureCode::AlterTableAlterColumnModifiers
        | SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction
        | SqlFeatureCode::AlterTableAlterColumnUnsupportedAction
        | SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn
        | SqlFeatureCode::AlterTableDropColumnIfExistsSyntax
        | SqlFeatureCode::AlterTableDropColumnModifiers
        | SqlFeatureCode::AlterTableDropStatementBeyondDropColumn
        | SqlFeatureCode::AlterTableRenameColumnMissingTo
        | SqlFeatureCode::AlterTableRenameColumnModifiers
        | SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn
        | SqlFeatureCode::AlterTableUnsupportedOperation
        | SqlFeatureCode::CreateIndexIfNotExistsSyntax
        | SqlFeatureCode::CreateIndexKeyOrderingModifiers
        | SqlFeatureCode::CreateIndexModifiers
        | SqlFeatureCode::CreateStatementBeyondCreateIndex
        | SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause
        | SqlFeatureCode::DdlSchemaVersionDuplicateSetClause
        | SqlFeatureCode::DropIndexModifiers
        | SqlFeatureCode::DropIndexIfExistsSyntax
        | SqlFeatureCode::DropStatementBeyondDropIndex
        | SqlFeatureCode::ExpressionIndexUnsupportedFunction => sql_ddl_feature_text(feature),
        SqlFeatureCode::ColumnAlias => "column or expression aliases",
        SqlFeatureCode::DescribeModifier => "DESCRIBE modifiers",
        SqlFeatureCode::Having => "HAVING",
        SqlFeatureCode::Insert => "INSERT",
        SqlFeatureCode::Join => "JOIN",
        SqlFeatureCode::LikePatternBeyondTrailingPrefix => {
            "LIKE patterns beyond trailing '%' prefix form"
        }
        SqlFeatureCode::LowerFieldPredicateUnsupported => {
            "LOWER(field) predicate forms beyond LIKE 'prefix%' or ordered text bounds"
        }
        SqlFeatureCode::MultiStatementSql => "multi-statement SQL input",
        SqlFeatureCode::NestedAggregateInput => {
            "nested aggregate references inside aggregate input expressions"
        }
        SqlFeatureCode::NestedProjectionFunctionInArithmetic => {
            "nested projection functions inside arithmetic expressions"
        }
        SqlFeatureCode::OrderByUnsupportedForm => "unsupported ORDER BY expression form",
        SqlFeatureCode::Other => "unsupported SQL feature",
        SqlFeatureCode::ParameterBinding => "parameter binding",
        SqlFeatureCode::ParameterizedSchemaVersion => "parameterized schema versions",
        SqlFeatureCode::PredicateStartsWithFirstArgument => {
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
        }
        SqlFeatureCode::QuotedIdentifiers => "quoted identifiers",
        SqlFeatureCode::ReturningUnsupportedShape => "unsupported RETURNING shape",
        SqlFeatureCode::ScalarFunctionExpressionPosition => {
            "functions beyond supported scalar forms in this expression position"
        }
        SqlFeatureCode::ScaleTakingNumericFunctionExpressionPosition => {
            "scale-taking numeric functions in this expression position"
        }
        SqlFeatureCode::SearchedCaseGroupedOrderBy => {
            "searched CASE in grouped ORDER BY expressions"
        }
        SqlFeatureCode::ShowColumnsModifiers => "SHOW COLUMNS modifiers",
        SqlFeatureCode::ShowEntitiesModifiers => "SHOW ENTITIES modifiers",
        SqlFeatureCode::ShowIndexesModifiers => "SHOW INDEXES modifiers",
        SqlFeatureCode::ShowMemoryModifiers => "SHOW MEMORY modifiers",
        SqlFeatureCode::ShowStoresModifiers => "SHOW STORES modifiers",
        SqlFeatureCode::ShowUnsupportedCommand => "unsupported SHOW command",
        SqlFeatureCode::SimpleCaseExpression => "simple CASE expressions",
        SqlFeatureCode::StandaloneLiteralProjectionItem => "standalone literal projection items",
        SqlFeatureCode::SupportedGroupedOrderByExpressionFamily => {
            "unsupported grouped ORDER BY expression family"
        }
        SqlFeatureCode::SupportedOrderByExpressionFamily => {
            "unsupported ORDER BY expression family"
        }
        SqlFeatureCode::UnionIntersectExcept => "UNION, INTERSECT, or EXCEPT",
        SqlFeatureCode::UnsupportedFunctionNamespace => "unsupported SQL function namespace",
        SqlFeatureCode::Update => "UPDATE",
        SqlFeatureCode::UpperFieldPredicateUnsupported => {
            "UPPER(field) predicate forms beyond LIKE 'prefix%' or ordered text bounds"
        }
        SqlFeatureCode::WindowFunction => "window functions",
        SqlFeatureCode::With => "WITH",
    }
}

const fn sql_ddl_feature_text(feature: SqlFeatureCode) -> &'static str {
    match feature {
        SqlFeatureCode::AlterStatementBeyondAlterTable => "ALTER statements beyond ALTER TABLE",
        SqlFeatureCode::AlterTableAddColumnDuplicateDefault => {
            "duplicate ALTER TABLE ADD COLUMN DEFAULT clauses"
        }
        SqlFeatureCode::AlterTableAddColumnModifiers => "ALTER TABLE ADD COLUMN modifiers",
        SqlFeatureCode::AlterTableAddStatementBeyondAddColumn => {
            "ALTER TABLE ADD statements beyond ADD COLUMN"
        }
        SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction => {
            "ALTER TABLE ALTER COLUMN DROP actions beyond DEFAULT and NOT NULL"
        }
        SqlFeatureCode::AlterTableAlterColumnModifiers => "ALTER TABLE ALTER COLUMN modifiers",
        SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction => {
            "ALTER TABLE ALTER COLUMN SET actions beyond DEFAULT and NOT NULL"
        }
        SqlFeatureCode::AlterTableAlterColumnUnsupportedAction => {
            "ALTER TABLE ALTER COLUMN actions beyond SET/DROP DEFAULT and SET/DROP NOT NULL"
        }
        SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn => {
            "ALTER TABLE ALTER statements beyond ALTER COLUMN"
        }
        SqlFeatureCode::AlterTableDropColumnIfExistsSyntax => {
            "ALTER TABLE DROP COLUMN IF EXISTS syntax"
        }
        SqlFeatureCode::AlterTableDropColumnModifiers => "ALTER TABLE DROP COLUMN modifiers",
        SqlFeatureCode::AlterTableDropStatementBeyondDropColumn => {
            "ALTER TABLE DROP statements beyond DROP COLUMN"
        }
        SqlFeatureCode::AlterTableRenameColumnMissingTo => "ALTER TABLE RENAME COLUMN without TO",
        SqlFeatureCode::AlterTableRenameColumnModifiers => "ALTER TABLE RENAME COLUMN modifiers",
        SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn => {
            "ALTER TABLE RENAME statements beyond RENAME COLUMN"
        }
        SqlFeatureCode::AlterTableUnsupportedOperation => "unsupported ALTER TABLE operation",
        SqlFeatureCode::CreateIndexIfNotExistsSyntax => "CREATE INDEX IF NOT EXISTS syntax",
        SqlFeatureCode::CreateIndexKeyOrderingModifiers => "CREATE INDEX key ordering modifiers",
        SqlFeatureCode::CreateIndexModifiers => "CREATE INDEX modifiers",
        SqlFeatureCode::CreateStatementBeyondCreateIndex => "CREATE statements beyond CREATE INDEX",
        SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause => {
            "duplicate EXPECT SCHEMA VERSION clauses"
        }
        SqlFeatureCode::DdlSchemaVersionDuplicateSetClause => {
            "duplicate SET SCHEMA VERSION clauses"
        }
        SqlFeatureCode::DropIndexModifiers => "DROP INDEX modifiers",
        SqlFeatureCode::DropIndexIfExistsSyntax => "DROP INDEX IF EXISTS syntax",
        SqlFeatureCode::DropStatementBeyondDropIndex => "DROP statements beyond DROP INDEX",
        SqlFeatureCode::ExpressionIndexUnsupportedFunction => {
            "expression index functions beyond LOWER, UPPER, and TRIM"
        }
        _ => "unsupported SQL feature",
    }
}

#[cfg(test)]
mod tests {
    use super::render_error;

    #[test]
    fn renders_schema_ddl_admission_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::SchemaDdlAdmission,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::SchemaDdlAdmission {
                reason: icydb::diagnostic::SchemaDdlAdmissionCode::PublicationRaceLost,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_SCHEMA_DDL_ADMISSION: SQL DDL admission rejected: accepted schema changed after DDL binding",
        );
    }

    #[test]
    fn renders_unsupported_sql_feature_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryUnsupportedSqlFeature,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::UnsupportedSqlFeature {
                feature: icydb::diagnostic::SqlFeatureCode::Join,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_UNSUPPORTED_SQL_FEATURE: unsupported SQL feature: JOIN",
        );
    }

    #[test]
    fn renders_sql_surface_mismatch_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QuerySqlSurfaceMismatch,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: icydb::diagnostic::SqlSurfaceMismatchCode::QueryRejectsInsert,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_SQL_SURFACE_MISMATCH: execute_sql_query rejects INSERT; use execute_sql_update::<E>()",
        );
    }

    #[test]
    fn renders_runtime_boundary_detail() {
        let err = icydb::Error::from_runtime_boundary(
            icydb::diagnostic::RuntimeBoundaryCode::SqlDdlTargetRequired,
            icydb::ErrorOrigin::Interface,
        );

        assert_eq!(
            render_error(&err),
            "E_RUNTIME_UNSUPPORTED: SQL DDL requires one target entity",
        );
    }

    #[test]
    fn falls_back_to_code_text_without_detail() {
        let err = icydb::Error::from_code(
            icydb::diagnostic::DiagnosticCode::RuntimeInternal,
            icydb::ErrorOrigin::Runtime,
        );

        assert_eq!(
            render_error(&err),
            "E_RUNTIME_INTERNAL: internal runtime failure"
        );
    }
}
