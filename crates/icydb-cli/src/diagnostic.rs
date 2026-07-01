//! Module: diagnostic rendering.
//! Responsibility: render compact IcyDB diagnostic payloads for host/CLI users.
//! Does not own: canister wire shape, core error classification, or recovery policy.
//! Boundary: keeps rich diagnostic prose out of production canister crates.

use icydb::diagnostic::{
    DiagnosticCode, DiagnosticDetail, QueryErrorKind, QueryProjectionCode, QueryReadAdmissionCode,
    QueryResultShapeCode, RuntimeBoundaryCode, RuntimeErrorKind, SchemaDdlAdmissionCode,
    SqlFeatureCode, SqlLoweringCode, SqlSurfaceMismatchCode, SqlWriteBoundaryCode,
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
        DiagnosticDetail::SqlWriteBoundary { boundary } => {
            format!("SQL write rejected: {}", sql_write_boundary_text(boundary))
        }
        DiagnosticDetail::QueryProjection { reason } => {
            format!(
                "query projection rejected: {}",
                query_projection_text(reason)
            )
        }
        DiagnosticDetail::QueryReadAdmission { reason } => {
            format!(
                "query read admission rejected: {}",
                query_read_admission_text(reason)
            )
        }
        DiagnosticDetail::QueryResultShape { reason } => {
            query_result_shape_text(reason).to_string()
        }
        DiagnosticDetail::SqlLowering { reason } => {
            format!("unsupported SQL lowering: {}", sql_lowering_text(reason))
        }
    }
}

const fn code_label(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "E_QUERY_VALIDATE",
        DiagnosticCode::QueryIntent => "E_QUERY_INTENT",
        DiagnosticCode::QueryPlan => "E_QUERY_PLAN",
        DiagnosticCode::QueryReadAdmission => "E_QUERY_READ_ADMISSION",
        DiagnosticCode::QueryAccessRequirement => "E_QUERY_ACCESS_REQUIREMENT",
        DiagnosticCode::QueryUnorderedPagination => "E_QUERY_UNORDERED_PAGINATION",
        DiagnosticCode::QueryInvalidContinuationCursor => "E_QUERY_INVALID_CONTINUATION_CURSOR",
        DiagnosticCode::QueryNotFound => "E_QUERY_NOT_FOUND",
        DiagnosticCode::QueryNotUnique => "E_QUERY_NOT_UNIQUE",
        DiagnosticCode::QueryNumericOverflow => "E_QUERY_NUMERIC_OVERFLOW",
        DiagnosticCode::QueryNumericNotRepresentable => "E_QUERY_NUMERIC_NOT_REPRESENTABLE",
        DiagnosticCode::QueryUnknownAggregateTargetField => {
            "E_QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD"
        }
        DiagnosticCode::QueryUnsupportedProjection => "E_QUERY_UNSUPPORTED_PROJECTION",
        DiagnosticCode::QueryResultShapeMismatch => "E_QUERY_RESULT_SHAPE_MISMATCH",
        DiagnosticCode::QueryUnsupportedSqlFeature => "E_QUERY_UNSUPPORTED_SQL_FEATURE",
        DiagnosticCode::QuerySqlSurfaceMismatch => "E_QUERY_SQL_SURFACE_MISMATCH",
        DiagnosticCode::QuerySqlWriteBoundary => "E_QUERY_SQL_WRITE_BOUNDARY",
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
        DiagnosticCode::QueryReadAdmission => "query read admission rejected",
        DiagnosticCode::QueryAccessRequirement => "query access requirement was not met",
        DiagnosticCode::QueryUnorderedPagination => "pagination requires deterministic ordering",
        DiagnosticCode::QueryInvalidContinuationCursor => "continuation cursor is invalid",
        DiagnosticCode::QueryNotFound => "query expected one row but found none",
        DiagnosticCode::QueryNotUnique => "query expected one row but found multiple rows",
        DiagnosticCode::QueryNumericOverflow => "numeric operation overflowed",
        DiagnosticCode::QueryNumericNotRepresentable => "numeric result is not representable",
        DiagnosticCode::QueryUnknownAggregateTargetField => "unknown aggregate target field",
        DiagnosticCode::QueryUnsupportedProjection => "query projection is not supported",
        DiagnosticCode::QueryResultShapeMismatch => "query result shape mismatch",
        DiagnosticCode::QueryUnsupportedSqlFeature => "SQL feature is not supported",
        DiagnosticCode::QuerySqlSurfaceMismatch => "SQL statement used the wrong endpoint surface",
        DiagnosticCode::QuerySqlWriteBoundary => "SQL write boundary rejected",
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

const fn query_projection_text(reason: QueryProjectionCode) -> &'static str {
    match reason {
        QueryProjectionCode::NumericLiteralRequired => {
            "scalar numeric projection requires a numeric literal"
        }
        QueryProjectionCode::NumericScaleArguments => {
            "scale-taking numeric projections require a non-negative integer scale"
        }
        QueryProjectionCode::NestedFieldPathPreview => {
            "nested field-path projection preview is not supported"
        }
        QueryProjectionCode::CaseConditionBooleanRequired => {
            "CASE projection conditions must evaluate to boolean values"
        }
        QueryProjectionCode::NumericInputRequired => {
            "numeric projection functions require numeric inputs"
        }
        QueryProjectionCode::TextOrBlobInputRequired => {
            "this projection function requires text or blob input"
        }
        QueryProjectionCode::TextInputRequired => "text projection functions require text input",
        QueryProjectionCode::TextOrNullArgumentRequired => {
            "this projection function requires a text or NULL literal argument"
        }
        QueryProjectionCode::IntegerOrNullArgumentRequired => {
            "this projection function requires an integer or NULL literal argument"
        }
        QueryProjectionCode::UnaryOperandIncompatible => {
            "projection unary operator operand is incompatible"
        }
        QueryProjectionCode::BinaryOperandsIncompatible => {
            "projection binary operator operands are incompatible"
        }
    }
}

fn query_read_admission_text(reason: QueryReadAdmissionCode) -> String {
    format!(
        "{}; fix: {}",
        query_read_admission_reason_text(reason),
        query_read_admission_fix_text(reason),
    )
}

const fn query_read_admission_reason_text(reason: QueryReadAdmissionCode) -> &'static str {
    match reason {
        QueryReadAdmissionCode::PublicQueryRequiresLimit => {
            "public read queries require an explicit LIMIT"
        }
        QueryReadAdmissionCode::PublicQueryRequiresIndex => {
            "public read queries require an index-backed access path"
        }
        QueryReadAdmissionCode::UnboundedFullScanRejected => {
            "public read queries cannot execute an unbounded full scan"
        }
        QueryReadAdmissionCode::ScanBoundUnavailable => {
            "the planner could not prove a scan bound for this read"
        }
        QueryReadAdmissionCode::ScanBoundExceedsPolicy => {
            "the proven scan bound exceeds this endpoint's read budget"
        }
        QueryReadAdmissionCode::EstimatedOnlyBoundRejected => {
            "estimated-only scan bounds are not sufficient for this read lane"
        }
        QueryReadAdmissionCode::SortRequiresMaterialization => {
            "this read requires materializing rows for ORDER BY"
        }
        QueryReadAdmissionCode::MaterializationExceedsBudget => {
            "materialized rows exceed this endpoint's read budget"
        }
        QueryReadAdmissionCode::ProjectionResponseMayExceedLimit => {
            "projected response size may exceed this endpoint's byte budget"
        }
        QueryReadAdmissionCode::GroupedQueryRequiresLimits => {
            "grouped reads require explicit group and memory budgets"
        }
        QueryReadAdmissionCode::GroupedQueryExceedsBudget => {
            "grouped read planning exceeds this endpoint's group budget"
        }
        QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute => {
            "diagnostic EXPLAIN lanes cannot execute rows"
        }
        QueryReadAdmissionCode::IntrospectionDisabledForLane => {
            "introspection is disabled for this read lane"
        }
        QueryReadAdmissionCode::UnsupportedStatementForQueryLane => {
            "this SQL statement is not supported by the selected read lane"
        }
        QueryReadAdmissionCode::PublicQueryOffsetRejected => {
            "public read queries cannot use a non-zero OFFSET"
        }
        QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy => {
            "the returned-row bound exceeds this endpoint's read budget"
        }
    }
}

const fn query_read_admission_fix_text(reason: QueryReadAdmissionCode) -> &'static str {
    match reason {
        QueryReadAdmissionCode::PublicQueryRequiresLimit => {
            "add a finite LIMIT or use an aggregate/grouped shape with explicit budgets"
        }
        QueryReadAdmissionCode::PublicQueryRequiresIndex
        | QueryReadAdmissionCode::UnboundedFullScanRejected
        | QueryReadAdmissionCode::ScanBoundUnavailable
        | QueryReadAdmissionCode::EstimatedOnlyBoundRejected => {
            "add a suitable index, tighten the predicate, or move the query behind a trusted admin endpoint"
        }
        QueryReadAdmissionCode::ScanBoundExceedsPolicy => {
            "tighten the predicate or lower the query bound so the proven scan fits the endpoint budget"
        }
        QueryReadAdmissionCode::SortRequiresMaterialization => {
            "order by the selected index order, remove the sort, or keep the query on a trusted admin path"
        }
        QueryReadAdmissionCode::MaterializationExceedsBudget => {
            "reduce the materialized row bound or use an index-backed order that avoids materialization"
        }
        QueryReadAdmissionCode::ProjectionResponseMayExceedLimit => {
            "return fewer rows or narrower projections before exposing the read publicly"
        }
        QueryReadAdmissionCode::GroupedQueryRequiresLimits => {
            "add grouped_limits(max_groups, max_group_bytes) and keep DISTINCT aggregates within policy"
        }
        QueryReadAdmissionCode::GroupedQueryExceedsBudget => {
            "lower grouped_limits or split the report into a trusted/admin query"
        }
        QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute => {
            "run EXPLAIN for diagnostics only, then execute through an admitted ordinary or trusted lane"
        }
        QueryReadAdmissionCode::IntrospectionDisabledForLane => {
            "use a controller-gated diagnostic/admin endpoint for introspection"
        }
        QueryReadAdmissionCode::UnsupportedStatementForQueryLane => {
            "use an ordinary typed/fluent read shape or a controller-gated trusted SQL endpoint"
        }
        QueryReadAdmissionCode::PublicQueryOffsetRejected => {
            "use cursor pagination instead of OFFSET"
        }
        QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy => {
            "lower LIMIT or split the query into smaller cursor-paged reads"
        }
    }
}

const fn query_result_shape_text(reason: QueryResultShapeCode) -> &'static str {
    match reason {
        QueryResultShapeCode::ExpectedRows => {
            "grouped query result cannot be consumed as entity rows"
        }
        QueryResultShapeCode::ExpectedGroupedRows => {
            "scalar query result cannot be consumed as grouped rows"
        }
    }
}

const fn sql_lowering_text(reason: SqlLoweringCode) -> &'static str {
    match reason {
        SqlLoweringCode::EntityMismatch => {
            "statement target entity does not match the requested entity"
        }
        SqlLoweringCode::SelectProjectionShape => "unsupported SELECT projection shape",
        SqlLoweringCode::SelectDistinct => "unsupported SELECT DISTINCT shape",
        SqlLoweringCode::DistinctOrderByProjection => {
            "SELECT DISTINCT ORDER BY terms must be derivable from the projected tuple"
        }
        SqlLoweringCode::GlobalAggregateProjection => {
            "unsupported global aggregate projection shape"
        }
        SqlLoweringCode::GlobalAggregateGroupBy => "global aggregate SQL does not support GROUP BY",
        SqlLoweringCode::SelectGroupByShape => "unsupported SELECT GROUP BY shape",
        SqlLoweringCode::GroupedProjectionExplicitListRequired => {
            "grouped SELECT requires an explicit projection list"
        }
        SqlLoweringCode::GroupedProjectionAggregateRequired => {
            "grouped SELECT projection must include at least one aggregate expression"
        }
        SqlLoweringCode::GroupedProjectionNonGroupField => {
            "grouped projection references fields outside GROUP BY keys"
        }
        SqlLoweringCode::GroupedProjectionScalarAfterAggregate => {
            "grouped projection scalar expression appears after aggregate expressions"
        }
        SqlLoweringCode::HavingRequiresGroupBy => "HAVING requires GROUP BY",
        SqlLoweringCode::SelectHavingShape => "unsupported SQL HAVING shape",
        SqlLoweringCode::AggregateInputExpressions => {
            "aggregate input expressions are not executable in this release"
        }
        SqlLoweringCode::WhereExpressionShape => "unsupported SQL WHERE expression shape",
        SqlLoweringCode::ParameterPlacement => "unsupported SQL parameter placement",
        SqlLoweringCode::SqlDdlExecutionUnsupported => {
            "SQL DDL execution is not supported in this release"
        }
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
        RuntimeBoundaryCode::SqlIntrospectionDisabled => {
            "SQL introspection is disabled for this canister build target"
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
        SchemaDdlAdmissionCode::InvalidAddColumnDefault => {
            "ADD COLUMN default value is not encodable"
        }
        SchemaDdlAdmissionCode::InvalidAlterColumnDefault => {
            "ALTER COLUMN SET DEFAULT value is not encodable"
        }
        SchemaDdlAdmissionCode::GeneratedIndexDropRejected => {
            "generated index cannot be dropped by SQL DDL"
        }
        SchemaDdlAdmissionCode::RequiredDropDefaultUnsupported => {
            "DROP DEFAULT is not supported for required fields"
        }
        SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected => {
            "generated field default cannot be changed by SQL DDL"
        }
        SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected => {
            "generated field nullability cannot be changed by SQL DDL"
        }
        SchemaDdlAdmissionCode::SetNotNullValidationFailed => {
            "SET NOT NULL validation found existing NULL values"
        }
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

const fn sql_write_boundary_text(boundary: SqlWriteBoundaryCode) -> &'static str {
    match boundary {
        SqlWriteBoundaryCode::PrimaryKeyLiteralShape => "primary key literal has the wrong shape",
        SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible => {
            "primary key literal is not compatible with the entity key type"
        }
        SqlWriteBoundaryCode::MissingPrimaryKey => "INSERT is missing required primary key fields",
        SqlWriteBoundaryCode::MissingRequiredFields => {
            "INSERT is missing required non-generated fields"
        }
        SqlWriteBoundaryCode::ExplicitManagedField => {
            "explicit writes to managed fields are not allowed"
        }
        SqlWriteBoundaryCode::ExplicitGeneratedField => {
            "explicit writes to generated fields are not allowed"
        }
        SqlWriteBoundaryCode::InsertSelectRequiresScalar => {
            "INSERT SELECT requires a scalar SELECT source"
        }
        SqlWriteBoundaryCode::InsertSelectAggregateProjection => {
            "INSERT SELECT does not support aggregate source projections"
        }
        SqlWriteBoundaryCode::InsertSelectWidthMismatch => {
            "INSERT SELECT projection width must match the target column list"
        }
        SqlWriteBoundaryCode::UpdatePrimaryKeyMutation => "UPDATE cannot mutate primary key fields",
        SqlWriteBoundaryCode::InvalidFieldLiteral => {
            "SQL write literal is not compatible with the target field type"
        }
        SqlWriteBoundaryCode::UnknownReturningField => {
            "RETURNING references a field that does not exist on the target entity"
        }
        SqlWriteBoundaryCode::DuplicateReturningField => {
            "RETURNING field lists cannot repeat the same target field"
        }
        SqlWriteBoundaryCode::UpdateMissingWherePredicate => "UPDATE requires a WHERE predicate",
        SqlWriteBoundaryCode::WriteOrderByUnsupportedShape => {
            "SQL write ORDER BY only supports direct field targets"
        }
        SqlWriteBoundaryCode::ReturningResponseTooLarge => {
            "UPDATE RETURNING response exceeds this endpoint's response-size budget"
        }
        SqlWriteBoundaryCode::ReturningRowsTooMany => {
            "UPDATE RETURNING emits more rows than this endpoint's row budget"
        }
        SqlWriteBoundaryCode::StagedRowsTooMany => {
            "SQL write stages more rows than this endpoint's row budget"
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
        SqlFeatureCode::NumericScaleFunctionArguments => {
            "scale-taking numeric function arguments beyond supported literal integer scale"
        }
        SqlFeatureCode::OrderByFieldNotOrderable => {
            "ORDER BY fields whose accepted catalog type is not orderable"
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
    fn renders_sql_write_boundary_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QuerySqlWriteBoundary,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::SqlWriteBoundary {
                boundary: icydb::diagnostic::SqlWriteBoundaryCode::MissingPrimaryKey,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_SQL_WRITE_BOUNDARY: SQL write rejected: INSERT is missing required primary key fields",
        );
    }

    #[test]
    fn renders_sql_write_staged_row_boundary_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QuerySqlWriteBoundary,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::SqlWriteBoundary {
                boundary: icydb::diagnostic::SqlWriteBoundaryCode::StagedRowsTooMany,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_SQL_WRITE_BOUNDARY: SQL write rejected: SQL write stages more rows than this endpoint's row budget",
        );
    }

    #[test]
    fn renders_query_projection_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryUnsupportedProjection,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::QueryProjection {
                reason: icydb::diagnostic::QueryProjectionCode::NumericScaleArguments,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_UNSUPPORTED_PROJECTION: query projection rejected: scale-taking numeric projections require a non-negative integer scale",
        );
    }

    #[test]
    fn renders_query_read_admission_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission {
                reason: icydb::diagnostic::QueryReadAdmissionCode::PublicQueryRequiresLimit,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_READ_ADMISSION: query read admission rejected: public read queries require an explicit LIMIT; fix: add a finite LIMIT or use an aggregate/grouped shape with explicit budgets",
        );
    }

    #[test]
    fn renders_query_read_admission_fix_hints_for_common_public_read_rejections() {
        let cases = [
            (
                icydb::diagnostic::QueryReadAdmissionCode::UnboundedFullScanRejected,
                "E_QUERY_READ_ADMISSION: query read admission rejected: public read queries cannot execute an unbounded full scan; fix: add a suitable index, tighten the predicate, or move the query behind a trusted admin endpoint",
            ),
            (
                icydb::diagnostic::QueryReadAdmissionCode::PublicQueryOffsetRejected,
                "E_QUERY_READ_ADMISSION: query read admission rejected: public read queries cannot use a non-zero OFFSET; fix: use cursor pagination instead of OFFSET",
            ),
            (
                icydb::diagnostic::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
                "E_QUERY_READ_ADMISSION: query read admission rejected: grouped reads require explicit group and memory budgets; fix: add grouped_limits(max_groups, max_group_bytes) and keep DISTINCT aggregates within policy",
            ),
            (
                icydb::diagnostic::QueryReadAdmissionCode::SortRequiresMaterialization,
                "E_QUERY_READ_ADMISSION: query read admission rejected: this read requires materializing rows for ORDER BY; fix: order by the selected index order, remove the sort, or keep the query on a trusted admin path",
            ),
        ];

        for (reason, expected) in cases {
            let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
                icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
                icydb::diagnostic::ErrorOrigin::Query,
                Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission { reason }),
            ));

            assert_eq!(render_error(&err), expected);
        }
    }

    #[test]
    fn renders_query_read_admission_fix_hint_for_every_rejection_code() {
        let reasons = [
            icydb::diagnostic::QueryReadAdmissionCode::PublicQueryRequiresLimit,
            icydb::diagnostic::QueryReadAdmissionCode::PublicQueryRequiresIndex,
            icydb::diagnostic::QueryReadAdmissionCode::UnboundedFullScanRejected,
            icydb::diagnostic::QueryReadAdmissionCode::ScanBoundUnavailable,
            icydb::diagnostic::QueryReadAdmissionCode::ScanBoundExceedsPolicy,
            icydb::diagnostic::QueryReadAdmissionCode::EstimatedOnlyBoundRejected,
            icydb::diagnostic::QueryReadAdmissionCode::SortRequiresMaterialization,
            icydb::diagnostic::QueryReadAdmissionCode::MaterializationExceedsBudget,
            icydb::diagnostic::QueryReadAdmissionCode::ProjectionResponseMayExceedLimit,
            icydb::diagnostic::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
            icydb::diagnostic::QueryReadAdmissionCode::GroupedQueryExceedsBudget,
            icydb::diagnostic::QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute,
            icydb::diagnostic::QueryReadAdmissionCode::IntrospectionDisabledForLane,
            icydb::diagnostic::QueryReadAdmissionCode::UnsupportedStatementForQueryLane,
            icydb::diagnostic::QueryReadAdmissionCode::PublicQueryOffsetRejected,
            icydb::diagnostic::QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy,
        ];

        for reason in reasons {
            let rendered = render_query_read_admission_error(reason);
            let (_, fix) = rendered
                .split_once("; fix: ")
                .expect("read-admission diagnostics should render a fix hint");

            assert!(
                !fix.is_empty(),
                "read-admission diagnostics should render a non-empty fix hint: {rendered}",
            );
        }
    }

    #[test]
    fn renders_unknown_aggregate_target_field_code() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::from_code(
            icydb::diagnostic::DiagnosticCode::QueryUnknownAggregateTargetField,
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD: unknown aggregate target field",
        );
    }

    #[test]
    fn renders_query_result_shape_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryResultShapeMismatch,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::QueryResultShape {
                reason: icydb::diagnostic::QueryResultShapeCode::ExpectedRows,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_RESULT_SHAPE_MISMATCH: grouped query result cannot be consumed as entity rows",
        );
    }

    #[test]
    fn renders_sql_lowering_detail() {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryUnsupportedSqlFeature,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::SqlLowering {
                reason: icydb::diagnostic::SqlLoweringCode::DistinctOrderByProjection,
            }),
        ));

        assert_eq!(
            render_error(&err),
            "E_QUERY_UNSUPPORTED_SQL_FEATURE: unsupported SQL lowering: SELECT DISTINCT ORDER BY terms must be derivable from the projected tuple",
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

    fn render_query_read_admission_error(
        reason: icydb::diagnostic::QueryReadAdmissionCode,
    ) -> String {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission { reason }),
        ));

        render_error(&err)
    }
}
