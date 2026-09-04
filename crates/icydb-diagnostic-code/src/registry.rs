//! Module: registry
//!
//! Responsibility: single public diagnostic-code registry.
//! Does not own: diagnostic prose or runtime error construction.
//! Boundary: generates numeric constants and reconstruction from one table.

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorClass, ErrorCode, ErrorOrigin,
    QueryErrorKind, QueryProjectionCode, QueryReadAdmissionCode, RuntimeBoundaryCode,
    RuntimeErrorKind, SchemaDdlAdmissionCode, SchemaMigrationCode, SqlFeatureCode, SqlLoweringCode,
    SqlSurfaceMismatchCode, SqlWriteBoundaryCode,
};

macro_rules! define_error_code_registry {
    (
        $(
            $name:ident = $raw:literal => $diagnostic:ident
            $(, detail($detail_variant:ident { $($detail_body:tt)* }))?;
        )+
    ) => {
        impl ErrorCode {
            $(
                pub const $name: Self = Self($raw);
            )+

            /// Recover a known public error code from its raw wire value.
            #[must_use]
            pub const fn known(raw: u16) -> Option<Self> {
                match raw {
                    $(
                        $raw => Some(Self::$name),
                    )+
                    _ => None,
                }
            }

            /// Return whether this error code is in the public registry.
            #[must_use]
            pub const fn is_known(self) -> bool {
                match Self::known(self.raw()) {
                    Some(_) => true,
                    None => false,
                }
            }

            /// Collapse a rich diagnostic into one public leaf code.
            #[must_use]
            pub const fn from_parts(
                code: DiagnosticCode,
                detail: Option<DiagnosticDetail>,
            ) -> Self {
                match (code, detail) {
                    $(
                        $(
                            (
                                DiagnosticCode::$diagnostic,
                                Some(DiagnosticDetail::$detail_variant { $($detail_body)* }),
                            ) => {
                                Self::$name
                            }
                        )?
                    )+
                    _ => code.error_code(),
                }
            }

            /// Return the broad diagnostic reason represented by this public code.
            #[must_use]
            pub const fn diagnostic_code(self) -> DiagnosticCode {
                match self.raw() {
                    $(
                        $raw => DiagnosticCode::$diagnostic,
                    )+
                    _ => DiagnosticCode::RuntimeInternal,
                }
            }

            /// Return the diagnostic class represented by this public code.
            #[must_use]
            pub const fn class(self) -> ErrorClass {
                self.diagnostic_code().class()
            }

            /// Reconstruct rich diagnostic detail for host-side rendering, when known.
            #[must_use]
            pub const fn diagnostic_detail(self) -> Option<DiagnosticDetail> {
                match self.raw() {
                    $(
                        $(
                            $raw => Some(DiagnosticDetail::$detail_variant {
                                $($detail_body)*
                            }),
                        )?
                    )+
                    _ => None,
                }
            }

            /// Reconstruct a rich diagnostic payload for host-side rendering.
            #[must_use]
            pub const fn diagnostic(self, origin: ErrorOrigin) -> Diagnostic {
                Diagnostic::new(self.diagnostic_code(), origin, self.diagnostic_detail())
            }
        }

        impl DiagnosticDetail {
            /// Return the broad diagnostic reason required by this detail payload.
            #[must_use]
            pub const fn diagnostic_code(self) -> DiagnosticCode {
                match self {
                    $(
                        $(
                            Self::$detail_variant { $($detail_body)* } => {
                                DiagnosticCode::$diagnostic
                            }
                        )?
                    )+
                }
            }
        }

        #[cfg(test)]
        pub(super) const ORDERED_ERROR_CODES: &[ErrorCode] = &[
            $(
                ErrorCode::$name,
            )+
        ];

        #[cfg(test)]
        pub(super) const DETAIL_ERROR_CODES: &[(ErrorCode, DiagnosticCode, DiagnosticDetail)] = &[
            $(
                $(
                    (
                        ErrorCode::$name,
                        DiagnosticCode::$diagnostic,
                        DiagnosticDetail::$detail_variant {
                            $($detail_body)*
                        },
                    ),
                )?
            )+
        ];
    };
}

// This table is the public numeric registry. Raw values are wire-significant;
// keep them contiguous unless a gap is represented by an explicit reservation.
define_error_code_registry! {
    QUERY_VALIDATE = 1 => QueryValidate,
        detail(QueryKind { kind: QueryErrorKind::Validate });
    QUERY_INTENT = 2 => QueryIntent,
        detail(QueryKind { kind: QueryErrorKind::Intent });
    QUERY_PLAN = 3 => QueryPlan,
        detail(QueryKind { kind: QueryErrorKind::Plan });
    QUERY_UNORDERED_PAGINATION = 4 => QueryUnorderedPagination,
        detail(QueryKind { kind: QueryErrorKind::UnorderedPagination });
    QUERY_INVALID_CONTINUATION_CURSOR = 5 => QueryInvalidContinuationCursor,
        detail(QueryKind { kind: QueryErrorKind::InvalidContinuationCursor });
    QUERY_NOT_FOUND = 6 => QueryNotFound,
        detail(QueryKind { kind: QueryErrorKind::NotFound });
    QUERY_NOT_UNIQUE = 7 => QueryNotUnique,
        detail(QueryKind { kind: QueryErrorKind::NotUnique });
    QUERY_NUMERIC_OVERFLOW = 8 => QueryNumericOverflow;
    QUERY_NUMERIC_NOT_REPRESENTABLE = 9 => QueryNumericNotRepresentable;
    QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD = 10 => QueryUnknownAggregateTargetField;
    QUERY_UNSUPPORTED_SQL_FEATURE = 11 => QueryUnsupportedSqlFeature;
    QUERY_SQL_SURFACE_MISMATCH = 12 => QuerySqlSurfaceMismatch;
    SCHEMA_DDL_ADMISSION = 13 => SchemaDdlAdmission;
    STORE_NOT_FOUND = 14 => StoreNotFound;
    STORE_CORRUPTION = 15 => StoreCorruption;
    STORE_INVARIANT_VIOLATION = 16 => StoreInvariantViolation;
    RUNTIME_CORRUPTION = 17 => RuntimeCorruption,
        detail(RuntimeKind { kind: RuntimeErrorKind::Corruption });
    RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT = 18 => RuntimeIncompatiblePersistedFormat,
        detail(RuntimeKind { kind: RuntimeErrorKind::IncompatiblePersistedFormat });
    RUNTIME_INVARIANT_VIOLATION = 19 => RuntimeInvariantViolation,
        detail(RuntimeKind { kind: RuntimeErrorKind::InvariantViolation });
    RUNTIME_CONFLICT = 20 => RuntimeConflict,
        detail(RuntimeKind { kind: RuntimeErrorKind::Conflict });
    RUNTIME_NOT_FOUND = 21 => RuntimeNotFound,
        detail(RuntimeKind { kind: RuntimeErrorKind::NotFound });
    RUNTIME_UNSUPPORTED = 22 => RuntimeUnsupported,
        detail(RuntimeKind { kind: RuntimeErrorKind::Unsupported });
    RUNTIME_INTERNAL = 23 => RuntimeInternal,
        detail(RuntimeKind { kind: RuntimeErrorKind::Internal });

    RUNTIME_BOUNDARY_SQL_SURFACE_CONTROLLER_REQUIRED = 24 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlSurfaceControllerRequired });
    RUNTIME_BOUNDARY_SCHEMA_SURFACE_CONTROLLER_REQUIRED = 25 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SchemaSurfaceControllerRequired });
    RUNTIME_BOUNDARY_SQL_QUERY_NO_CONFIGURED_ENTITIES = 26 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlQueryNoConfiguredEntities });
    RUNTIME_BOUNDARY_SQL_QUERY_ENTITY_NOT_FOUND = 27 => RuntimeNotFound,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlQueryEntityNotFound });
    RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED = 28 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlDdlTargetRequired });
    RUNTIME_BOUNDARY_SQL_DDL_ENTITY_NOT_CONFIGURED = 29 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlDdlEntityNotConfigured });
    SQL_FEATURE_AGGREGATE_FILTER_CLAUSE = 30 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AggregateFilterClause });
    SQL_FEATURE_ALTER_STATEMENT_BEYOND_ALTER_TABLE = 31 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterStatementBeyondAlterTable });
    SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_DUPLICATE_DEFAULT = 32 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddColumnDuplicateDefault });
    SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_MODIFIERS = 33 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_ADD_STATEMENT_BEYOND_ADD_COLUMN = 34 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddStatementBeyondAddColumn });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_DROP_UNSUPPORTED_ACTION = 35 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_MODIFIERS = 36 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_SET_UNSUPPORTED_ACTION = 37 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_UNSUPPORTED_ACTION = 38 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_STATEMENT_BEYOND_ALTER_COLUMN = 39 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn });
    SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_IF_EXISTS_SYNTAX = 40 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropColumnIfExistsSyntax });
    SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_MODIFIERS = 41 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_DROP_STATEMENT_BEYOND_DROP_COLUMN = 42 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropStatementBeyondDropColumn });
    SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MISSING_TO = 43 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameColumnMissingTo });
    SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MODIFIERS = 44 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_RENAME_STATEMENT_BEYOND_RENAME_COLUMN = 45 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn });
    SQL_FEATURE_ALTER_TABLE_UNSUPPORTED_OPERATION = 46 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableUnsupportedOperation });
    SQL_FEATURE_COLUMN_ALIAS = 47 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ColumnAlias });
    SQL_FEATURE_CREATE_INDEX_IF_NOT_EXISTS_SYNTAX = 48 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexIfNotExistsSyntax });
    SQL_FEATURE_CREATE_INDEX_KEY_ORDERING_MODIFIERS = 49 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexKeyOrderingModifiers });
    SQL_FEATURE_CREATE_INDEX_MODIFIERS = 50 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexModifiers });
    SQL_FEATURE_CREATE_STATEMENT_BEYOND_CREATE_INDEX = 51 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateStatementBeyondCreateIndex });
    SQL_FEATURE_DESCRIBE_MODIFIER = 52 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DescribeModifier });
    SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_EXPECTED_CLAUSE = 53 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause });
    SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_SET_CLAUSE = 54 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DdlSchemaVersionDuplicateSetClause });
    SQL_FEATURE_DROP_INDEX_MODIFIERS = 55 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropIndexModifiers });
    SQL_FEATURE_DROP_INDEX_IF_EXISTS_SYNTAX = 56 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropIndexIfExistsSyntax });
    SQL_FEATURE_DROP_STATEMENT_BEYOND_DROP_INDEX = 57 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropStatementBeyondDropIndex });
    SQL_FEATURE_EXPRESSION_INDEX_UNSUPPORTED_FUNCTION = 58 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ExpressionIndexUnsupportedFunction });
    SQL_FEATURE_HAVING = 59 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Having });
    SQL_FEATURE_INSERT = 60 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Insert });
    SQL_FEATURE_JOIN = 61 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Join });
    SQL_FEATURE_LIKE_PATTERN_BEYOND_TRAILING_PREFIX = 62 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::LikePatternBeyondTrailingPrefix });
    SQL_FEATURE_LOWER_FIELD_PREDICATE_UNSUPPORTED = 63 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::LowerFieldPredicateUnsupported });
    SQL_FEATURE_MULTI_STATEMENT_SQL = 64 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::MultiStatementSql });
    SQL_FEATURE_NESTED_AGGREGATE_INPUT = 65 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NestedAggregateInput });
    SQL_FEATURE_NESTED_PROJECTION_FUNCTION_IN_ARITHMETIC = 66 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NestedProjectionFunctionInArithmetic });
    SQL_FEATURE_ORDER_BY_UNSUPPORTED_FORM = 67 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::OrderByUnsupportedForm });
    SQL_FEATURE_OTHER = 68 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Other });
    SQL_FEATURE_PREDICATE_STARTS_WITH_FIRST_ARGUMENT = 69 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::PredicateStartsWithFirstArgument });
    SQL_FEATURE_QUOTED_IDENTIFIERS = 70 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::QuotedIdentifiers });
    SQL_FEATURE_RETURNING_UNSUPPORTED_SHAPE = 71 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ReturningUnsupportedShape });
    SQL_FEATURE_SCALAR_FUNCTION_EXPRESSION_POSITION = 72 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ScalarFunctionExpressionPosition });
    SQL_FEATURE_SCALE_TAKING_NUMERIC_FUNCTION_EXPRESSION_POSITION = 73 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ScaleTakingNumericFunctionExpressionPosition });
    SQL_FEATURE_SHOW_COLUMNS_MODIFIERS = 74 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowColumnsModifiers });
    SQL_FEATURE_SHOW_ENTITIES_MODIFIERS = 75 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowEntitiesModifiers });
    SQL_FEATURE_SHOW_INDEXES_MODIFIERS = 76 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowIndexesModifiers });
    SQL_FEATURE_SHOW_MEMORY_MODIFIERS = 77 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowMemoryModifiers });
    SQL_FEATURE_SHOW_STORES_MODIFIERS = 78 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowStoresModifiers });
    SQL_FEATURE_SHOW_UNSUPPORTED_COMMAND = 79 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowUnsupportedCommand });
    SQL_FEATURE_SIMPLE_CASE_EXPRESSION = 80 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SimpleCaseExpression });
    SQL_FEATURE_STANDALONE_LITERAL_PROJECTION_ITEM = 81 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::StandaloneLiteralProjectionItem });
    SQL_FEATURE_UNION_INTERSECT_EXCEPT = 82 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UnionIntersectExcept });
    SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE = 83 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UnsupportedFunctionNamespace });
    SQL_FEATURE_UPDATE = 84 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Update });
    SQL_FEATURE_UPPER_FIELD_PREDICATE_UNSUPPORTED = 85 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UpperFieldPredicateUnsupported });
    SQL_FEATURE_WINDOW_FUNCTION = 86 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::WindowFunction });
    SQL_FEATURE_WITH = 87 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::With });
    SQL_FEATURE_NUMERIC_SCALE_FUNCTION_ARGUMENTS = 88 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NumericScaleFunctionArguments });
    SQL_FEATURE_ORDER_BY_FIELD_NOT_ORDERABLE = 89 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::OrderByFieldNotOrderable });

    SQL_SURFACE_QUERY_REJECTS_INSERT = 90 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsInsert });
    SQL_SURFACE_QUERY_REJECTS_UPDATE = 91 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsUpdate });
    SQL_SURFACE_QUERY_REJECTS_DELETE = 92 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsDelete });
    SQL_SURFACE_MUTATION_REJECTS_SELECT = 93 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsSelect });
    SQL_SURFACE_MUTATION_REJECTS_EXPLAIN = 94 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsExplain });
    SQL_SURFACE_MUTATION_REJECTS_DESCRIBE = 95 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsDescribe });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_INDEXES = 96 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowIndexes });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_COLUMNS = 97 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowColumns });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_ENTITIES = 98 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowEntities });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_STORES = 99 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowStores });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_MEMORY = 100 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowMemory });

    SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION = 101 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::MissingExpectedSchemaVersion });
    SCHEMA_DDL_MISSING_NEXT_SCHEMA_VERSION = 102 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::MissingNextSchemaVersion });
    SCHEMA_DDL_STALE_EXPECTED_SCHEMA_VERSION = 103 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::StaleExpectedSchemaVersion });
    SCHEMA_DDL_INVALID_EXPECTED_SCHEMA_VERSION = 104 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion });
    SCHEMA_DDL_INVALID_NEXT_SCHEMA_VERSION = 105 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidNextSchemaVersion });
    SCHEMA_DDL_ACCEPTED_SCHEMA_CHANGE_WITHOUT_VERSION_BUMP = 106 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump });
    SCHEMA_DDL_EMPTY_VERSION_BUMP = 107 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::EmptyVersionBump });
    SCHEMA_DDL_VERSION_GAP = 108 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::VersionGap });
    SCHEMA_DDL_VERSION_ROLLBACK = 109 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::VersionRollback });
    SCHEMA_DDL_FINGERPRINT_METHOD_MISMATCH = 110 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::FingerprintMethodMismatch });
    SCHEMA_DDL_UNSUPPORTED_TRANSITION_CLASS = 111 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::UnsupportedTransitionClass });
    SCHEMA_DDL_PHYSICAL_RUNNER_MISSING = 112 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::PhysicalRunnerMissing });
    SCHEMA_DDL_VALIDATION_FAILED = 113 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::ValidationFailed });
    SCHEMA_DDL_PUBLICATION_RACE_LOST = 114 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::PublicationRaceLost });
    SCHEMA_DDL_INVALID_ADD_COLUMN_DEFAULT = 115 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidAddColumnDefault });
    SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT = 116 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidAlterColumnDefault });
    SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED = 117 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedIndexDropRejected });
    SCHEMA_DDL_REWRITE_REQUIRES_MIGRATION = 118 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::SchemaRewriteRequiresMigration });
    SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED = 119 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected });
    SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED = 120 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected });
    SQL_FEATURE_SHOW_CONSTRAINTS_MODIFIERS = 121 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowConstraintsModifiers });
    QUERY_SQL_WRITE_BOUNDARY = 122 => QuerySqlWriteBoundary;
    SQL_WRITE_PRIMARY_KEY_LITERAL_INCOMPATIBLE = 123 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible });
    SQL_WRITE_MISSING_PRIMARY_KEY = 124 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::MissingPrimaryKey });
    SQL_WRITE_MISSING_REQUIRED_FIELDS = 125 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::MissingRequiredFields });
    SQL_WRITE_EXPLICIT_MANAGED_FIELD = 126 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExplicitManagedField });
    SQL_WRITE_EXPLICIT_GENERATED_FIELD = 127 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExplicitGeneratedField });
    SQL_WRITE_INSERT_SELECT_REQUIRES_SCALAR = 128 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectRequiresScalar });
    SQL_WRITE_INSERT_SELECT_AGGREGATE_PROJECTION = 129 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectAggregateProjection });
    SQL_WRITE_INSERT_SELECT_WIDTH_MISMATCH = 130 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectWidthMismatch });
    SQL_WRITE_UPDATE_PRIMARY_KEY_MUTATION = 131 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdatePrimaryKeyMutation });
    SQL_WRITE_INVALID_FIELD_LITERAL = 132 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InvalidFieldLiteral });
    SQL_WRITE_UNKNOWN_RETURNING_FIELD = 133 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UnknownReturningField });
    SQL_WRITE_DUPLICATE_RETURNING_FIELD = 134 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::DuplicateReturningField });
    SQL_WRITE_UPDATE_MISSING_WHERE_PREDICATE = 135 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdateMissingWherePredicate });
    SQL_WRITE_ORDER_BY_UNSUPPORTED_SHAPE = 136 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::WriteOrderByUnsupportedShape });

    QUERY_UNSUPPORTED_PROJECTION = 137 => QueryUnsupportedProjection;
    QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED = 138 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericLiteralRequired });
    QUERY_PROJECTION_NUMERIC_SCALE_ARGUMENTS = 139 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericScaleArguments });
    QUERY_PROJECTION_NESTED_FIELD_PATH_PREVIEW = 140 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NestedFieldPathPreview });
    QUERY_PROJECTION_CASE_CONDITION_BOOLEAN_REQUIRED = 141 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::CaseConditionBooleanRequired });
    QUERY_PROJECTION_NUMERIC_INPUT_REQUIRED = 142 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericInputRequired });
    QUERY_PROJECTION_TEXT_OR_BLOB_INPUT_REQUIRED = 143 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextOrBlobInputRequired });
    QUERY_PROJECTION_TEXT_INPUT_REQUIRED = 144 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextInputRequired });
    QUERY_PROJECTION_TEXT_OR_NULL_ARGUMENT_REQUIRED = 145 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextOrNullArgumentRequired });
    QUERY_PROJECTION_INTEGER_OR_NULL_ARGUMENT_REQUIRED = 146 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::IntegerOrNullArgumentRequired });
    QUERY_PROJECTION_UNARY_OPERAND_INCOMPATIBLE = 147 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::UnaryOperandIncompatible });
    QUERY_PROJECTION_BINARY_OPERANDS_INCOMPATIBLE = 148 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::BinaryOperandsIncompatible });

    SQL_LOWERING_ENTITY_MISMATCH = 149 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::EntityMismatch });
    SQL_LOWERING_SELECT_PROJECTION_SHAPE = 150 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectProjectionShape });
    SQL_LOWERING_SELECT_DISTINCT = 151 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectDistinct });
    SQL_LOWERING_DISTINCT_ORDER_BY_PROJECTION = 152 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::DistinctOrderByProjection });
    SQL_LOWERING_GLOBAL_AGGREGATE_PROJECTION = 153 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GlobalAggregateProjection });
    SQL_LOWERING_GLOBAL_AGGREGATE_GROUP_BY = 154 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GlobalAggregateGroupBy });
    SQL_LOWERING_SELECT_GROUP_BY_SHAPE = 155 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectGroupByShape });
    SQL_LOWERING_GROUPED_PROJECTION_EXPLICIT_LIST_REQUIRED = 156 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionExplicitListRequired });
    SQL_LOWERING_GROUPED_PROJECTION_AGGREGATE_REQUIRED = 157 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionAggregateRequired });
    SQL_LOWERING_GROUPED_PROJECTION_NON_GROUP_FIELD = 158 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionNonGroupField });
    SQL_LOWERING_GROUPED_PROJECTION_SCALAR_AFTER_AGGREGATE = 159 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionScalarAfterAggregate });
    SQL_LOWERING_HAVING_REQUIRES_GROUP_BY = 160 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::HavingRequiresGroupBy });
    SQL_LOWERING_SELECT_HAVING_SHAPE = 161 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectHavingShape });
    SQL_LOWERING_AGGREGATE_INPUT_EXPRESSIONS = 162 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::AggregateInputExpressions });
    SQL_LOWERING_WHERE_EXPRESSION_SHAPE = 163 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::WhereExpressionShape });
    SQL_LOWERING_PARAMETER_PLACEMENT = 164 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::ParameterPlacement });
    SQL_LOWERING_SQL_DDL_EXECUTION_UNSUPPORTED = 165 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SqlDdlExecutionUnsupported });

    SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE = 166 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ReturningResponseTooLarge });
    SQL_WRITE_RETURNING_ROWS_TOO_MANY = 167 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ReturningRowsTooMany });
    RUNTIME_BOUNDARY_SQL_INTROSPECTION_DISABLED = 168 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlIntrospectionDisabled });
    SQL_WRITE_STAGED_ROWS_TOO_MANY = 169 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::StagedRowsTooMany });

    QUERY_READ_ADMISSION = 170 => QueryReadAdmission;
    QUERY_READ_PUBLIC_REQUIRES_LIMIT = 171 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PublicQueryRequiresLimit });
    QUERY_READ_PUBLIC_REQUIRES_INDEX = 172 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PublicQueryRequiresIndex });
    QUERY_READ_UNBOUNDED_FULL_SCAN_REJECTED = 173 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::UnboundedFullScanRejected });
    QUERY_READ_SORT_REQUIRES_MATERIALIZATION = 174 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::SortRequiresMaterialization });
    QUERY_READ_GROUPED_QUERY_REQUIRES_LIMITS = 175 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::GroupedQueryRequiresLimits });
    QUERY_READ_GROUPED_QUERY_EXCEEDS_BUDGET = 176 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::GroupedQueryExceedsBudget });
    QUERY_READ_DIAGNOSTIC_LANE_DOES_NOT_EXECUTE = 177 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute });
    QUERY_READ_RETURNED_ROW_BOUND_EXCEEDS_POLICY = 178 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy });
    QUERY_READ_PRIMARY_KEY_INPUT_EXCEEDS_POLICY = 179 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy });

    SCHEMA_DDL_ROW_LAYOUT_VERSION_EXHAUSTED = 180 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::RowLayoutVersionExhausted });
    SCHEMA_DDL_TRANSITION_BUDGET_EXCEEDED = 181 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::SchemaTransitionBudgetExceeded });
    SQL_WRITE_INSERT_DEFAULT_REQUIRED_FIELD = 182 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertDefaultRequiredField });
    SQL_WRITE_UPDATE_DEFAULT_REQUIRED_FIELD = 183 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdateDefaultRequiredField });
    SQL_WRITE_UPDATE_DEFAULT_DATABASE_OWNED_FIELD = 184 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdateDefaultDatabaseOwnedField });
    RUNTIME_BOUNDARY_MUTATION_REQUIRED_FIELD_MISSING = 185 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationRequiredFieldMissing });
    RUNTIME_BOUNDARY_PERSISTED_ROW_LAYOUT_OUTSIDE_ACCEPTED_WINDOW = 186 => RuntimeCorruption,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::PersistedRowLayoutOutsideAcceptedWindow });
    RUNTIME_BOUNDARY_PERSISTED_ROW_SLOT_COUNT_MISMATCH = 187 => RuntimeCorruption,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::PersistedRowSlotCountMismatch });
    RUNTIME_BOUNDARY_GENERATED_FIELD_AFTER_DDL_FIELD = 188 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::GeneratedFieldAfterDdlField });
    SQL_SURFACE_MUTATION_REQUIRES_EXPLICIT_UPDATE_INTENT = 189 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRequiresExplicitUpdateIntent });
    SQL_WRITE_EXACT_UPDATE_ASSERTION_REQUIRED = 190 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateAssertionRequired });
    SQL_WRITE_EXACT_UPDATE_ASSERTION_TOO_HIGH = 191 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateAssertionTooHigh });
    SQL_WRITE_EXACT_UPDATE_AFFECTED_ROWS_EXCEEDED = 192 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateAffectedRowsExceeded });
    SQL_WRITE_EXACT_UPDATE_WINDOW_UNSUPPORTED = 193 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateWindowUnsupported });
    SQL_WRITE_EXACT_UPDATE_SCAN_BUDGET_EXCEEDED = 194 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateScanBudgetExceeded });
    SQL_WRITE_RESUMABLE_UPDATE_WINDOW_UNSUPPORTED = 195 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateWindowUnsupported });
    SQL_WRITE_RESUMABLE_UPDATE_RETURNING_UNSUPPORTED = 196 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateReturningUnsupported });
    SQL_WRITE_RESUMABLE_UPDATE_REQUIRES_JOURNALED_STORE = 197 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore });
    SQL_WRITE_RESUMABLE_UPDATE_ASSIGNED_FIELD_HAS_GLOBAL_CONSTRAINT = 198 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateAssignedFieldHasGlobalConstraint });
    SQL_WRITE_RESUMABLE_UPDATE_SCOPE_DEPENDS_ON_ASSIGNED_FIELD = 199 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateScopeDependsOnAssignedField });
    SQL_WRITE_RESUMABLE_UPDATE_SCOPE_DEPENDENCY_UNKNOWN = 200 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateScopeDependencyUnknown });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_MALFORMED = 201 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationMalformed });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_TARGET_MISMATCH = 202 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationTargetMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_SCHEMA_MISMATCH = 203 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationSchemaMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_SCOPE_MISMATCH = 204 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationScopeMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_PATCH_MISMATCH = 205 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationPatchMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_BATCH_POLICY_MISMATCH = 206 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationBatchPolicyMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_MANAGED_FIELD_HAS_GLOBAL_CONSTRAINT = 207 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateManagedFieldHasGlobalConstraint });
    RUNTIME_BOUNDARY_JOURNAL_MUTATION_REVISION_EXHAUSTED = 208 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::JournalMutationRevisionExhausted });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_CONSTRAINTS = 209 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowConstraints });
    RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION = 210 => RuntimeInvariantViolation,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ConstraintViolation });
    RUNTIME_BOUNDARY_ACCEPTED_ROW_CONSTRAINT_PROGRAM_CORRUPT = 211 => RuntimeCorruption,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::AcceptedRowConstraintProgramCorrupt });
    RUNTIME_BOUNDARY_CONSTRAINT_ACTIVATION_WRITE_BLOCKED = 212 => RuntimeConflict,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ConstraintActivationWriteBlocked });
    SQL_FEATURE_ALTER_TABLE_ADD_CONSTRAINT_BEYOND_CHECK = 213 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddConstraintBeyondCheck });
    SQL_FEATURE_ALTER_TABLE_ADD_CONSTRAINT_MODIFIERS = 214 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddConstraintModifiers });
    SQL_FEATURE_ALTER_TABLE_DROP_CONSTRAINT_IF_EXISTS_SYNTAX = 215 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropConstraintIfExistsSyntax });
    SQL_FEATURE_ALTER_TABLE_DROP_CONSTRAINT_MODIFIERS = 216 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropConstraintModifiers });
    SQL_FEATURE_ALTER_TABLE_VALIDATE_BEYOND_CONSTRAINT = 217 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableValidateBeyondConstraint });
    SQL_FEATURE_ALTER_TABLE_VALIDATE_CONSTRAINT_MODIFIERS = 218 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableValidateConstraintModifiers });
    RUNTIME_BOUNDARY_GENERATED_CONSTRAINT_ACTIVATION_STALE = 219 => RuntimeConflict,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::GeneratedConstraintActivationStale });
    RUNTIME_BOUNDARY_MUTATION_MANAGED_TIMESTAMP_REGRESSION = 220 => RuntimeInvariantViolation,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationManagedTimestampRegression });
    RUNTIME_BOUNDARY_MUTATION_DATABASE_OWNED_FIELD_EXPLICIT = 221 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationDatabaseOwnedFieldExplicit });
    RUNTIME_BOUNDARY_MUTATION_BATCH_EMPTY = 222 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchEmpty });
    RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS = 223 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchTooManyItems });
    RUNTIME_BOUNDARY_MUTATION_BATCH_STAGED_BYTES_EXCEEDED = 224 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchStagedBytesExceeded });
    RUNTIME_BOUNDARY_MUTATION_BATCH_RESULT_BYTES_EXCEEDED = 225 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchResultBytesExceeded });
    RUNTIME_BOUNDARY_MUTATION_BATCH_STORE_MISMATCH = 226 => RuntimeConflict,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchStoreMismatch });
    RUNTIME_BOUNDARY_MUTATION_BATCH_DUPLICATE_KEY = 227 => RuntimeConflict,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchDuplicateKey });
    RUNTIME_BOUNDARY_OPERATIONAL_SURFACE_CONTROLLER_REQUIRED = 228 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::OperationalSurfaceControllerRequired });
    SCHEMA_MIGRATION_UNADOPTED = 229 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::Unadopted });
    SCHEMA_MIGRATION_MISSING_MIGRATION = 230 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::MissingMigration });
    SCHEMA_MIGRATION_VERSION_GAP = 231 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::VersionGap });
    SCHEMA_MIGRATION_DOWNGRADE = 232 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::Downgrade });
    SCHEMA_MIGRATION_EMPTY_ENTITY_VERSION_BUMP = 233 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::EmptyEntityVersionBump });
    SCHEMA_MIGRATION_STALE_ACCEPTED_HEAD = 234 => RuntimeConflict,
        detail(SchemaMigration { reason: SchemaMigrationCode::StaleAcceptedHead });
    SCHEMA_MIGRATION_PLAN_CHANGED = 235 => RuntimeConflict,
        detail(SchemaMigration { reason: SchemaMigrationCode::PlanChanged });
    SCHEMA_MIGRATION_UNKNOWN_FROM_OBJECT = 236 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::UnknownFromObject });
    SCHEMA_MIGRATION_UNKNOWN_TO_OBJECT = 237 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::UnknownToObject });
    SCHEMA_MIGRATION_KIND_MISMATCH = 238 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::KindMismatch });
    SCHEMA_MIGRATION_IDENTITY_CONFLICT = 239 => RuntimeConflict,
        detail(SchemaMigration { reason: SchemaMigrationCode::IdentityConflict });
    SCHEMA_MIGRATION_UNEXPLAINED_SCHEMA_DIFFERENCE = 240 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::UnexplainedSchemaDifference });
    SCHEMA_MIGRATION_UNSUPPORTED_TRANSFORM = 241 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::UnsupportedTransform });
    SCHEMA_MIGRATION_PHYSICAL_RUNNER_MISSING = 242 => RuntimeUnsupported,
        detail(SchemaMigration { reason: SchemaMigrationCode::PhysicalRunnerMissing });
    SCHEMA_MIGRATION_IN_PROGRESS = 243 => RuntimeConflict,
        detail(SchemaMigration { reason: SchemaMigrationCode::MigrationInProgress });
    SCHEMA_MIGRATION_ABORT_TOO_LATE = 244 => RuntimeConflict,
        detail(SchemaMigration { reason: SchemaMigrationCode::AbortTooLate });
    SCHEMA_MIGRATION_PROGRESS_CORRUPT = 245 => RuntimeCorruption,
        detail(SchemaMigration { reason: SchemaMigrationCode::ProgressCorrupt });
    SCHEMA_MIGRATION_CANDIDATE_MISMATCH = 246 => RuntimeCorruption,
        detail(SchemaMigration { reason: SchemaMigrationCode::CandidateMismatch });
    SCHEMA_MIGRATION_PUBLICATION_RACE_LOST = 247 => RuntimeConflict,
        detail(SchemaMigration { reason: SchemaMigrationCode::PublicationRaceLost });
    RUNTIME_BOUNDARY_EXACT_KEY_BATCH_TOO_MANY_ITEMS = 248 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ExactKeyBatchTooManyItems });
    RUNTIME_BOUNDARY_EXACT_KEY_BATCH_INPUT_BYTES_EXCEEDED = 249 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ExactKeyBatchInputBytesExceeded });
    RUNTIME_BOUNDARY_EXACT_KEY_BATCH_STORED_BYTES_EXCEEDED = 250 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ExactKeyBatchStoredBytesExceeded });
    RUNTIME_BOUNDARY_EXACT_KEY_BATCH_RESULT_BYTES_EXCEEDED = 251 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ExactKeyBatchResultBytesExceeded });
    RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED = 252 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded });
    RUNTIME_BOUNDARY_PAGE_UNIT_TOO_LARGE = 253 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::PageUnitTooLarge });
    RUNTIME_BOUNDARY_REQUEST_EXECUTION_SCOPE_REQUIRED = 254 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::RequestExecutionScopeRequired });
    RUNTIME_BOUNDARY_REQUEST_EXECUTION_ROOT_MISMATCH = 255 => RuntimeConflict,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::RequestExecutionRootMismatch });
    SQL_FEATURE_SHOW_RELATIONS_MODIFIERS = 256 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowRelationsModifiers });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_RELATIONS = 257 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowRelations });
    RUNTIME_BOUNDARY_SQL_QUERY_REPLY_BYTES_EXCEEDED = 258 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlQueryReplyBytesExceeded });
    RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING = 259 => RuntimeConflict,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::DatabaseStartupRecoveryPending });
    RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED = 260 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlSurfacePolicyDenied });
    RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED = 261 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SchemaSurfacePolicyDenied });
    RUNTIME_BOUNDARY_MUTATION_BATCH_COMMIT_WORK_EXCEEDED = 262 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchCommitWorkExceeded });
    RUNTIME_BOUNDARY_CONVERGENCE_BACKLOG_PRESSURE = 263 => RuntimeConflict,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::ConvergenceBacklogPressure });
    RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ENTITIES = 264 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationBatchTooManyEntities });
}
