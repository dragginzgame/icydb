//! Module: registry
//!
//! Responsibility: single public diagnostic-code registry.
//! Does not own: diagnostic prose or runtime error construction.
//! Boundary: generates numeric constants and reconstruction from one table.

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorClass, ErrorCode, ErrorOrigin,
    QueryErrorKind, QueryProjectionCode, QueryReadAdmissionCode, QueryResultShapeCode,
    RuntimeBoundaryCode, RuntimeErrorKind, SchemaDdlAdmissionCode, SqlFeatureCode, SqlLoweringCode,
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
    QUERY_ACCESS_REQUIREMENT = 4 => QueryAccessRequirement,
        detail(QueryKind { kind: QueryErrorKind::AccessRequirement });
    QUERY_UNORDERED_PAGINATION = 5 => QueryUnorderedPagination,
        detail(QueryKind { kind: QueryErrorKind::UnorderedPagination });
    QUERY_INVALID_CONTINUATION_CURSOR = 6 => QueryInvalidContinuationCursor,
        detail(QueryKind { kind: QueryErrorKind::InvalidContinuationCursor });
    QUERY_NOT_FOUND = 7 => QueryNotFound,
        detail(QueryKind { kind: QueryErrorKind::NotFound });
    QUERY_NOT_UNIQUE = 8 => QueryNotUnique,
        detail(QueryKind { kind: QueryErrorKind::NotUnique });
    QUERY_NUMERIC_OVERFLOW = 9 => QueryNumericOverflow;
    QUERY_NUMERIC_NOT_REPRESENTABLE = 10 => QueryNumericNotRepresentable;
    QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD = 11 => QueryUnknownAggregateTargetField;
    QUERY_UNSUPPORTED_SQL_FEATURE = 12 => QueryUnsupportedSqlFeature;
    QUERY_SQL_SURFACE_MISMATCH = 13 => QuerySqlSurfaceMismatch;
    SCHEMA_DDL_ADMISSION = 14 => SchemaDdlAdmission;
    STORE_NOT_FOUND = 15 => StoreNotFound;
    STORE_CORRUPTION = 16 => StoreCorruption;
    STORE_INVARIANT_VIOLATION = 17 => StoreInvariantViolation;
    RUNTIME_CORRUPTION = 18 => RuntimeCorruption,
        detail(RuntimeKind { kind: RuntimeErrorKind::Corruption });
    RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT = 19 => RuntimeIncompatiblePersistedFormat,
        detail(RuntimeKind { kind: RuntimeErrorKind::IncompatiblePersistedFormat });
    RUNTIME_INVARIANT_VIOLATION = 20 => RuntimeInvariantViolation,
        detail(RuntimeKind { kind: RuntimeErrorKind::InvariantViolation });
    RUNTIME_CONFLICT = 21 => RuntimeConflict,
        detail(RuntimeKind { kind: RuntimeErrorKind::Conflict });
    RUNTIME_NOT_FOUND = 22 => RuntimeNotFound,
        detail(RuntimeKind { kind: RuntimeErrorKind::NotFound });
    RUNTIME_UNSUPPORTED = 23 => RuntimeUnsupported,
        detail(RuntimeKind { kind: RuntimeErrorKind::Unsupported });
    RUNTIME_INTERNAL = 24 => RuntimeInternal,
        detail(RuntimeKind { kind: RuntimeErrorKind::Internal });

    RUNTIME_BOUNDARY_SQL_SURFACE_CONTROLLER_REQUIRED = 25 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlSurfaceControllerRequired });
    RUNTIME_BOUNDARY_SCHEMA_SURFACE_CONTROLLER_REQUIRED = 26 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SchemaSurfaceControllerRequired });
    RUNTIME_BOUNDARY_SQL_QUERY_NO_CONFIGURED_ENTITIES = 27 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlQueryNoConfiguredEntities });
    RUNTIME_BOUNDARY_SQL_QUERY_ENTITY_NOT_CONFIGURED = 28 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlQueryEntityNotConfigured });
    RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED = 29 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlDdlTargetRequired });
    RUNTIME_BOUNDARY_SQL_DDL_ENTITY_NOT_CONFIGURED = 30 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlDdlEntityNotConfigured });
    RUNTIME_BOUNDARY_QUERY_RESPONSE_ROWS_REQUIRED = 31 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::QueryResponseRowsRequired });
    RUNTIME_BOUNDARY_QUERY_RESPONSE_GROUPED_ROWS_REQUIRED = 32 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::QueryResponseGroupedRowsRequired });
    RUNTIME_BOUNDARY_ROW_PROJECTION_FIELD_NOT_CONFIGURED = 33 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::RowProjectionFieldNotConfigured });

    SQL_FEATURE_AGGREGATE_FILTER_CLAUSE = 34 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AggregateFilterClause });
    SQL_FEATURE_ALTER_STATEMENT_BEYOND_ALTER_TABLE = 35 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterStatementBeyondAlterTable });
    SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_DUPLICATE_DEFAULT = 36 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddColumnDuplicateDefault });
    SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_MODIFIERS = 37 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_ADD_STATEMENT_BEYOND_ADD_COLUMN = 38 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddStatementBeyondAddColumn });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_DROP_UNSUPPORTED_ACTION = 39 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_MODIFIERS = 40 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_SET_UNSUPPORTED_ACTION = 41 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_UNSUPPORTED_ACTION = 42 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_STATEMENT_BEYOND_ALTER_COLUMN = 43 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn });
    SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_IF_EXISTS_SYNTAX = 44 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropColumnIfExistsSyntax });
    SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_MODIFIERS = 45 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_DROP_STATEMENT_BEYOND_DROP_COLUMN = 46 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropStatementBeyondDropColumn });
    SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MISSING_TO = 47 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameColumnMissingTo });
    SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MODIFIERS = 48 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_RENAME_STATEMENT_BEYOND_RENAME_COLUMN = 49 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn });
    SQL_FEATURE_ALTER_TABLE_UNSUPPORTED_OPERATION = 50 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableUnsupportedOperation });
    SQL_FEATURE_COLUMN_ALIAS = 51 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ColumnAlias });
    SQL_FEATURE_CREATE_INDEX_IF_NOT_EXISTS_SYNTAX = 52 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexIfNotExistsSyntax });
    SQL_FEATURE_CREATE_INDEX_KEY_ORDERING_MODIFIERS = 53 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexKeyOrderingModifiers });
    SQL_FEATURE_CREATE_INDEX_MODIFIERS = 54 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexModifiers });
    SQL_FEATURE_CREATE_STATEMENT_BEYOND_CREATE_INDEX = 55 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateStatementBeyondCreateIndex });
    SQL_FEATURE_DESCRIBE_MODIFIER = 56 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DescribeModifier });
    SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_EXPECTED_CLAUSE = 57 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause });
    SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_SET_CLAUSE = 58 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DdlSchemaVersionDuplicateSetClause });
    SQL_FEATURE_DROP_INDEX_MODIFIERS = 59 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropIndexModifiers });
    SQL_FEATURE_DROP_INDEX_IF_EXISTS_SYNTAX = 60 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropIndexIfExistsSyntax });
    SQL_FEATURE_DROP_STATEMENT_BEYOND_DROP_INDEX = 61 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropStatementBeyondDropIndex });
    SQL_FEATURE_EXPRESSION_INDEX_UNSUPPORTED_FUNCTION = 62 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ExpressionIndexUnsupportedFunction });
    SQL_FEATURE_HAVING = 63 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Having });
    SQL_FEATURE_INSERT = 64 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Insert });
    SQL_FEATURE_JOIN = 65 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Join });
    SQL_FEATURE_LIKE_PATTERN_BEYOND_TRAILING_PREFIX = 66 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::LikePatternBeyondTrailingPrefix });
    SQL_FEATURE_LOWER_FIELD_PREDICATE_UNSUPPORTED = 67 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::LowerFieldPredicateUnsupported });
    SQL_FEATURE_MULTI_STATEMENT_SQL = 68 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::MultiStatementSql });
    SQL_FEATURE_NESTED_AGGREGATE_INPUT = 69 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NestedAggregateInput });
    SQL_FEATURE_NESTED_PROJECTION_FUNCTION_IN_ARITHMETIC = 70 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NestedProjectionFunctionInArithmetic });
    SQL_FEATURE_ORDER_BY_UNSUPPORTED_FORM = 71 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::OrderByUnsupportedForm });
    SQL_FEATURE_OTHER = 72 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Other });
    SQL_FEATURE_PREDICATE_STARTS_WITH_FIRST_ARGUMENT = 73 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::PredicateStartsWithFirstArgument });
    SQL_FEATURE_QUOTED_IDENTIFIERS = 74 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::QuotedIdentifiers });
    SQL_FEATURE_RETURNING_UNSUPPORTED_SHAPE = 75 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ReturningUnsupportedShape });
    SQL_FEATURE_SCALAR_FUNCTION_EXPRESSION_POSITION = 76 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ScalarFunctionExpressionPosition });
    SQL_FEATURE_SCALE_TAKING_NUMERIC_FUNCTION_EXPRESSION_POSITION = 77 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ScaleTakingNumericFunctionExpressionPosition });
    SQL_FEATURE_SEARCHED_CASE_GROUPED_ORDER_BY = 78 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SearchedCaseGroupedOrderBy });
    SQL_FEATURE_SHOW_COLUMNS_MODIFIERS = 79 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowColumnsModifiers });
    SQL_FEATURE_SHOW_ENTITIES_MODIFIERS = 80 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowEntitiesModifiers });
    SQL_FEATURE_SHOW_INDEXES_MODIFIERS = 81 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowIndexesModifiers });
    SQL_FEATURE_SHOW_MEMORY_MODIFIERS = 82 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowMemoryModifiers });
    SQL_FEATURE_SHOW_STORES_MODIFIERS = 83 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowStoresModifiers });
    SQL_FEATURE_SHOW_UNSUPPORTED_COMMAND = 84 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowUnsupportedCommand });
    SQL_FEATURE_SIMPLE_CASE_EXPRESSION = 85 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SimpleCaseExpression });
    SQL_FEATURE_STANDALONE_LITERAL_PROJECTION_ITEM = 86 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::StandaloneLiteralProjectionItem });
    SQL_FEATURE_SUPPORTED_GROUPED_ORDER_BY_EXPRESSION_FAMILY = 87 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SupportedGroupedOrderByExpressionFamily });
    SQL_FEATURE_SUPPORTED_ORDER_BY_EXPRESSION_FAMILY = 88 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SupportedOrderByExpressionFamily });
    SQL_FEATURE_UNION_INTERSECT_EXCEPT = 89 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UnionIntersectExcept });
    SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE = 90 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UnsupportedFunctionNamespace });
    SQL_FEATURE_UPDATE = 91 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Update });
    SQL_FEATURE_UPPER_FIELD_PREDICATE_UNSUPPORTED = 92 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UpperFieldPredicateUnsupported });
    SQL_FEATURE_WINDOW_FUNCTION = 93 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::WindowFunction });
    SQL_FEATURE_WITH = 94 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::With });
    SQL_FEATURE_NUMERIC_SCALE_FUNCTION_ARGUMENTS = 95 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NumericScaleFunctionArguments });
    SQL_FEATURE_ORDER_BY_FIELD_NOT_ORDERABLE = 96 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::OrderByFieldNotOrderable });

    SQL_SURFACE_QUERY_REJECTS_INSERT = 97 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsInsert });
    SQL_SURFACE_QUERY_REJECTS_UPDATE = 98 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsUpdate });
    SQL_SURFACE_QUERY_REJECTS_DELETE = 99 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsDelete });
    SQL_SURFACE_MUTATION_REJECTS_SELECT = 100 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsSelect });
    SQL_SURFACE_MUTATION_REJECTS_EXPLAIN = 101 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsExplain });
    SQL_SURFACE_MUTATION_REJECTS_DESCRIBE = 102 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsDescribe });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_INDEXES = 103 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowIndexes });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_COLUMNS = 104 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowColumns });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_ENTITIES = 105 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowEntities });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_STORES = 106 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowStores });
    SQL_SURFACE_MUTATION_REJECTS_SHOW_MEMORY = 107 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRejectsShowMemory });

    SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION = 108 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::MissingExpectedSchemaVersion });
    SCHEMA_DDL_MISSING_NEXT_SCHEMA_VERSION = 109 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::MissingNextSchemaVersion });
    SCHEMA_DDL_STALE_EXPECTED_SCHEMA_VERSION = 110 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::StaleExpectedSchemaVersion });
    SCHEMA_DDL_INVALID_EXPECTED_SCHEMA_VERSION = 111 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion });
    SCHEMA_DDL_INVALID_NEXT_SCHEMA_VERSION = 112 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidNextSchemaVersion });
    SCHEMA_DDL_ACCEPTED_SCHEMA_CHANGE_WITHOUT_VERSION_BUMP = 113 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump });
    SCHEMA_DDL_EMPTY_VERSION_BUMP = 114 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::EmptyVersionBump });
    SCHEMA_DDL_VERSION_GAP = 115 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::VersionGap });
    SCHEMA_DDL_VERSION_ROLLBACK = 116 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::VersionRollback });
    SCHEMA_DDL_FINGERPRINT_METHOD_MISMATCH = 117 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::FingerprintMethodMismatch });
    SCHEMA_DDL_UNSUPPORTED_TRANSITION_CLASS = 118 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::UnsupportedTransitionClass });
    SCHEMA_DDL_PHYSICAL_RUNNER_MISSING = 119 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::PhysicalRunnerMissing });
    SCHEMA_DDL_VALIDATION_FAILED = 120 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::ValidationFailed });
    SCHEMA_DDL_PUBLICATION_RACE_LOST = 121 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::PublicationRaceLost });
    SCHEMA_DDL_INVALID_ADD_COLUMN_DEFAULT = 122 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidAddColumnDefault });
    SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT = 123 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidAlterColumnDefault });
    SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED = 124 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedIndexDropRejected });
    SCHEMA_DDL_REWRITE_REQUIRES_MIGRATION = 125 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::SchemaRewriteRequiresMigration });
    SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED = 126 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected });
    SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED = 127 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected });
    SCHEMA_DDL_SET_NOT_NULL_VALIDATION_FAILED = 128 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::SetNotNullValidationFailed });

    QUERY_SQL_WRITE_BOUNDARY = 129 => QuerySqlWriteBoundary;
    SQL_WRITE_PRIMARY_KEY_LITERAL_SHAPE = 130 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::PrimaryKeyLiteralShape });
    SQL_WRITE_PRIMARY_KEY_LITERAL_INCOMPATIBLE = 131 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible });
    SQL_WRITE_MISSING_PRIMARY_KEY = 132 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::MissingPrimaryKey });
    SQL_WRITE_MISSING_REQUIRED_FIELDS = 133 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::MissingRequiredFields });
    SQL_WRITE_EXPLICIT_MANAGED_FIELD = 134 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExplicitManagedField });
    SQL_WRITE_EXPLICIT_GENERATED_FIELD = 135 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExplicitGeneratedField });
    SQL_WRITE_INSERT_SELECT_REQUIRES_SCALAR = 136 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectRequiresScalar });
    SQL_WRITE_INSERT_SELECT_AGGREGATE_PROJECTION = 137 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectAggregateProjection });
    SQL_WRITE_INSERT_SELECT_WIDTH_MISMATCH = 138 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectWidthMismatch });
    SQL_WRITE_UPDATE_PRIMARY_KEY_MUTATION = 139 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdatePrimaryKeyMutation });
    SQL_WRITE_INVALID_FIELD_LITERAL = 140 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InvalidFieldLiteral });
    SQL_WRITE_UNKNOWN_RETURNING_FIELD = 141 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UnknownReturningField });
    SQL_WRITE_DUPLICATE_RETURNING_FIELD = 142 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::DuplicateReturningField });
    SQL_WRITE_UPDATE_MISSING_WHERE_PREDICATE = 143 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdateMissingWherePredicate });
    SQL_WRITE_ORDER_BY_UNSUPPORTED_SHAPE = 144 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::WriteOrderByUnsupportedShape });

    QUERY_UNSUPPORTED_PROJECTION = 145 => QueryUnsupportedProjection;
    QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED = 146 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericLiteralRequired });
    QUERY_PROJECTION_NUMERIC_SCALE_ARGUMENTS = 147 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericScaleArguments });
    QUERY_PROJECTION_NESTED_FIELD_PATH_PREVIEW = 148 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NestedFieldPathPreview });
    QUERY_PROJECTION_CASE_CONDITION_BOOLEAN_REQUIRED = 149 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::CaseConditionBooleanRequired });
    QUERY_PROJECTION_NUMERIC_INPUT_REQUIRED = 150 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericInputRequired });
    QUERY_PROJECTION_TEXT_OR_BLOB_INPUT_REQUIRED = 151 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextOrBlobInputRequired });
    QUERY_PROJECTION_TEXT_INPUT_REQUIRED = 152 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextInputRequired });
    QUERY_PROJECTION_TEXT_OR_NULL_ARGUMENT_REQUIRED = 153 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextOrNullArgumentRequired });
    QUERY_PROJECTION_INTEGER_OR_NULL_ARGUMENT_REQUIRED = 154 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::IntegerOrNullArgumentRequired });
    QUERY_PROJECTION_UNARY_OPERAND_INCOMPATIBLE = 155 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::UnaryOperandIncompatible });
    QUERY_PROJECTION_BINARY_OPERANDS_INCOMPATIBLE = 156 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::BinaryOperandsIncompatible });

    QUERY_RESULT_SHAPE_MISMATCH = 157 => QueryResultShapeMismatch;
    QUERY_RESULT_EXPECTED_ROWS = 158 => QueryResultShapeMismatch,
        detail(QueryResultShape { reason: QueryResultShapeCode::ExpectedRows });
    QUERY_RESULT_EXPECTED_GROUPED = 159 => QueryResultShapeMismatch,
        detail(QueryResultShape { reason: QueryResultShapeCode::ExpectedGroupedRows });

    SQL_LOWERING_ENTITY_MISMATCH = 160 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::EntityMismatch });
    SQL_LOWERING_SELECT_PROJECTION_SHAPE = 161 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectProjectionShape });
    SQL_LOWERING_SELECT_DISTINCT = 162 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectDistinct });
    SQL_LOWERING_DISTINCT_ORDER_BY_PROJECTION = 163 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::DistinctOrderByProjection });
    SQL_LOWERING_GLOBAL_AGGREGATE_PROJECTION = 164 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GlobalAggregateProjection });
    SQL_LOWERING_GLOBAL_AGGREGATE_GROUP_BY = 165 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GlobalAggregateGroupBy });
    SQL_LOWERING_SELECT_GROUP_BY_SHAPE = 166 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectGroupByShape });
    SQL_LOWERING_GROUPED_PROJECTION_EXPLICIT_LIST_REQUIRED = 167 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionExplicitListRequired });
    SQL_LOWERING_GROUPED_PROJECTION_AGGREGATE_REQUIRED = 168 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionAggregateRequired });
    SQL_LOWERING_GROUPED_PROJECTION_NON_GROUP_FIELD = 169 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionNonGroupField });
    SQL_LOWERING_GROUPED_PROJECTION_SCALAR_AFTER_AGGREGATE = 170 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionScalarAfterAggregate });
    SQL_LOWERING_HAVING_REQUIRES_GROUP_BY = 171 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::HavingRequiresGroupBy });
    SQL_LOWERING_SELECT_HAVING_SHAPE = 172 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectHavingShape });
    SQL_LOWERING_AGGREGATE_INPUT_EXPRESSIONS = 173 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::AggregateInputExpressions });
    SQL_LOWERING_WHERE_EXPRESSION_SHAPE = 174 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::WhereExpressionShape });
    SQL_LOWERING_PARAMETER_PLACEMENT = 175 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::ParameterPlacement });
    SQL_LOWERING_SQL_DDL_EXECUTION_UNSUPPORTED = 176 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SqlDdlExecutionUnsupported });

    SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE = 177 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ReturningResponseTooLarge });
    SQL_WRITE_RETURNING_ROWS_TOO_MANY = 178 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ReturningRowsTooMany });
    RUNTIME_BOUNDARY_SQL_INTROSPECTION_DISABLED = 179 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlIntrospectionDisabled });
    SQL_WRITE_STAGED_ROWS_TOO_MANY = 180 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::StagedRowsTooMany });

    QUERY_READ_ADMISSION = 181 => QueryReadAdmission;
    QUERY_READ_PUBLIC_REQUIRES_LIMIT = 182 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PublicQueryRequiresLimit });
    QUERY_READ_PUBLIC_REQUIRES_INDEX = 183 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PublicQueryRequiresIndex });
    QUERY_READ_UNBOUNDED_FULL_SCAN_REJECTED = 184 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::UnboundedFullScanRejected });
    QUERY_READ_SORT_REQUIRES_MATERIALIZATION = 185 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::SortRequiresMaterialization });
    QUERY_READ_GROUPED_QUERY_REQUIRES_LIMITS = 186 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::GroupedQueryRequiresLimits });
    QUERY_READ_GROUPED_QUERY_EXCEEDS_BUDGET = 187 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::GroupedQueryExceedsBudget });
    QUERY_READ_DIAGNOSTIC_LANE_DOES_NOT_EXECUTE = 188 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute });
    QUERY_READ_RETURNED_ROW_BOUND_EXCEEDS_POLICY = 189 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy });
    QUERY_READ_PRIMARY_KEY_INPUT_EXCEEDS_POLICY = 190 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy });

    SCHEMA_DDL_ROW_LAYOUT_VERSION_EXHAUSTED = 191 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::RowLayoutVersionExhausted });
    SCHEMA_DDL_TRANSITION_BUDGET_EXCEEDED = 192 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::SchemaTransitionBudgetExceeded });
    SQL_WRITE_INSERT_DEFAULT_REQUIRED_FIELD = 193 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertDefaultRequiredField });
    SQL_WRITE_UPDATE_DEFAULT_REQUIRED_FIELD = 194 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdateDefaultRequiredField });
    SQL_WRITE_UPDATE_DEFAULT_DATABASE_OWNED_FIELD = 195 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdateDefaultDatabaseOwnedField });
    RUNTIME_BOUNDARY_MUTATION_REQUIRED_FIELD_MISSING = 196 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationRequiredFieldMissing });
    RUNTIME_BOUNDARY_PERSISTED_ROW_LAYOUT_OUTSIDE_ACCEPTED_WINDOW = 197 => RuntimeCorruption,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::PersistedRowLayoutOutsideAcceptedWindow });
    RUNTIME_BOUNDARY_PERSISTED_ROW_SLOT_COUNT_MISMATCH = 198 => RuntimeCorruption,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::PersistedRowSlotCountMismatch });
    RUNTIME_BOUNDARY_GENERATED_FIELD_AFTER_DDL_FIELD = 199 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::GeneratedFieldAfterDdlField });
    SQL_SURFACE_MUTATION_REQUIRES_EXPLICIT_UPDATE_INTENT = 200 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::MutationRequiresExplicitUpdateIntent });
    SQL_WRITE_EXACT_UPDATE_ASSERTION_REQUIRED = 201 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateAssertionRequired });
    SQL_WRITE_EXACT_UPDATE_ASSERTION_TOO_HIGH = 202 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateAssertionTooHigh });
    SQL_WRITE_EXACT_UPDATE_AFFECTED_ROWS_EXCEEDED = 203 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateAffectedRowsExceeded });
    SQL_WRITE_EXACT_UPDATE_WINDOW_UNSUPPORTED = 204 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateWindowUnsupported });
    SQL_WRITE_EXACT_UPDATE_SCAN_BUDGET_EXCEEDED = 205 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExactUpdateScanBudgetExceeded });
    SQL_WRITE_RESUMABLE_UPDATE_WINDOW_UNSUPPORTED = 206 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateWindowUnsupported });
    SQL_WRITE_RESUMABLE_UPDATE_RETURNING_UNSUPPORTED = 207 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateReturningUnsupported });
    SQL_WRITE_RESUMABLE_UPDATE_REQUIRES_JOURNALED_STORE = 208 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore });
    SQL_WRITE_RESUMABLE_UPDATE_ASSIGNED_FIELD_HAS_GLOBAL_CONSTRAINT = 209 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateAssignedFieldHasGlobalConstraint });
    SQL_WRITE_RESUMABLE_UPDATE_SCOPE_DEPENDS_ON_ASSIGNED_FIELD = 210 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateScopeDependsOnAssignedField });
    SQL_WRITE_RESUMABLE_UPDATE_SCOPE_DEPENDENCY_UNKNOWN = 211 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateScopeDependencyUnknown });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_MALFORMED = 212 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationMalformed });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_TARGET_MISMATCH = 213 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationTargetMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_SCHEMA_MISMATCH = 214 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationSchemaMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_SCOPE_MISMATCH = 215 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationScopeMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_PATCH_MISMATCH = 216 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationPatchMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_BATCH_POLICY_MISMATCH = 217 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationBatchPolicyMismatch });
    SQL_WRITE_RESUMABLE_UPDATE_SINGLE_ROW_RESOURCE_EXCEEDED = 218 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateSingleRowResourceExceeded });
    SQL_WRITE_RESUMABLE_UPDATE_APPLICATION_CALLBACKS_UNSUPPORTED = 219 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateApplicationCallbacksUnsupported });
    SQL_WRITE_RESUMABLE_UPDATE_MANAGED_FIELD_HAS_GLOBAL_CONSTRAINT = 220 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateManagedFieldHasGlobalConstraint });
    SQL_WRITE_RESUMABLE_UPDATE_CONTINUATION_OPERATION_MISMATCH = 221 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ResumableUpdateContinuationOperationMismatch });
    RUNTIME_BOUNDARY_JOURNAL_MUTATION_REVISION_EXHAUSTED = 222 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::JournalMutationRevisionExhausted });
}
