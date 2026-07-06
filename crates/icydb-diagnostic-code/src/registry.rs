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
    RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITY_REQUIRED = 33 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationResultEntityRequired });
    RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITIES_REQUIRED = 34 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationResultEntitiesRequired });
    RUNTIME_BOUNDARY_MUTATION_RESULT_ID_REQUIRED = 35 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationResultIdRequired });
    RUNTIME_BOUNDARY_MUTATION_RESULT_IDS_REQUIRED = 36 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::MutationResultIdsRequired });
    RUNTIME_BOUNDARY_ROW_PROJECTION_FIELD_NOT_CONFIGURED = 37 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::RowProjectionFieldNotConfigured });

    SQL_FEATURE_AGGREGATE_FILTER_CLAUSE = 38 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AggregateFilterClause });
    SQL_FEATURE_ALTER_STATEMENT_BEYOND_ALTER_TABLE = 39 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterStatementBeyondAlterTable });
    SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_DUPLICATE_DEFAULT = 40 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddColumnDuplicateDefault });
    SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_MODIFIERS = 41 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_ADD_STATEMENT_BEYOND_ADD_COLUMN = 42 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAddStatementBeyondAddColumn });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_DROP_UNSUPPORTED_ACTION = 43 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_MODIFIERS = 44 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_SET_UNSUPPORTED_ACTION = 45 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_UNSUPPORTED_ACTION = 46 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterColumnUnsupportedAction });
    SQL_FEATURE_ALTER_TABLE_ALTER_STATEMENT_BEYOND_ALTER_COLUMN = 47 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn });
    SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_IF_EXISTS_SYNTAX = 48 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropColumnIfExistsSyntax });
    SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_MODIFIERS = 49 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_DROP_STATEMENT_BEYOND_DROP_COLUMN = 50 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableDropStatementBeyondDropColumn });
    SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MISSING_TO = 51 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameColumnMissingTo });
    SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MODIFIERS = 52 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameColumnModifiers });
    SQL_FEATURE_ALTER_TABLE_RENAME_STATEMENT_BEYOND_RENAME_COLUMN = 53 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn });
    SQL_FEATURE_ALTER_TABLE_UNSUPPORTED_OPERATION = 54 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::AlterTableUnsupportedOperation });
    SQL_FEATURE_COLUMN_ALIAS = 55 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ColumnAlias });
    SQL_FEATURE_CREATE_INDEX_IF_NOT_EXISTS_SYNTAX = 56 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexIfNotExistsSyntax });
    SQL_FEATURE_CREATE_INDEX_KEY_ORDERING_MODIFIERS = 57 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexKeyOrderingModifiers });
    SQL_FEATURE_CREATE_INDEX_MODIFIERS = 58 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateIndexModifiers });
    SQL_FEATURE_CREATE_STATEMENT_BEYOND_CREATE_INDEX = 59 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::CreateStatementBeyondCreateIndex });
    SQL_FEATURE_DESCRIBE_MODIFIER = 60 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DescribeModifier });
    SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_EXPECTED_CLAUSE = 61 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause });
    SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_SET_CLAUSE = 62 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DdlSchemaVersionDuplicateSetClause });
    SQL_FEATURE_DROP_INDEX_MODIFIERS = 63 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropIndexModifiers });
    SQL_FEATURE_DROP_INDEX_IF_EXISTS_SYNTAX = 64 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropIndexIfExistsSyntax });
    SQL_FEATURE_DROP_STATEMENT_BEYOND_DROP_INDEX = 65 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::DropStatementBeyondDropIndex });
    SQL_FEATURE_EXPRESSION_INDEX_UNSUPPORTED_FUNCTION = 66 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ExpressionIndexUnsupportedFunction });
    SQL_FEATURE_HAVING = 67 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Having });
    SQL_FEATURE_INSERT = 68 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Insert });
    SQL_FEATURE_JOIN = 69 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Join });
    SQL_FEATURE_LIKE_PATTERN_BEYOND_TRAILING_PREFIX = 70 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::LikePatternBeyondTrailingPrefix });
    SQL_FEATURE_LOWER_FIELD_PREDICATE_UNSUPPORTED = 71 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::LowerFieldPredicateUnsupported });
    SQL_FEATURE_MULTI_STATEMENT_SQL = 72 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::MultiStatementSql });
    SQL_FEATURE_NESTED_AGGREGATE_INPUT = 73 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NestedAggregateInput });
    SQL_FEATURE_NESTED_PROJECTION_FUNCTION_IN_ARITHMETIC = 74 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NestedProjectionFunctionInArithmetic });
    SQL_FEATURE_ORDER_BY_UNSUPPORTED_FORM = 75 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::OrderByUnsupportedForm });
    SQL_FEATURE_OTHER = 76 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Other });
    SQL_FEATURE_PARAMETER_BINDING = 77 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ParameterBinding });
    SQL_FEATURE_PARAMETERIZED_SCHEMA_VERSION = 78 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ParameterizedSchemaVersion });
    SQL_FEATURE_PREDICATE_STARTS_WITH_FIRST_ARGUMENT = 79 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::PredicateStartsWithFirstArgument });
    SQL_FEATURE_QUOTED_IDENTIFIERS = 80 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::QuotedIdentifiers });
    SQL_FEATURE_RETURNING_UNSUPPORTED_SHAPE = 81 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ReturningUnsupportedShape });
    SQL_FEATURE_SCALAR_FUNCTION_EXPRESSION_POSITION = 82 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ScalarFunctionExpressionPosition });
    SQL_FEATURE_SCALE_TAKING_NUMERIC_FUNCTION_EXPRESSION_POSITION = 83 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ScaleTakingNumericFunctionExpressionPosition });
    SQL_FEATURE_SEARCHED_CASE_GROUPED_ORDER_BY = 84 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SearchedCaseGroupedOrderBy });
    SQL_FEATURE_SHOW_COLUMNS_MODIFIERS = 85 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowColumnsModifiers });
    SQL_FEATURE_SHOW_ENTITIES_MODIFIERS = 86 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowEntitiesModifiers });
    SQL_FEATURE_SHOW_INDEXES_MODIFIERS = 87 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowIndexesModifiers });
    SQL_FEATURE_SHOW_MEMORY_MODIFIERS = 88 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowMemoryModifiers });
    SQL_FEATURE_SHOW_STORES_MODIFIERS = 89 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowStoresModifiers });
    SQL_FEATURE_SHOW_UNSUPPORTED_COMMAND = 90 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::ShowUnsupportedCommand });
    SQL_FEATURE_SIMPLE_CASE_EXPRESSION = 91 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SimpleCaseExpression });
    SQL_FEATURE_STANDALONE_LITERAL_PROJECTION_ITEM = 92 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::StandaloneLiteralProjectionItem });
    SQL_FEATURE_SUPPORTED_GROUPED_ORDER_BY_EXPRESSION_FAMILY = 93 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SupportedGroupedOrderByExpressionFamily });
    SQL_FEATURE_SUPPORTED_ORDER_BY_EXPRESSION_FAMILY = 94 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::SupportedOrderByExpressionFamily });
    SQL_FEATURE_UNION_INTERSECT_EXCEPT = 95 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UnionIntersectExcept });
    SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE = 96 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UnsupportedFunctionNamespace });
    SQL_FEATURE_UPDATE = 97 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::Update });
    SQL_FEATURE_UPPER_FIELD_PREDICATE_UNSUPPORTED = 98 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::UpperFieldPredicateUnsupported });
    SQL_FEATURE_WINDOW_FUNCTION = 99 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::WindowFunction });
    SQL_FEATURE_WITH = 100 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::With });
    SQL_FEATURE_NUMERIC_SCALE_FUNCTION_ARGUMENTS = 101 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::NumericScaleFunctionArguments });
    SQL_FEATURE_ORDER_BY_FIELD_NOT_ORDERABLE = 102 => QueryUnsupportedSqlFeature,
        detail(UnsupportedSqlFeature { feature: SqlFeatureCode::OrderByFieldNotOrderable });

    SQL_SURFACE_QUERY_REJECTS_INSERT = 103 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsInsert });
    SQL_SURFACE_QUERY_REJECTS_UPDATE = 104 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsUpdate });
    SQL_SURFACE_QUERY_REJECTS_DELETE = 105 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::QueryRejectsDelete });
    SQL_SURFACE_UPDATE_REJECTS_SELECT = 106 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsSelect });
    SQL_SURFACE_UPDATE_REJECTS_EXPLAIN = 107 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsExplain });
    SQL_SURFACE_UPDATE_REJECTS_DESCRIBE = 108 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsDescribe });
    SQL_SURFACE_UPDATE_REJECTS_SHOW_INDEXES = 109 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowIndexes });
    SQL_SURFACE_UPDATE_REJECTS_SHOW_COLUMNS = 110 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowColumns });
    SQL_SURFACE_UPDATE_REJECTS_SHOW_ENTITIES = 111 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowEntities });
    SQL_SURFACE_UPDATE_REJECTS_SHOW_STORES = 112 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowStores });
    SQL_SURFACE_UPDATE_REJECTS_SHOW_MEMORY = 113 => QuerySqlSurfaceMismatch,
        detail(SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowMemory });

    SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION = 114 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::MissingExpectedSchemaVersion });
    SCHEMA_DDL_MISSING_NEXT_SCHEMA_VERSION = 115 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::MissingNextSchemaVersion });
    SCHEMA_DDL_STALE_EXPECTED_SCHEMA_VERSION = 116 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::StaleExpectedSchemaVersion });
    SCHEMA_DDL_INVALID_EXPECTED_SCHEMA_VERSION = 117 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion });
    SCHEMA_DDL_INVALID_NEXT_SCHEMA_VERSION = 118 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidNextSchemaVersion });
    SCHEMA_DDL_ACCEPTED_SCHEMA_CHANGE_WITHOUT_VERSION_BUMP = 119 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump });
    SCHEMA_DDL_EMPTY_VERSION_BUMP = 120 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::EmptyVersionBump });
    SCHEMA_DDL_VERSION_GAP = 121 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::VersionGap });
    SCHEMA_DDL_VERSION_ROLLBACK = 122 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::VersionRollback });
    SCHEMA_DDL_FINGERPRINT_METHOD_MISMATCH = 123 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::FingerprintMethodMismatch });
    SCHEMA_DDL_UNSUPPORTED_TRANSITION_CLASS = 124 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::UnsupportedTransitionClass });
    SCHEMA_DDL_PHYSICAL_RUNNER_MISSING = 125 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::PhysicalRunnerMissing });
    SCHEMA_DDL_VALIDATION_FAILED = 126 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::ValidationFailed });
    SCHEMA_DDL_PUBLICATION_RACE_LOST = 127 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::PublicationRaceLost });
    SCHEMA_DDL_INVALID_ADD_COLUMN_DEFAULT = 128 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidAddColumnDefault });
    SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT = 129 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::InvalidAlterColumnDefault });
    SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED = 130 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedIndexDropRejected });
    SCHEMA_DDL_REQUIRED_DROP_DEFAULT_UNSUPPORTED = 131 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::RequiredDropDefaultUnsupported });
    SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED = 132 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected });
    SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED = 133 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected });
    SCHEMA_DDL_SET_NOT_NULL_VALIDATION_FAILED = 134 => SchemaDdlAdmission,
        detail(SchemaDdlAdmission { reason: SchemaDdlAdmissionCode::SetNotNullValidationFailed });

    QUERY_SQL_WRITE_BOUNDARY = 135 => QuerySqlWriteBoundary;
    SQL_WRITE_PRIMARY_KEY_LITERAL_SHAPE = 136 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::PrimaryKeyLiteralShape });
    SQL_WRITE_PRIMARY_KEY_LITERAL_INCOMPATIBLE = 137 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible });
    SQL_WRITE_MISSING_PRIMARY_KEY = 138 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::MissingPrimaryKey });
    SQL_WRITE_MISSING_REQUIRED_FIELDS = 139 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::MissingRequiredFields });
    SQL_WRITE_EXPLICIT_MANAGED_FIELD = 140 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExplicitManagedField });
    SQL_WRITE_EXPLICIT_GENERATED_FIELD = 141 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ExplicitGeneratedField });
    SQL_WRITE_INSERT_SELECT_REQUIRES_SCALAR = 142 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectRequiresScalar });
    SQL_WRITE_INSERT_SELECT_AGGREGATE_PROJECTION = 143 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectAggregateProjection });
    SQL_WRITE_INSERT_SELECT_WIDTH_MISMATCH = 144 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InsertSelectWidthMismatch });
    SQL_WRITE_UPDATE_PRIMARY_KEY_MUTATION = 145 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdatePrimaryKeyMutation });
    SQL_WRITE_INVALID_FIELD_LITERAL = 146 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::InvalidFieldLiteral });
    SQL_WRITE_UNKNOWN_RETURNING_FIELD = 147 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UnknownReturningField });
    SQL_WRITE_DUPLICATE_RETURNING_FIELD = 148 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::DuplicateReturningField });
    SQL_WRITE_UPDATE_MISSING_WHERE_PREDICATE = 149 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::UpdateMissingWherePredicate });
    SQL_WRITE_ORDER_BY_UNSUPPORTED_SHAPE = 150 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::WriteOrderByUnsupportedShape });

    QUERY_UNSUPPORTED_PROJECTION = 151 => QueryUnsupportedProjection;
    QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED = 152 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericLiteralRequired });
    QUERY_PROJECTION_NUMERIC_SCALE_ARGUMENTS = 153 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericScaleArguments });
    QUERY_PROJECTION_NESTED_FIELD_PATH_PREVIEW = 154 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NestedFieldPathPreview });
    QUERY_PROJECTION_CASE_CONDITION_BOOLEAN_REQUIRED = 155 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::CaseConditionBooleanRequired });
    QUERY_PROJECTION_NUMERIC_INPUT_REQUIRED = 156 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::NumericInputRequired });
    QUERY_PROJECTION_TEXT_OR_BLOB_INPUT_REQUIRED = 157 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextOrBlobInputRequired });
    QUERY_PROJECTION_TEXT_INPUT_REQUIRED = 158 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextInputRequired });
    QUERY_PROJECTION_TEXT_OR_NULL_ARGUMENT_REQUIRED = 159 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::TextOrNullArgumentRequired });
    QUERY_PROJECTION_INTEGER_OR_NULL_ARGUMENT_REQUIRED = 160 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::IntegerOrNullArgumentRequired });
    QUERY_PROJECTION_UNARY_OPERAND_INCOMPATIBLE = 161 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::UnaryOperandIncompatible });
    QUERY_PROJECTION_BINARY_OPERANDS_INCOMPATIBLE = 162 => QueryUnsupportedProjection,
        detail(QueryProjection { reason: QueryProjectionCode::BinaryOperandsIncompatible });

    QUERY_RESULT_SHAPE_MISMATCH = 163 => QueryResultShapeMismatch;
    QUERY_RESULT_EXPECTED_ROWS = 164 => QueryResultShapeMismatch,
        detail(QueryResultShape { reason: QueryResultShapeCode::ExpectedRows });
    QUERY_RESULT_EXPECTED_GROUPED = 165 => QueryResultShapeMismatch,
        detail(QueryResultShape { reason: QueryResultShapeCode::ExpectedGroupedRows });

    SQL_LOWERING_ENTITY_MISMATCH = 166 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::EntityMismatch });
    SQL_LOWERING_SELECT_PROJECTION_SHAPE = 167 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectProjectionShape });
    SQL_LOWERING_SELECT_DISTINCT = 168 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectDistinct });
    SQL_LOWERING_DISTINCT_ORDER_BY_PROJECTION = 169 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::DistinctOrderByProjection });
    SQL_LOWERING_GLOBAL_AGGREGATE_PROJECTION = 170 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GlobalAggregateProjection });
    SQL_LOWERING_GLOBAL_AGGREGATE_GROUP_BY = 171 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GlobalAggregateGroupBy });
    SQL_LOWERING_SELECT_GROUP_BY_SHAPE = 172 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectGroupByShape });
    SQL_LOWERING_GROUPED_PROJECTION_EXPLICIT_LIST_REQUIRED = 173 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionExplicitListRequired });
    SQL_LOWERING_GROUPED_PROJECTION_AGGREGATE_REQUIRED = 174 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionAggregateRequired });
    SQL_LOWERING_GROUPED_PROJECTION_NON_GROUP_FIELD = 175 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionNonGroupField });
    SQL_LOWERING_GROUPED_PROJECTION_SCALAR_AFTER_AGGREGATE = 176 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::GroupedProjectionScalarAfterAggregate });
    SQL_LOWERING_HAVING_REQUIRES_GROUP_BY = 177 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::HavingRequiresGroupBy });
    SQL_LOWERING_SELECT_HAVING_SHAPE = 178 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SelectHavingShape });
    SQL_LOWERING_AGGREGATE_INPUT_EXPRESSIONS = 179 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::AggregateInputExpressions });
    SQL_LOWERING_WHERE_EXPRESSION_SHAPE = 180 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::WhereExpressionShape });
    SQL_LOWERING_PARAMETER_PLACEMENT = 181 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::ParameterPlacement });
    SQL_LOWERING_SQL_DDL_EXECUTION_UNSUPPORTED = 182 => QueryUnsupportedSqlFeature,
        detail(SqlLowering { reason: SqlLoweringCode::SqlDdlExecutionUnsupported });

    SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE = 183 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ReturningResponseTooLarge });
    SQL_WRITE_RETURNING_ROWS_TOO_MANY = 184 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::ReturningRowsTooMany });
    RUNTIME_BOUNDARY_SQL_INTROSPECTION_DISABLED = 185 => RuntimeUnsupported,
        detail(RuntimeBoundary { boundary: RuntimeBoundaryCode::SqlIntrospectionDisabled });
    SQL_WRITE_STAGED_ROWS_TOO_MANY = 186 => QuerySqlWriteBoundary,
        detail(SqlWriteBoundary { boundary: SqlWriteBoundaryCode::StagedRowsTooMany });

    QUERY_READ_ADMISSION = 187 => QueryReadAdmission;
    QUERY_READ_PUBLIC_REQUIRES_LIMIT = 188 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PublicQueryRequiresLimit });
    QUERY_READ_PUBLIC_REQUIRES_INDEX = 189 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PublicQueryRequiresIndex });
    QUERY_READ_UNBOUNDED_FULL_SCAN_REJECTED = 190 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::UnboundedFullScanRejected });
    QUERY_READ_SCAN_BOUND_UNAVAILABLE = 191 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::ScanBoundUnavailable });
    QUERY_READ_SCAN_BOUND_EXCEEDS_POLICY = 192 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::ScanBoundExceedsPolicy });
    QUERY_READ_ESTIMATED_ONLY_BOUND_REJECTED = 193 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::EstimatedOnlyBoundRejected });
    QUERY_READ_SORT_REQUIRES_MATERIALIZATION = 194 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::SortRequiresMaterialization });
    QUERY_READ_MATERIALIZATION_EXCEEDS_BUDGET = 195 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::MaterializationExceedsBudget });
    QUERY_READ_PROJECTION_RESPONSE_MAY_EXCEED_LIMIT = 196 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::ProjectionResponseMayExceedLimit });
    QUERY_READ_GROUPED_QUERY_REQUIRES_LIMITS = 197 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::GroupedQueryRequiresLimits });
    QUERY_READ_GROUPED_QUERY_EXCEEDS_BUDGET = 198 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::GroupedQueryExceedsBudget });
    QUERY_READ_DIAGNOSTIC_LANE_DOES_NOT_EXECUTE = 199 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute });
    QUERY_READ_INTROSPECTION_DISABLED_FOR_LANE = 200 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::IntrospectionDisabledForLane });
    QUERY_READ_UNSUPPORTED_STATEMENT_FOR_QUERY_LANE = 201 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::UnsupportedStatementForQueryLane });
    QUERY_READ_PUBLIC_OFFSET_REJECTED = 202 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PublicQueryOffsetRejected });
    QUERY_READ_RETURNED_ROW_BOUND_EXCEEDS_POLICY = 203 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy });
    QUERY_READ_PRIMARY_KEY_INPUT_EXCEEDS_POLICY = 204 => QueryReadAdmission,
        detail(QueryReadAdmission { reason: QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy });
}
