//! Compact diagnostic identity for IcyDB.
//!
//! This crate intentionally contains no rich diagnostic prose or Candid wire
//! types. Production canister builds collapse diagnostics to numeric wire
//! codes before they cross the public canister boundary.

///
/// DiagnosticCode
///
/// Stable machine-readable diagnostic reason.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DiagnosticCode {
    QueryValidate,
    QueryIntent,
    QueryPlan,
    QueryAccessRequirement,
    QueryUnorderedPagination,
    QueryInvalidContinuationCursor,
    QueryNotFound,
    QueryNotUnique,
    QueryNumericOverflow,
    QueryNumericNotRepresentable,
    QueryUnsupportedSqlFeature,
    QuerySqlSurfaceMismatch,
    SchemaDdlAdmission,
    StoreNotFound,
    StoreCorruption,
    StoreInvariantViolation,
    RuntimeCorruption,
    RuntimeIncompatiblePersistedFormat,
    RuntimeInvariantViolation,
    RuntimeConflict,
    RuntimeNotFound,
    RuntimeUnsupported,
    RuntimeInternal,
}

impl DiagnosticCode {
    /// Return the broad diagnostic class for this code.
    #[must_use]
    pub const fn class(self) -> ErrorClass {
        match self {
            Self::StoreCorruption | Self::RuntimeCorruption => ErrorClass::Corruption,
            Self::RuntimeIncompatiblePersistedFormat => ErrorClass::IncompatiblePersistedFormat,
            Self::QueryNotFound | Self::StoreNotFound | Self::RuntimeNotFound => {
                ErrorClass::NotFound
            }
            Self::RuntimeConflict => ErrorClass::Conflict,
            Self::QueryUnsupportedSqlFeature
            | Self::QuerySqlSurfaceMismatch
            | Self::RuntimeUnsupported => ErrorClass::Unsupported,
            Self::StoreInvariantViolation | Self::RuntimeInvariantViolation => {
                ErrorClass::InvariantViolation
            }
            Self::RuntimeInternal => ErrorClass::Internal,
            Self::QueryValidate
            | Self::QueryIntent
            | Self::QueryPlan
            | Self::QueryAccessRequirement
            | Self::QueryUnorderedPagination
            | Self::QueryInvalidContinuationCursor
            | Self::QueryNotUnique
            | Self::QueryNumericOverflow
            | Self::QueryNumericNotRepresentable
            | Self::SchemaDdlAdmission => ErrorClass::Query,
        }
    }

    /// Return the default diagnostic origin for this code.
    #[must_use]
    pub const fn origin(self) -> ErrorOrigin {
        match self {
            Self::StoreNotFound | Self::StoreCorruption | Self::StoreInvariantViolation => {
                ErrorOrigin::Store
            }
            Self::RuntimeCorruption
            | Self::RuntimeIncompatiblePersistedFormat
            | Self::RuntimeInvariantViolation
            | Self::RuntimeConflict
            | Self::RuntimeNotFound
            | Self::RuntimeUnsupported
            | Self::RuntimeInternal => ErrorOrigin::Runtime,
            Self::QueryValidate
            | Self::QueryIntent
            | Self::QueryPlan
            | Self::QueryAccessRequirement
            | Self::QueryUnorderedPagination
            | Self::QueryInvalidContinuationCursor
            | Self::QueryNotFound
            | Self::QueryNotUnique
            | Self::QueryNumericOverflow
            | Self::QueryNumericNotRepresentable
            | Self::QueryUnsupportedSqlFeature
            | Self::QuerySqlSurfaceMismatch
            | Self::SchemaDdlAdmission => ErrorOrigin::Query,
        }
    }

    /// Return the compact public wire code for this broad diagnostic reason.
    #[must_use]
    pub const fn error_code(self) -> ErrorCode {
        match self {
            Self::QueryValidate => ErrorCode::QUERY_VALIDATE,
            Self::QueryIntent => ErrorCode::QUERY_INTENT,
            Self::QueryPlan => ErrorCode::QUERY_PLAN,
            Self::QueryAccessRequirement => ErrorCode::QUERY_ACCESS_REQUIREMENT,
            Self::QueryUnorderedPagination => ErrorCode::QUERY_UNORDERED_PAGINATION,
            Self::QueryInvalidContinuationCursor => ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
            Self::QueryNotFound => ErrorCode::QUERY_NOT_FOUND,
            Self::QueryNotUnique => ErrorCode::QUERY_NOT_UNIQUE,
            Self::QueryNumericOverflow => ErrorCode::QUERY_NUMERIC_OVERFLOW,
            Self::QueryNumericNotRepresentable => ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE,
            Self::QueryUnsupportedSqlFeature => ErrorCode::QUERY_UNSUPPORTED_SQL_FEATURE,
            Self::QuerySqlSurfaceMismatch => ErrorCode::QUERY_SQL_SURFACE_MISMATCH,
            Self::SchemaDdlAdmission => ErrorCode::SCHEMA_DDL_ADMISSION,
            Self::StoreNotFound => ErrorCode::STORE_NOT_FOUND,
            Self::StoreCorruption => ErrorCode::STORE_CORRUPTION,
            Self::StoreInvariantViolation => ErrorCode::STORE_INVARIANT_VIOLATION,
            Self::RuntimeCorruption => ErrorCode::RUNTIME_CORRUPTION,
            Self::RuntimeIncompatiblePersistedFormat => {
                ErrorCode::RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT
            }
            Self::RuntimeInvariantViolation => ErrorCode::RUNTIME_INVARIANT_VIOLATION,
            Self::RuntimeConflict => ErrorCode::RUNTIME_CONFLICT,
            Self::RuntimeNotFound => ErrorCode::RUNTIME_NOT_FOUND,
            Self::RuntimeUnsupported => ErrorCode::RUNTIME_UNSUPPORTED,
            Self::RuntimeInternal => ErrorCode::RUNTIME_INTERNAL,
        }
    }
}

///
/// ErrorCode
///
/// Stable numeric public error identity.
///
/// The public Candid `icydb::Error` stores this value as `nat16` so canister
/// interfaces do not retain rich diagnostic enum labels. Rich diagnostics can
/// still be reconstructed by host-side tooling from this leaf code. Before
/// 1.0.0, the code space is hard-cut to a single compact sequential range.
///

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ErrorCode(u16);

impl ErrorCode {
    pub const QUERY_VALIDATE: Self = Self(1);
    pub const QUERY_INTENT: Self = Self(2);
    pub const QUERY_PLAN: Self = Self(3);
    pub const QUERY_ACCESS_REQUIREMENT: Self = Self(4);
    pub const QUERY_UNORDERED_PAGINATION: Self = Self(5);
    pub const QUERY_INVALID_CONTINUATION_CURSOR: Self = Self(6);
    pub const QUERY_NOT_FOUND: Self = Self(7);
    pub const QUERY_NOT_UNIQUE: Self = Self(8);
    pub const QUERY_NUMERIC_OVERFLOW: Self = Self(9);
    pub const QUERY_NUMERIC_NOT_REPRESENTABLE: Self = Self(10);
    pub const QUERY_UNSUPPORTED_SQL_FEATURE: Self = Self(11);
    pub const QUERY_SQL_SURFACE_MISMATCH: Self = Self(12);
    pub const SCHEMA_DDL_ADMISSION: Self = Self(13);
    pub const STORE_NOT_FOUND: Self = Self(14);
    pub const STORE_CORRUPTION: Self = Self(15);
    pub const STORE_INVARIANT_VIOLATION: Self = Self(16);
    pub const RUNTIME_CORRUPTION: Self = Self(17);
    pub const RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT: Self = Self(18);
    pub const RUNTIME_INVARIANT_VIOLATION: Self = Self(19);
    pub const RUNTIME_CONFLICT: Self = Self(20);
    pub const RUNTIME_NOT_FOUND: Self = Self(21);
    pub const RUNTIME_UNSUPPORTED: Self = Self(22);
    pub const RUNTIME_INTERNAL: Self = Self(23);

    pub const RUNTIME_BOUNDARY_SQL_SURFACE_CONTROLLER_REQUIRED: Self = Self(24);
    pub const RUNTIME_BOUNDARY_SCHEMA_SURFACE_CONTROLLER_REQUIRED: Self = Self(25);
    pub const RUNTIME_BOUNDARY_SQL_QUERY_NO_CONFIGURED_ENTITIES: Self = Self(26);
    pub const RUNTIME_BOUNDARY_SQL_QUERY_ENTITY_NOT_CONFIGURED: Self = Self(27);
    pub const RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED: Self = Self(28);
    pub const RUNTIME_BOUNDARY_SQL_DDL_ENTITY_NOT_CONFIGURED: Self = Self(29);
    pub const RUNTIME_BOUNDARY_QUERY_RESPONSE_ROWS_REQUIRED: Self = Self(30);
    pub const RUNTIME_BOUNDARY_QUERY_RESPONSE_GROUPED_ROWS_REQUIRED: Self = Self(31);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITY_REQUIRED: Self = Self(32);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITIES_REQUIRED: Self = Self(33);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_ID_REQUIRED: Self = Self(34);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_IDS_REQUIRED: Self = Self(35);
    pub const RUNTIME_BOUNDARY_ROW_PROJECTION_FIELD_NOT_CONFIGURED: Self = Self(36);

    pub const SQL_FEATURE_AGGREGATE_FILTER_CLAUSE: Self = Self(37);
    pub const SQL_FEATURE_ALTER_STATEMENT_BEYOND_ALTER_TABLE: Self = Self(38);
    pub const SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_DUPLICATE_DEFAULT: Self = Self(39);
    pub const SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_MODIFIERS: Self = Self(40);
    pub const SQL_FEATURE_ALTER_TABLE_ADD_STATEMENT_BEYOND_ADD_COLUMN: Self = Self(41);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_DROP_UNSUPPORTED_ACTION: Self = Self(42);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_MODIFIERS: Self = Self(43);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_SET_UNSUPPORTED_ACTION: Self = Self(44);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_UNSUPPORTED_ACTION: Self = Self(45);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_STATEMENT_BEYOND_ALTER_COLUMN: Self = Self(46);
    pub const SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_IF_EXISTS_SYNTAX: Self = Self(47);
    pub const SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_MODIFIERS: Self = Self(48);
    pub const SQL_FEATURE_ALTER_TABLE_DROP_STATEMENT_BEYOND_DROP_COLUMN: Self = Self(49);
    pub const SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MISSING_TO: Self = Self(50);
    pub const SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MODIFIERS: Self = Self(51);
    pub const SQL_FEATURE_ALTER_TABLE_RENAME_STATEMENT_BEYOND_RENAME_COLUMN: Self = Self(52);
    pub const SQL_FEATURE_ALTER_TABLE_UNSUPPORTED_OPERATION: Self = Self(53);
    pub const SQL_FEATURE_COLUMN_ALIAS: Self = Self(54);
    pub const SQL_FEATURE_CREATE_INDEX_IF_NOT_EXISTS_SYNTAX: Self = Self(55);
    pub const SQL_FEATURE_CREATE_INDEX_KEY_ORDERING_MODIFIERS: Self = Self(56);
    pub const SQL_FEATURE_CREATE_INDEX_MODIFIERS: Self = Self(57);
    pub const SQL_FEATURE_CREATE_STATEMENT_BEYOND_CREATE_INDEX: Self = Self(58);
    pub const SQL_FEATURE_DESCRIBE_MODIFIER: Self = Self(59);
    pub const SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_EXPECTED_CLAUSE: Self = Self(60);
    pub const SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_SET_CLAUSE: Self = Self(61);
    pub const SQL_FEATURE_DROP_INDEX_MODIFIERS: Self = Self(62);
    pub const SQL_FEATURE_DROP_INDEX_IF_EXISTS_SYNTAX: Self = Self(63);
    pub const SQL_FEATURE_DROP_STATEMENT_BEYOND_DROP_INDEX: Self = Self(64);
    pub const SQL_FEATURE_EXPRESSION_INDEX_UNSUPPORTED_FUNCTION: Self = Self(65);
    pub const SQL_FEATURE_HAVING: Self = Self(66);
    pub const SQL_FEATURE_INSERT: Self = Self(67);
    pub const SQL_FEATURE_JOIN: Self = Self(68);
    pub const SQL_FEATURE_LIKE_PATTERN_BEYOND_TRAILING_PREFIX: Self = Self(69);
    pub const SQL_FEATURE_LOWER_FIELD_PREDICATE_UNSUPPORTED: Self = Self(70);
    pub const SQL_FEATURE_MULTI_STATEMENT_SQL: Self = Self(71);
    pub const SQL_FEATURE_NESTED_AGGREGATE_INPUT: Self = Self(72);
    pub const SQL_FEATURE_NESTED_PROJECTION_FUNCTION_IN_ARITHMETIC: Self = Self(73);
    pub const SQL_FEATURE_ORDER_BY_UNSUPPORTED_FORM: Self = Self(74);
    pub const SQL_FEATURE_OTHER: Self = Self(75);
    pub const SQL_FEATURE_PARAMETER_BINDING: Self = Self(76);
    pub const SQL_FEATURE_PARAMETERIZED_SCHEMA_VERSION: Self = Self(77);
    pub const SQL_FEATURE_PREDICATE_STARTS_WITH_FIRST_ARGUMENT: Self = Self(78);
    pub const SQL_FEATURE_QUOTED_IDENTIFIERS: Self = Self(79);
    pub const SQL_FEATURE_RETURNING_UNSUPPORTED_SHAPE: Self = Self(80);
    pub const SQL_FEATURE_SCALAR_FUNCTION_EXPRESSION_POSITION: Self = Self(81);
    pub const SQL_FEATURE_SCALE_TAKING_NUMERIC_FUNCTION_EXPRESSION_POSITION: Self = Self(82);
    pub const SQL_FEATURE_SEARCHED_CASE_GROUPED_ORDER_BY: Self = Self(83);
    pub const SQL_FEATURE_SHOW_COLUMNS_MODIFIERS: Self = Self(84);
    pub const SQL_FEATURE_SHOW_ENTITIES_MODIFIERS: Self = Self(85);
    pub const SQL_FEATURE_SHOW_INDEXES_MODIFIERS: Self = Self(86);
    pub const SQL_FEATURE_SHOW_MEMORY_MODIFIERS: Self = Self(87);
    pub const SQL_FEATURE_SHOW_STORES_MODIFIERS: Self = Self(88);
    pub const SQL_FEATURE_SHOW_UNSUPPORTED_COMMAND: Self = Self(89);
    pub const SQL_FEATURE_SIMPLE_CASE_EXPRESSION: Self = Self(90);
    pub const SQL_FEATURE_STANDALONE_LITERAL_PROJECTION_ITEM: Self = Self(91);
    pub const SQL_FEATURE_SUPPORTED_GROUPED_ORDER_BY_EXPRESSION_FAMILY: Self = Self(92);
    pub const SQL_FEATURE_SUPPORTED_ORDER_BY_EXPRESSION_FAMILY: Self = Self(93);
    pub const SQL_FEATURE_UNION_INTERSECT_EXCEPT: Self = Self(94);
    pub const SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE: Self = Self(95);
    pub const SQL_FEATURE_UPDATE: Self = Self(96);
    pub const SQL_FEATURE_UPPER_FIELD_PREDICATE_UNSUPPORTED: Self = Self(97);
    pub const SQL_FEATURE_WINDOW_FUNCTION: Self = Self(98);
    pub const SQL_FEATURE_WITH: Self = Self(99);

    const SQL_FEATURE_DETAILS: [SqlFeatureCode; 63] = [
        SqlFeatureCode::AggregateFilterClause,
        SqlFeatureCode::AlterStatementBeyondAlterTable,
        SqlFeatureCode::AlterTableAddColumnDuplicateDefault,
        SqlFeatureCode::AlterTableAddColumnModifiers,
        SqlFeatureCode::AlterTableAddStatementBeyondAddColumn,
        SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction,
        SqlFeatureCode::AlterTableAlterColumnModifiers,
        SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction,
        SqlFeatureCode::AlterTableAlterColumnUnsupportedAction,
        SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn,
        SqlFeatureCode::AlterTableDropColumnIfExistsSyntax,
        SqlFeatureCode::AlterTableDropColumnModifiers,
        SqlFeatureCode::AlterTableDropStatementBeyondDropColumn,
        SqlFeatureCode::AlterTableRenameColumnMissingTo,
        SqlFeatureCode::AlterTableRenameColumnModifiers,
        SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn,
        SqlFeatureCode::AlterTableUnsupportedOperation,
        SqlFeatureCode::ColumnAlias,
        SqlFeatureCode::CreateIndexIfNotExistsSyntax,
        SqlFeatureCode::CreateIndexKeyOrderingModifiers,
        SqlFeatureCode::CreateIndexModifiers,
        SqlFeatureCode::CreateStatementBeyondCreateIndex,
        SqlFeatureCode::DescribeModifier,
        SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause,
        SqlFeatureCode::DdlSchemaVersionDuplicateSetClause,
        SqlFeatureCode::DropIndexModifiers,
        SqlFeatureCode::DropIndexIfExistsSyntax,
        SqlFeatureCode::DropStatementBeyondDropIndex,
        SqlFeatureCode::ExpressionIndexUnsupportedFunction,
        SqlFeatureCode::Having,
        SqlFeatureCode::Insert,
        SqlFeatureCode::Join,
        SqlFeatureCode::LikePatternBeyondTrailingPrefix,
        SqlFeatureCode::LowerFieldPredicateUnsupported,
        SqlFeatureCode::MultiStatementSql,
        SqlFeatureCode::NestedAggregateInput,
        SqlFeatureCode::NestedProjectionFunctionInArithmetic,
        SqlFeatureCode::OrderByUnsupportedForm,
        SqlFeatureCode::Other,
        SqlFeatureCode::ParameterBinding,
        SqlFeatureCode::ParameterizedSchemaVersion,
        SqlFeatureCode::PredicateStartsWithFirstArgument,
        SqlFeatureCode::QuotedIdentifiers,
        SqlFeatureCode::ReturningUnsupportedShape,
        SqlFeatureCode::ScalarFunctionExpressionPosition,
        SqlFeatureCode::ScaleTakingNumericFunctionExpressionPosition,
        SqlFeatureCode::SearchedCaseGroupedOrderBy,
        SqlFeatureCode::ShowColumnsModifiers,
        SqlFeatureCode::ShowEntitiesModifiers,
        SqlFeatureCode::ShowIndexesModifiers,
        SqlFeatureCode::ShowMemoryModifiers,
        SqlFeatureCode::ShowStoresModifiers,
        SqlFeatureCode::ShowUnsupportedCommand,
        SqlFeatureCode::SimpleCaseExpression,
        SqlFeatureCode::StandaloneLiteralProjectionItem,
        SqlFeatureCode::SupportedGroupedOrderByExpressionFamily,
        SqlFeatureCode::SupportedOrderByExpressionFamily,
        SqlFeatureCode::UnionIntersectExcept,
        SqlFeatureCode::UnsupportedFunctionNamespace,
        SqlFeatureCode::Update,
        SqlFeatureCode::UpperFieldPredicateUnsupported,
        SqlFeatureCode::WindowFunction,
        SqlFeatureCode::With,
    ];

    pub const SQL_SURFACE_QUERY_REJECTS_INSERT: Self = Self(100);
    pub const SQL_SURFACE_QUERY_REJECTS_UPDATE: Self = Self(101);
    pub const SQL_SURFACE_QUERY_REJECTS_DELETE: Self = Self(102);
    pub const SQL_SURFACE_UPDATE_REJECTS_SELECT: Self = Self(103);
    pub const SQL_SURFACE_UPDATE_REJECTS_EXPLAIN: Self = Self(104);
    pub const SQL_SURFACE_UPDATE_REJECTS_DESCRIBE: Self = Self(105);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_INDEXES: Self = Self(106);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_COLUMNS: Self = Self(107);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_ENTITIES: Self = Self(108);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_STORES: Self = Self(109);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_MEMORY: Self = Self(110);

    pub const SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION: Self = Self(111);
    pub const SCHEMA_DDL_MISSING_NEXT_SCHEMA_VERSION: Self = Self(112);
    pub const SCHEMA_DDL_STALE_EXPECTED_SCHEMA_VERSION: Self = Self(113);
    pub const SCHEMA_DDL_INVALID_EXPECTED_SCHEMA_VERSION: Self = Self(114);
    pub const SCHEMA_DDL_INVALID_NEXT_SCHEMA_VERSION: Self = Self(115);
    pub const SCHEMA_DDL_ACCEPTED_SCHEMA_CHANGE_WITHOUT_VERSION_BUMP: Self = Self(116);
    pub const SCHEMA_DDL_EMPTY_VERSION_BUMP: Self = Self(117);
    pub const SCHEMA_DDL_VERSION_GAP: Self = Self(118);
    pub const SCHEMA_DDL_VERSION_ROLLBACK: Self = Self(119);
    pub const SCHEMA_DDL_FINGERPRINT_METHOD_MISMATCH: Self = Self(120);
    pub const SCHEMA_DDL_UNSUPPORTED_TRANSITION_CLASS: Self = Self(121);
    pub const SCHEMA_DDL_PHYSICAL_RUNNER_MISSING: Self = Self(122);
    pub const SCHEMA_DDL_VALIDATION_FAILED: Self = Self(123);
    pub const SCHEMA_DDL_PUBLICATION_RACE_LOST: Self = Self(124);
    pub const SCHEMA_DDL_INVALID_ADD_COLUMN_DEFAULT: Self = Self(125);
    pub const SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT: Self = Self(126);
    pub const SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED: Self = Self(127);
    pub const SCHEMA_DDL_REQUIRED_DROP_DEFAULT_UNSUPPORTED: Self = Self(128);
    pub const SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED: Self = Self(129);
    pub const SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED: Self = Self(130);
    pub const SCHEMA_DDL_SET_NOT_NULL_VALIDATION_FAILED: Self = Self(131);

    /// Build an error code from its raw public wire value.
    #[must_use]
    pub const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    /// Return the raw public wire value.
    #[must_use]
    pub const fn raw(self) -> u16 {
        self.0
    }

    /// Collapse a rich diagnostic into one public leaf code.
    #[must_use]
    pub const fn from_parts(code: DiagnosticCode, detail: Option<DiagnosticDetail>) -> Self {
        match detail {
            Some(DiagnosticDetail::QueryKind { kind }) => Self::from_query_kind(kind),
            Some(DiagnosticDetail::RuntimeKind { kind }) => Self::from_runtime_kind(kind),
            Some(DiagnosticDetail::RuntimeBoundary { boundary }) => {
                Self::from_runtime_boundary(boundary)
            }
            Some(DiagnosticDetail::SchemaDdlAdmission { reason }) => Self::from_schema_ddl(reason),
            Some(DiagnosticDetail::UnsupportedSqlFeature { feature }) => {
                Self::from_sql_feature(feature)
            }
            Some(DiagnosticDetail::SqlSurfaceMismatch { mismatch }) => {
                Self::from_sql_surface_mismatch(mismatch)
            }
            None => code.error_code(),
        }
    }

    /// Return the broad diagnostic reason represented by this public code.
    #[must_use]
    pub const fn diagnostic_code(self) -> DiagnosticCode {
        match self.raw() {
            1 => DiagnosticCode::QueryValidate,
            2 => DiagnosticCode::QueryIntent,
            3 => DiagnosticCode::QueryPlan,
            4 => DiagnosticCode::QueryAccessRequirement,
            5 => DiagnosticCode::QueryUnorderedPagination,
            6 => DiagnosticCode::QueryInvalidContinuationCursor,
            7 => DiagnosticCode::QueryNotFound,
            8 => DiagnosticCode::QueryNotUnique,
            9 => DiagnosticCode::QueryNumericOverflow,
            10 => DiagnosticCode::QueryNumericNotRepresentable,
            11 | 37..=99 => DiagnosticCode::QueryUnsupportedSqlFeature,
            12 | 100..=110 => DiagnosticCode::QuerySqlSurfaceMismatch,
            13 | 111..=131 => DiagnosticCode::SchemaDdlAdmission,
            14 => DiagnosticCode::StoreNotFound,
            15 => DiagnosticCode::StoreCorruption,
            16 => DiagnosticCode::StoreInvariantViolation,
            17 => DiagnosticCode::RuntimeCorruption,
            18 => DiagnosticCode::RuntimeIncompatiblePersistedFormat,
            19 => DiagnosticCode::RuntimeInvariantViolation,
            20 => DiagnosticCode::RuntimeConflict,
            21 => DiagnosticCode::RuntimeNotFound,
            22 | 24..=36 => DiagnosticCode::RuntimeUnsupported,
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
            1..=8 => Self::query_kind_detail(self.raw()),
            17..=23 => Self::runtime_kind_detail(self.raw()),
            24..=36 => Self::runtime_boundary_detail(self.raw()),
            37..=99 => Self::sql_feature_detail(self.raw()),
            100..=110 => Self::sql_surface_detail(self.raw()),
            111..=131 => Self::schema_ddl_detail(self.raw()),
            _ => None,
        }
    }

    /// Reconstruct a rich diagnostic payload for host-side rendering.
    #[must_use]
    pub const fn diagnostic(self, origin: ErrorOrigin) -> Diagnostic {
        Diagnostic::new(self.diagnostic_code(), origin, self.diagnostic_detail())
    }

    const fn from_query_kind(kind: QueryErrorKind) -> Self {
        match kind {
            QueryErrorKind::Validate => Self::QUERY_VALIDATE,
            QueryErrorKind::Intent => Self::QUERY_INTENT,
            QueryErrorKind::Plan => Self::QUERY_PLAN,
            QueryErrorKind::AccessRequirement => Self::QUERY_ACCESS_REQUIREMENT,
            QueryErrorKind::UnorderedPagination => Self::QUERY_UNORDERED_PAGINATION,
            QueryErrorKind::InvalidContinuationCursor => Self::QUERY_INVALID_CONTINUATION_CURSOR,
            QueryErrorKind::NotFound => Self::QUERY_NOT_FOUND,
            QueryErrorKind::NotUnique => Self::QUERY_NOT_UNIQUE,
        }
    }

    const fn from_runtime_kind(kind: RuntimeErrorKind) -> Self {
        match kind {
            RuntimeErrorKind::Corruption => Self::RUNTIME_CORRUPTION,
            RuntimeErrorKind::IncompatiblePersistedFormat => {
                Self::RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT
            }
            RuntimeErrorKind::InvariantViolation => Self::RUNTIME_INVARIANT_VIOLATION,
            RuntimeErrorKind::Conflict => Self::RUNTIME_CONFLICT,
            RuntimeErrorKind::NotFound => Self::RUNTIME_NOT_FOUND,
            RuntimeErrorKind::Unsupported => Self::RUNTIME_UNSUPPORTED,
            RuntimeErrorKind::Internal => Self::RUNTIME_INTERNAL,
        }
    }

    const fn from_runtime_boundary(boundary: RuntimeBoundaryCode) -> Self {
        Self(Self::RUNTIME_BOUNDARY_SQL_SURFACE_CONTROLLER_REQUIRED.raw() + boundary as u16)
    }

    const fn from_sql_feature(feature: SqlFeatureCode) -> Self {
        Self(Self::SQL_FEATURE_AGGREGATE_FILTER_CLAUSE.raw() + feature as u16)
    }

    const fn from_sql_surface_mismatch(mismatch: SqlSurfaceMismatchCode) -> Self {
        Self(Self::SQL_SURFACE_QUERY_REJECTS_INSERT.raw() + mismatch as u16)
    }

    const fn from_schema_ddl(reason: SchemaDdlAdmissionCode) -> Self {
        Self(Self::SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION.raw() + reason as u16)
    }

    const fn query_kind_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            1 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::Validate,
            }),
            2 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::Intent,
            }),
            3 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::Plan,
            }),
            4 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::AccessRequirement,
            }),
            5 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::UnorderedPagination,
            }),
            6 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::InvalidContinuationCursor,
            }),
            7 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::NotFound,
            }),
            8 => Some(DiagnosticDetail::QueryKind {
                kind: QueryErrorKind::NotUnique,
            }),
            _ => None,
        }
    }

    const fn runtime_kind_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            17 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Corruption,
            }),
            18 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::IncompatiblePersistedFormat,
            }),
            19 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::InvariantViolation,
            }),
            20 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Conflict,
            }),
            21 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::NotFound,
            }),
            22 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Unsupported,
            }),
            23 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Internal,
            }),
            _ => None,
        }
    }

    const fn runtime_boundary_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            24 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlSurfaceControllerRequired,
            }),
            25 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SchemaSurfaceControllerRequired,
            }),
            26 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
            }),
            27 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlQueryEntityNotConfigured,
            }),
            28 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlDdlTargetRequired,
            }),
            29 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlDdlEntityNotConfigured,
            }),
            30 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::QueryResponseRowsRequired,
            }),
            31 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::QueryResponseGroupedRowsRequired,
            }),
            32 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultEntityRequired,
            }),
            33 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultEntitiesRequired,
            }),
            34 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultIdRequired,
            }),
            35 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultIdsRequired,
            }),
            36 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::RowProjectionFieldNotConfigured,
            }),
            _ => None,
        }
    }

    const fn sql_feature_detail(raw: u16) -> Option<DiagnosticDetail> {
        let base = Self::SQL_FEATURE_AGGREGATE_FILTER_CLAUSE.raw();
        if raw < base {
            return None;
        }

        let offset = (raw - base) as usize;
        if offset < Self::SQL_FEATURE_DETAILS.len() {
            Some(DiagnosticDetail::UnsupportedSqlFeature {
                feature: Self::SQL_FEATURE_DETAILS[offset],
            })
        } else {
            None
        }
    }

    const fn sql_surface_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            100 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::QueryRejectsInsert,
            }),
            101 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::QueryRejectsUpdate,
            }),
            102 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::QueryRejectsDelete,
            }),
            103 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsSelect,
            }),
            104 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsExplain,
            }),
            105 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsDescribe,
            }),
            106 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowIndexes,
            }),
            107 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowColumns,
            }),
            108 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowEntities,
            }),
            109 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowStores,
            }),
            110 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowMemory,
            }),
            _ => None,
        }
    }

    const fn schema_ddl_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            111 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::MissingExpectedSchemaVersion,
            }),
            112 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::MissingNextSchemaVersion,
            }),
            113 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::StaleExpectedSchemaVersion,
            }),
            114 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion,
            }),
            115 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidNextSchemaVersion,
            }),
            116 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump,
            }),
            117 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::EmptyVersionBump,
            }),
            118 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::VersionGap,
            }),
            119 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::VersionRollback,
            }),
            120 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::FingerprintMethodMismatch,
            }),
            121 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::UnsupportedTransitionClass,
            }),
            122 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::PhysicalRunnerMissing,
            }),
            123 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::ValidationFailed,
            }),
            124 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::PublicationRaceLost,
            }),
            125 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidAddColumnDefault,
            }),
            126 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidAlterColumnDefault,
            }),
            127 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::GeneratedIndexDropRejected,
            }),
            128 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::RequiredDropDefaultUnsupported,
            }),
            129 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected,
            }),
            130 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected,
            }),
            131 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::SetNotNullValidationFailed,
            }),
            _ => None,
        }
    }
}

impl std::fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ErrorCode").field(&self.0).finish()
    }
}

///
/// ErrorClass
///
/// Broad diagnostic class used for recovery decisions.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorClass {
    Query,
    Corruption,
    IncompatiblePersistedFormat,
    NotFound,
    Internal,
    Conflict,
    Unsupported,
    InvariantViolation,
}

///
/// ErrorOrigin
///
/// Subsystem that owns the diagnostic.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorOrigin {
    Cursor,
    Executor,
    Identity,
    Index,
    Interface,
    Planner,
    Query,
    Recovery,
    Response,
    Runtime,
    Serialize,
    Store,
}

///
/// QueryErrorKind
///
/// Public query error category.
///

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryErrorKind {
    Validate,
    Intent,
    Plan,
    AccessRequirement,
    UnorderedPagination,
    InvalidContinuationCursor,
    NotFound,
    NotUnique,
}

///
/// RuntimeErrorKind
///
/// Public runtime error category.
///

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuntimeErrorKind {
    Corruption,
    IncompatiblePersistedFormat,
    InvariantViolation,
    Conflict,
    NotFound,
    Unsupported,
    Internal,
}

///
/// RuntimeBoundaryCode
///
/// Compact public-runtime boundary identifier.
///

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuntimeBoundaryCode {
    SqlSurfaceControllerRequired,
    SchemaSurfaceControllerRequired,
    SqlQueryNoConfiguredEntities,
    SqlQueryEntityNotConfigured,
    SqlDdlTargetRequired,
    SqlDdlEntityNotConfigured,
    QueryResponseRowsRequired,
    QueryResponseGroupedRowsRequired,
    MutationResultEntityRequired,
    MutationResultEntitiesRequired,
    MutationResultIdRequired,
    MutationResultIdsRequired,
    RowProjectionFieldNotConfigured,
}

///
/// SqlFeatureCode
///
/// Compact SQL feature identifier used by unsupported-feature diagnostics.
///

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SqlFeatureCode {
    AggregateFilterClause,
    AlterStatementBeyondAlterTable,
    AlterTableAddColumnDuplicateDefault,
    AlterTableAddColumnModifiers,
    AlterTableAddStatementBeyondAddColumn,
    AlterTableAlterColumnDropUnsupportedAction,
    AlterTableAlterColumnModifiers,
    AlterTableAlterColumnSetUnsupportedAction,
    AlterTableAlterColumnUnsupportedAction,
    AlterTableAlterStatementBeyondAlterColumn,
    AlterTableDropColumnIfExistsSyntax,
    AlterTableDropColumnModifiers,
    AlterTableDropStatementBeyondDropColumn,
    AlterTableRenameColumnMissingTo,
    AlterTableRenameColumnModifiers,
    AlterTableRenameStatementBeyondRenameColumn,
    AlterTableUnsupportedOperation,
    ColumnAlias,
    CreateIndexIfNotExistsSyntax,
    CreateIndexKeyOrderingModifiers,
    CreateIndexModifiers,
    CreateStatementBeyondCreateIndex,
    DescribeModifier,
    DdlSchemaVersionDuplicateExpectedClause,
    DdlSchemaVersionDuplicateSetClause,
    DropIndexModifiers,
    DropIndexIfExistsSyntax,
    DropStatementBeyondDropIndex,
    ExpressionIndexUnsupportedFunction,
    Having,
    Insert,
    Join,
    LikePatternBeyondTrailingPrefix,
    LowerFieldPredicateUnsupported,
    MultiStatementSql,
    NestedAggregateInput,
    NestedProjectionFunctionInArithmetic,
    OrderByUnsupportedForm,
    Other,
    ParameterBinding,
    ParameterizedSchemaVersion,
    PredicateStartsWithFirstArgument,
    QuotedIdentifiers,
    ReturningUnsupportedShape,
    ScalarFunctionExpressionPosition,
    ScaleTakingNumericFunctionExpressionPosition,
    SearchedCaseGroupedOrderBy,
    ShowColumnsModifiers,
    ShowEntitiesModifiers,
    ShowIndexesModifiers,
    ShowMemoryModifiers,
    ShowStoresModifiers,
    ShowUnsupportedCommand,
    SimpleCaseExpression,
    StandaloneLiteralProjectionItem,
    SupportedGroupedOrderByExpressionFamily,
    SupportedOrderByExpressionFamily,
    UnionIntersectExcept,
    UnsupportedFunctionNamespace,
    Update,
    UpperFieldPredicateUnsupported,
    WindowFunction,
    With,
}

///
/// SqlSurfaceMismatchCode
///
/// Compact SQL endpoint surface mismatch identifier.
///

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SqlSurfaceMismatchCode {
    QueryRejectsInsert,
    QueryRejectsUpdate,
    QueryRejectsDelete,
    UpdateRejectsSelect,
    UpdateRejectsExplain,
    UpdateRejectsDescribe,
    UpdateRejectsShowIndexes,
    UpdateRejectsShowColumns,
    UpdateRejectsShowEntities,
    UpdateRejectsShowStores,
    UpdateRejectsShowMemory,
}

///
/// SchemaDdlAdmissionCode
///
/// Compact SQL DDL admission rejection reason.
///

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SchemaDdlAdmissionCode {
    MissingExpectedSchemaVersion,
    MissingNextSchemaVersion,
    StaleExpectedSchemaVersion,
    InvalidExpectedSchemaVersion,
    InvalidNextSchemaVersion,
    AcceptedSchemaChangeWithoutVersionBump,
    EmptyVersionBump,
    VersionGap,
    VersionRollback,
    FingerprintMethodMismatch,
    UnsupportedTransitionClass,
    PhysicalRunnerMissing,
    ValidationFailed,
    PublicationRaceLost,
    InvalidAddColumnDefault,
    InvalidAlterColumnDefault,
    GeneratedIndexDropRejected,
    RequiredDropDefaultUnsupported,
    GeneratedFieldDefaultChangeRejected,
    GeneratedFieldNullabilityChangeRejected,
    SetNotNullValidationFailed,
}

///
/// DiagnosticDetail
///
/// Small structured diagnostic payload for callers and CLI rendering.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticDetail {
    QueryKind { kind: QueryErrorKind },
    RuntimeKind { kind: RuntimeErrorKind },
    RuntimeBoundary { boundary: RuntimeBoundaryCode },
    SchemaDdlAdmission { reason: SchemaDdlAdmissionCode },
    UnsupportedSqlFeature { feature: SqlFeatureCode },
    SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode },
}

///
/// Diagnostic
///
/// Compact public diagnostic payload.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    code: DiagnosticCode,
    origin: ErrorOrigin,
    detail: Option<DiagnosticDetail>,
}

impl Diagnostic {
    /// Build a compact diagnostic from a code and optional structured detail.
    #[must_use]
    pub const fn new(
        code: DiagnosticCode,
        origin: ErrorOrigin,
        detail: Option<DiagnosticDetail>,
    ) -> Self {
        Self {
            code,
            origin,
            detail,
        }
    }

    /// Build a compact diagnostic using the code's default origin.
    #[must_use]
    pub const fn from_code(code: DiagnosticCode) -> Self {
        Self::new(code, code.origin(), None)
    }

    /// Return the stable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> DiagnosticCode {
        self.code
    }

    /// Return the diagnostic class.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        self.code.class()
    }

    /// Return the subsystem origin.
    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        self.origin
    }

    /// Return structured diagnostic detail, when available.
    #[must_use]
    pub const fn detail(&self) -> Option<&DiagnosticDetail> {
        self.detail.as_ref()
    }

    /// Return the numeric public wire code for this diagnostic.
    #[must_use]
    pub const fn error_code(&self) -> ErrorCode {
        ErrorCode::from_parts(self.code, self.detail)
    }
}

#[cfg(test)]
mod tests {
    use super::{Diagnostic, DiagnosticCode, ErrorClass, ErrorCode, ErrorOrigin};

    const ORDERED_ERROR_CODES: [ErrorCode; 131] = [
        ErrorCode::QUERY_VALIDATE,
        ErrorCode::QUERY_INTENT,
        ErrorCode::QUERY_PLAN,
        ErrorCode::QUERY_ACCESS_REQUIREMENT,
        ErrorCode::QUERY_UNORDERED_PAGINATION,
        ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
        ErrorCode::QUERY_NOT_FOUND,
        ErrorCode::QUERY_NOT_UNIQUE,
        ErrorCode::QUERY_NUMERIC_OVERFLOW,
        ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE,
        ErrorCode::QUERY_UNSUPPORTED_SQL_FEATURE,
        ErrorCode::QUERY_SQL_SURFACE_MISMATCH,
        ErrorCode::SCHEMA_DDL_ADMISSION,
        ErrorCode::STORE_NOT_FOUND,
        ErrorCode::STORE_CORRUPTION,
        ErrorCode::STORE_INVARIANT_VIOLATION,
        ErrorCode::RUNTIME_CORRUPTION,
        ErrorCode::RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT,
        ErrorCode::RUNTIME_INVARIANT_VIOLATION,
        ErrorCode::RUNTIME_CONFLICT,
        ErrorCode::RUNTIME_NOT_FOUND,
        ErrorCode::RUNTIME_UNSUPPORTED,
        ErrorCode::RUNTIME_INTERNAL,
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_CONTROLLER_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_CONTROLLER_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_SQL_QUERY_NO_CONFIGURED_ENTITIES,
        ErrorCode::RUNTIME_BOUNDARY_SQL_QUERY_ENTITY_NOT_CONFIGURED,
        ErrorCode::RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_SQL_DDL_ENTITY_NOT_CONFIGURED,
        ErrorCode::RUNTIME_BOUNDARY_QUERY_RESPONSE_ROWS_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_QUERY_RESPONSE_GROUPED_ROWS_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITY_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITIES_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_MUTATION_RESULT_ID_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_MUTATION_RESULT_IDS_REQUIRED,
        ErrorCode::RUNTIME_BOUNDARY_ROW_PROJECTION_FIELD_NOT_CONFIGURED,
        ErrorCode::SQL_FEATURE_AGGREGATE_FILTER_CLAUSE,
        ErrorCode::SQL_FEATURE_ALTER_STATEMENT_BEYOND_ALTER_TABLE,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_DUPLICATE_DEFAULT,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_MODIFIERS,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ADD_STATEMENT_BEYOND_ADD_COLUMN,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_DROP_UNSUPPORTED_ACTION,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_MODIFIERS,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_SET_UNSUPPORTED_ACTION,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_UNSUPPORTED_ACTION,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_ALTER_STATEMENT_BEYOND_ALTER_COLUMN,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_IF_EXISTS_SYNTAX,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_MODIFIERS,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_DROP_STATEMENT_BEYOND_DROP_COLUMN,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MISSING_TO,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MODIFIERS,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_RENAME_STATEMENT_BEYOND_RENAME_COLUMN,
        ErrorCode::SQL_FEATURE_ALTER_TABLE_UNSUPPORTED_OPERATION,
        ErrorCode::SQL_FEATURE_COLUMN_ALIAS,
        ErrorCode::SQL_FEATURE_CREATE_INDEX_IF_NOT_EXISTS_SYNTAX,
        ErrorCode::SQL_FEATURE_CREATE_INDEX_KEY_ORDERING_MODIFIERS,
        ErrorCode::SQL_FEATURE_CREATE_INDEX_MODIFIERS,
        ErrorCode::SQL_FEATURE_CREATE_STATEMENT_BEYOND_CREATE_INDEX,
        ErrorCode::SQL_FEATURE_DESCRIBE_MODIFIER,
        ErrorCode::SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_EXPECTED_CLAUSE,
        ErrorCode::SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_SET_CLAUSE,
        ErrorCode::SQL_FEATURE_DROP_INDEX_MODIFIERS,
        ErrorCode::SQL_FEATURE_DROP_INDEX_IF_EXISTS_SYNTAX,
        ErrorCode::SQL_FEATURE_DROP_STATEMENT_BEYOND_DROP_INDEX,
        ErrorCode::SQL_FEATURE_EXPRESSION_INDEX_UNSUPPORTED_FUNCTION,
        ErrorCode::SQL_FEATURE_HAVING,
        ErrorCode::SQL_FEATURE_INSERT,
        ErrorCode::SQL_FEATURE_JOIN,
        ErrorCode::SQL_FEATURE_LIKE_PATTERN_BEYOND_TRAILING_PREFIX,
        ErrorCode::SQL_FEATURE_LOWER_FIELD_PREDICATE_UNSUPPORTED,
        ErrorCode::SQL_FEATURE_MULTI_STATEMENT_SQL,
        ErrorCode::SQL_FEATURE_NESTED_AGGREGATE_INPUT,
        ErrorCode::SQL_FEATURE_NESTED_PROJECTION_FUNCTION_IN_ARITHMETIC,
        ErrorCode::SQL_FEATURE_ORDER_BY_UNSUPPORTED_FORM,
        ErrorCode::SQL_FEATURE_OTHER,
        ErrorCode::SQL_FEATURE_PARAMETER_BINDING,
        ErrorCode::SQL_FEATURE_PARAMETERIZED_SCHEMA_VERSION,
        ErrorCode::SQL_FEATURE_PREDICATE_STARTS_WITH_FIRST_ARGUMENT,
        ErrorCode::SQL_FEATURE_QUOTED_IDENTIFIERS,
        ErrorCode::SQL_FEATURE_RETURNING_UNSUPPORTED_SHAPE,
        ErrorCode::SQL_FEATURE_SCALAR_FUNCTION_EXPRESSION_POSITION,
        ErrorCode::SQL_FEATURE_SCALE_TAKING_NUMERIC_FUNCTION_EXPRESSION_POSITION,
        ErrorCode::SQL_FEATURE_SEARCHED_CASE_GROUPED_ORDER_BY,
        ErrorCode::SQL_FEATURE_SHOW_COLUMNS_MODIFIERS,
        ErrorCode::SQL_FEATURE_SHOW_ENTITIES_MODIFIERS,
        ErrorCode::SQL_FEATURE_SHOW_INDEXES_MODIFIERS,
        ErrorCode::SQL_FEATURE_SHOW_MEMORY_MODIFIERS,
        ErrorCode::SQL_FEATURE_SHOW_STORES_MODIFIERS,
        ErrorCode::SQL_FEATURE_SHOW_UNSUPPORTED_COMMAND,
        ErrorCode::SQL_FEATURE_SIMPLE_CASE_EXPRESSION,
        ErrorCode::SQL_FEATURE_STANDALONE_LITERAL_PROJECTION_ITEM,
        ErrorCode::SQL_FEATURE_SUPPORTED_GROUPED_ORDER_BY_EXPRESSION_FAMILY,
        ErrorCode::SQL_FEATURE_SUPPORTED_ORDER_BY_EXPRESSION_FAMILY,
        ErrorCode::SQL_FEATURE_UNION_INTERSECT_EXCEPT,
        ErrorCode::SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE,
        ErrorCode::SQL_FEATURE_UPDATE,
        ErrorCode::SQL_FEATURE_UPPER_FIELD_PREDICATE_UNSUPPORTED,
        ErrorCode::SQL_FEATURE_WINDOW_FUNCTION,
        ErrorCode::SQL_FEATURE_WITH,
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_INSERT,
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_UPDATE,
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_DELETE,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_SELECT,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_EXPLAIN,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_DESCRIBE,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_SHOW_INDEXES,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_SHOW_COLUMNS,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_SHOW_ENTITIES,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_SHOW_STORES,
        ErrorCode::SQL_SURFACE_UPDATE_REJECTS_SHOW_MEMORY,
        ErrorCode::SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION,
        ErrorCode::SCHEMA_DDL_MISSING_NEXT_SCHEMA_VERSION,
        ErrorCode::SCHEMA_DDL_STALE_EXPECTED_SCHEMA_VERSION,
        ErrorCode::SCHEMA_DDL_INVALID_EXPECTED_SCHEMA_VERSION,
        ErrorCode::SCHEMA_DDL_INVALID_NEXT_SCHEMA_VERSION,
        ErrorCode::SCHEMA_DDL_ACCEPTED_SCHEMA_CHANGE_WITHOUT_VERSION_BUMP,
        ErrorCode::SCHEMA_DDL_EMPTY_VERSION_BUMP,
        ErrorCode::SCHEMA_DDL_VERSION_GAP,
        ErrorCode::SCHEMA_DDL_VERSION_ROLLBACK,
        ErrorCode::SCHEMA_DDL_FINGERPRINT_METHOD_MISMATCH,
        ErrorCode::SCHEMA_DDL_UNSUPPORTED_TRANSITION_CLASS,
        ErrorCode::SCHEMA_DDL_PHYSICAL_RUNNER_MISSING,
        ErrorCode::SCHEMA_DDL_VALIDATION_FAILED,
        ErrorCode::SCHEMA_DDL_PUBLICATION_RACE_LOST,
        ErrorCode::SCHEMA_DDL_INVALID_ADD_COLUMN_DEFAULT,
        ErrorCode::SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT,
        ErrorCode::SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED,
        ErrorCode::SCHEMA_DDL_REQUIRED_DROP_DEFAULT_UNSUPPORTED,
        ErrorCode::SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED,
        ErrorCode::SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED,
        ErrorCode::SCHEMA_DDL_SET_NOT_NULL_VALIDATION_FAILED,
    ];

    #[test]
    fn diagnostic_from_code_uses_default_origin() {
        let diagnostic = Diagnostic::from_code(DiagnosticCode::QueryPlan);

        assert_eq!(diagnostic.code(), DiagnosticCode::QueryPlan);
        assert_eq!(diagnostic.origin(), ErrorOrigin::Query);
    }

    #[test]
    fn diagnostic_code_reports_broad_class() {
        assert_eq!(
            DiagnosticCode::QueryUnsupportedSqlFeature.class(),
            ErrorClass::Unsupported
        );
        assert_eq!(
            DiagnosticCode::QuerySqlSurfaceMismatch.class(),
            ErrorClass::Unsupported
        );
        assert_eq!(DiagnosticCode::QueryPlan.class(), ErrorClass::Query);
        assert_eq!(
            DiagnosticCode::StoreCorruption.class(),
            ErrorClass::Corruption
        );
    }

    #[test]
    fn public_error_codes_are_sequential() {
        for (index, code) in ORDERED_ERROR_CODES.iter().enumerate() {
            let expected = u16::try_from(index + 1).expect("test error-code index fits u16");
            assert_eq!(code.raw(), expected);
        }
    }
}
