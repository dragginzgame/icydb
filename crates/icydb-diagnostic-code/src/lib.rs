//! Compact diagnostic identity for IcyDB.
//!
//! This crate intentionally contains no rich diagnostic prose or Candid wire
//! types. Production canister builds collapse diagnostics to numeric wire
//! codes before they cross the public canister boundary. `Debug` output is
//! numeric for the same reason: host tooling can recover labels from the code
//! table without making every wasm canister retain those labels.

use std::fmt;

///
/// DiagnosticCode
///
/// Stable machine-readable diagnostic reason.
///

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
    QueryUnknownAggregateTargetField,
    QueryUnsupportedProjection,
    QueryResultShapeMismatch,
    QueryUnsupportedSqlFeature,
    QuerySqlSurfaceMismatch,
    QuerySqlWriteBoundary,
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
            | Self::QueryUnknownAggregateTargetField
            | Self::QueryUnsupportedProjection
            | Self::QueryResultShapeMismatch
            | Self::QuerySqlSurfaceMismatch
            | Self::QuerySqlWriteBoundary
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
            | Self::QueryUnknownAggregateTargetField
            | Self::QueryUnsupportedProjection
            | Self::QueryResultShapeMismatch
            | Self::QueryUnsupportedSqlFeature
            | Self::QuerySqlSurfaceMismatch
            | Self::QuerySqlWriteBoundary
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
            Self::QueryUnknownAggregateTargetField => {
                ErrorCode::QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD
            }
            Self::QueryUnsupportedProjection => ErrorCode::QUERY_UNSUPPORTED_PROJECTION,
            Self::QueryResultShapeMismatch => ErrorCode::QUERY_RESULT_SHAPE_MISMATCH,
            Self::QueryUnsupportedSqlFeature => ErrorCode::QUERY_UNSUPPORTED_SQL_FEATURE,
            Self::QuerySqlSurfaceMismatch => ErrorCode::QUERY_SQL_SURFACE_MISMATCH,
            Self::QuerySqlWriteBoundary => ErrorCode::QUERY_SQL_WRITE_BOUNDARY,
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
    pub const QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD: Self = Self(11);
    pub const QUERY_UNSUPPORTED_SQL_FEATURE: Self = Self(12);
    pub const QUERY_SQL_SURFACE_MISMATCH: Self = Self(13);
    pub const SCHEMA_DDL_ADMISSION: Self = Self(14);
    pub const STORE_NOT_FOUND: Self = Self(15);
    pub const STORE_CORRUPTION: Self = Self(16);
    pub const STORE_INVARIANT_VIOLATION: Self = Self(17);
    pub const RUNTIME_CORRUPTION: Self = Self(18);
    pub const RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT: Self = Self(19);
    pub const RUNTIME_INVARIANT_VIOLATION: Self = Self(20);
    pub const RUNTIME_CONFLICT: Self = Self(21);
    pub const RUNTIME_NOT_FOUND: Self = Self(22);
    pub const RUNTIME_UNSUPPORTED: Self = Self(23);
    pub const RUNTIME_INTERNAL: Self = Self(24);

    pub const RUNTIME_BOUNDARY_SQL_SURFACE_CONTROLLER_REQUIRED: Self = Self(25);
    pub const RUNTIME_BOUNDARY_SCHEMA_SURFACE_CONTROLLER_REQUIRED: Self = Self(26);
    pub const RUNTIME_BOUNDARY_SQL_QUERY_NO_CONFIGURED_ENTITIES: Self = Self(27);
    pub const RUNTIME_BOUNDARY_SQL_QUERY_ENTITY_NOT_CONFIGURED: Self = Self(28);
    pub const RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED: Self = Self(29);
    pub const RUNTIME_BOUNDARY_SQL_DDL_ENTITY_NOT_CONFIGURED: Self = Self(30);
    pub const RUNTIME_BOUNDARY_QUERY_RESPONSE_ROWS_REQUIRED: Self = Self(31);
    pub const RUNTIME_BOUNDARY_QUERY_RESPONSE_GROUPED_ROWS_REQUIRED: Self = Self(32);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITY_REQUIRED: Self = Self(33);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_ENTITIES_REQUIRED: Self = Self(34);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_ID_REQUIRED: Self = Self(35);
    pub const RUNTIME_BOUNDARY_MUTATION_RESULT_IDS_REQUIRED: Self = Self(36);
    pub const RUNTIME_BOUNDARY_ROW_PROJECTION_FIELD_NOT_CONFIGURED: Self = Self(37);

    pub const SQL_FEATURE_AGGREGATE_FILTER_CLAUSE: Self = Self(38);
    pub const SQL_FEATURE_ALTER_STATEMENT_BEYOND_ALTER_TABLE: Self = Self(39);
    pub const SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_DUPLICATE_DEFAULT: Self = Self(40);
    pub const SQL_FEATURE_ALTER_TABLE_ADD_COLUMN_MODIFIERS: Self = Self(41);
    pub const SQL_FEATURE_ALTER_TABLE_ADD_STATEMENT_BEYOND_ADD_COLUMN: Self = Self(42);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_DROP_UNSUPPORTED_ACTION: Self = Self(43);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_MODIFIERS: Self = Self(44);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_SET_UNSUPPORTED_ACTION: Self = Self(45);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_COLUMN_UNSUPPORTED_ACTION: Self = Self(46);
    pub const SQL_FEATURE_ALTER_TABLE_ALTER_STATEMENT_BEYOND_ALTER_COLUMN: Self = Self(47);
    pub const SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_IF_EXISTS_SYNTAX: Self = Self(48);
    pub const SQL_FEATURE_ALTER_TABLE_DROP_COLUMN_MODIFIERS: Self = Self(49);
    pub const SQL_FEATURE_ALTER_TABLE_DROP_STATEMENT_BEYOND_DROP_COLUMN: Self = Self(50);
    pub const SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MISSING_TO: Self = Self(51);
    pub const SQL_FEATURE_ALTER_TABLE_RENAME_COLUMN_MODIFIERS: Self = Self(52);
    pub const SQL_FEATURE_ALTER_TABLE_RENAME_STATEMENT_BEYOND_RENAME_COLUMN: Self = Self(53);
    pub const SQL_FEATURE_ALTER_TABLE_UNSUPPORTED_OPERATION: Self = Self(54);
    pub const SQL_FEATURE_COLUMN_ALIAS: Self = Self(55);
    pub const SQL_FEATURE_CREATE_INDEX_IF_NOT_EXISTS_SYNTAX: Self = Self(56);
    pub const SQL_FEATURE_CREATE_INDEX_KEY_ORDERING_MODIFIERS: Self = Self(57);
    pub const SQL_FEATURE_CREATE_INDEX_MODIFIERS: Self = Self(58);
    pub const SQL_FEATURE_CREATE_STATEMENT_BEYOND_CREATE_INDEX: Self = Self(59);
    pub const SQL_FEATURE_DESCRIBE_MODIFIER: Self = Self(60);
    pub const SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_EXPECTED_CLAUSE: Self = Self(61);
    pub const SQL_FEATURE_DDL_SCHEMA_VERSION_DUPLICATE_SET_CLAUSE: Self = Self(62);
    pub const SQL_FEATURE_DROP_INDEX_MODIFIERS: Self = Self(63);
    pub const SQL_FEATURE_DROP_INDEX_IF_EXISTS_SYNTAX: Self = Self(64);
    pub const SQL_FEATURE_DROP_STATEMENT_BEYOND_DROP_INDEX: Self = Self(65);
    pub const SQL_FEATURE_EXPRESSION_INDEX_UNSUPPORTED_FUNCTION: Self = Self(66);
    pub const SQL_FEATURE_HAVING: Self = Self(67);
    pub const SQL_FEATURE_INSERT: Self = Self(68);
    pub const SQL_FEATURE_JOIN: Self = Self(69);
    pub const SQL_FEATURE_LIKE_PATTERN_BEYOND_TRAILING_PREFIX: Self = Self(70);
    pub const SQL_FEATURE_LOWER_FIELD_PREDICATE_UNSUPPORTED: Self = Self(71);
    pub const SQL_FEATURE_MULTI_STATEMENT_SQL: Self = Self(72);
    pub const SQL_FEATURE_NESTED_AGGREGATE_INPUT: Self = Self(73);
    pub const SQL_FEATURE_NESTED_PROJECTION_FUNCTION_IN_ARITHMETIC: Self = Self(74);
    pub const SQL_FEATURE_ORDER_BY_UNSUPPORTED_FORM: Self = Self(75);
    pub const SQL_FEATURE_OTHER: Self = Self(76);
    pub const SQL_FEATURE_PARAMETER_BINDING: Self = Self(77);
    pub const SQL_FEATURE_PARAMETERIZED_SCHEMA_VERSION: Self = Self(78);
    pub const SQL_FEATURE_PREDICATE_STARTS_WITH_FIRST_ARGUMENT: Self = Self(79);
    pub const SQL_FEATURE_QUOTED_IDENTIFIERS: Self = Self(80);
    pub const SQL_FEATURE_RETURNING_UNSUPPORTED_SHAPE: Self = Self(81);
    pub const SQL_FEATURE_SCALAR_FUNCTION_EXPRESSION_POSITION: Self = Self(82);
    pub const SQL_FEATURE_SCALE_TAKING_NUMERIC_FUNCTION_EXPRESSION_POSITION: Self = Self(83);
    pub const SQL_FEATURE_SEARCHED_CASE_GROUPED_ORDER_BY: Self = Self(84);
    pub const SQL_FEATURE_SHOW_COLUMNS_MODIFIERS: Self = Self(85);
    pub const SQL_FEATURE_SHOW_ENTITIES_MODIFIERS: Self = Self(86);
    pub const SQL_FEATURE_SHOW_INDEXES_MODIFIERS: Self = Self(87);
    pub const SQL_FEATURE_SHOW_MEMORY_MODIFIERS: Self = Self(88);
    pub const SQL_FEATURE_SHOW_STORES_MODIFIERS: Self = Self(89);
    pub const SQL_FEATURE_SHOW_UNSUPPORTED_COMMAND: Self = Self(90);
    pub const SQL_FEATURE_SIMPLE_CASE_EXPRESSION: Self = Self(91);
    pub const SQL_FEATURE_STANDALONE_LITERAL_PROJECTION_ITEM: Self = Self(92);
    pub const SQL_FEATURE_SUPPORTED_GROUPED_ORDER_BY_EXPRESSION_FAMILY: Self = Self(93);
    pub const SQL_FEATURE_SUPPORTED_ORDER_BY_EXPRESSION_FAMILY: Self = Self(94);
    pub const SQL_FEATURE_UNION_INTERSECT_EXCEPT: Self = Self(95);
    pub const SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE: Self = Self(96);
    pub const SQL_FEATURE_UPDATE: Self = Self(97);
    pub const SQL_FEATURE_UPPER_FIELD_PREDICATE_UNSUPPORTED: Self = Self(98);
    pub const SQL_FEATURE_WINDOW_FUNCTION: Self = Self(99);
    pub const SQL_FEATURE_WITH: Self = Self(100);
    pub const SQL_FEATURE_NUMERIC_SCALE_FUNCTION_ARGUMENTS: Self = Self(101);
    pub const SQL_FEATURE_ORDER_BY_FIELD_NOT_ORDERABLE: Self = Self(102);

    const SQL_FEATURE_DETAILS: [SqlFeatureCode; 65] = [
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
        SqlFeatureCode::NumericScaleFunctionArguments,
        SqlFeatureCode::OrderByFieldNotOrderable,
    ];

    pub const SQL_SURFACE_QUERY_REJECTS_INSERT: Self = Self(103);
    pub const SQL_SURFACE_QUERY_REJECTS_UPDATE: Self = Self(104);
    pub const SQL_SURFACE_QUERY_REJECTS_DELETE: Self = Self(105);
    pub const SQL_SURFACE_UPDATE_REJECTS_SELECT: Self = Self(106);
    pub const SQL_SURFACE_UPDATE_REJECTS_EXPLAIN: Self = Self(107);
    pub const SQL_SURFACE_UPDATE_REJECTS_DESCRIBE: Self = Self(108);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_INDEXES: Self = Self(109);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_COLUMNS: Self = Self(110);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_ENTITIES: Self = Self(111);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_STORES: Self = Self(112);
    pub const SQL_SURFACE_UPDATE_REJECTS_SHOW_MEMORY: Self = Self(113);

    pub const SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION: Self = Self(114);
    pub const SCHEMA_DDL_MISSING_NEXT_SCHEMA_VERSION: Self = Self(115);
    pub const SCHEMA_DDL_STALE_EXPECTED_SCHEMA_VERSION: Self = Self(116);
    pub const SCHEMA_DDL_INVALID_EXPECTED_SCHEMA_VERSION: Self = Self(117);
    pub const SCHEMA_DDL_INVALID_NEXT_SCHEMA_VERSION: Self = Self(118);
    pub const SCHEMA_DDL_ACCEPTED_SCHEMA_CHANGE_WITHOUT_VERSION_BUMP: Self = Self(119);
    pub const SCHEMA_DDL_EMPTY_VERSION_BUMP: Self = Self(120);
    pub const SCHEMA_DDL_VERSION_GAP: Self = Self(121);
    pub const SCHEMA_DDL_VERSION_ROLLBACK: Self = Self(122);
    pub const SCHEMA_DDL_FINGERPRINT_METHOD_MISMATCH: Self = Self(123);
    pub const SCHEMA_DDL_UNSUPPORTED_TRANSITION_CLASS: Self = Self(124);
    pub const SCHEMA_DDL_PHYSICAL_RUNNER_MISSING: Self = Self(125);
    pub const SCHEMA_DDL_VALIDATION_FAILED: Self = Self(126);
    pub const SCHEMA_DDL_PUBLICATION_RACE_LOST: Self = Self(127);
    pub const SCHEMA_DDL_INVALID_ADD_COLUMN_DEFAULT: Self = Self(128);
    pub const SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT: Self = Self(129);
    pub const SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED: Self = Self(130);
    pub const SCHEMA_DDL_REQUIRED_DROP_DEFAULT_UNSUPPORTED: Self = Self(131);
    pub const SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED: Self = Self(132);
    pub const SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED: Self = Self(133);
    pub const SCHEMA_DDL_SET_NOT_NULL_VALIDATION_FAILED: Self = Self(134);
    pub const QUERY_SQL_WRITE_BOUNDARY: Self = Self(135);
    pub const SQL_WRITE_PRIMARY_KEY_LITERAL_SHAPE: Self = Self(136);
    pub const SQL_WRITE_PRIMARY_KEY_LITERAL_INCOMPATIBLE: Self = Self(137);
    pub const SQL_WRITE_MISSING_PRIMARY_KEY: Self = Self(138);
    pub const SQL_WRITE_MISSING_REQUIRED_FIELDS: Self = Self(139);
    pub const SQL_WRITE_EXPLICIT_MANAGED_FIELD: Self = Self(140);
    pub const SQL_WRITE_EXPLICIT_GENERATED_FIELD: Self = Self(141);
    pub const SQL_WRITE_INSERT_SELECT_REQUIRES_SCALAR: Self = Self(142);
    pub const SQL_WRITE_INSERT_SELECT_AGGREGATE_PROJECTION: Self = Self(143);
    pub const SQL_WRITE_INSERT_SELECT_WIDTH_MISMATCH: Self = Self(144);
    pub const SQL_WRITE_UPDATE_PRIMARY_KEY_MUTATION: Self = Self(145);
    pub const SQL_WRITE_INVALID_FIELD_LITERAL: Self = Self(146);
    pub const SQL_WRITE_UNKNOWN_RETURNING_FIELD: Self = Self(147);
    pub const SQL_WRITE_DUPLICATE_RETURNING_FIELD: Self = Self(148);
    pub const SQL_WRITE_UPDATE_MISSING_WHERE_PREDICATE: Self = Self(149);
    pub const SQL_WRITE_ORDER_BY_UNSUPPORTED_SHAPE: Self = Self(150);
    pub const SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE: Self = Self(183);
    pub const SQL_WRITE_RETURNING_ROWS_TOO_MANY: Self = Self(184);
    pub const RUNTIME_BOUNDARY_SQL_INTROSPECTION_DISABLED: Self = Self(185);
    pub const QUERY_UNSUPPORTED_PROJECTION: Self = Self(151);
    pub const QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED: Self = Self(152);
    pub const QUERY_PROJECTION_NUMERIC_SCALE_ARGUMENTS: Self = Self(153);
    pub const QUERY_PROJECTION_NESTED_FIELD_PATH_PREVIEW: Self = Self(154);
    pub const QUERY_PROJECTION_CASE_CONDITION_BOOLEAN_REQUIRED: Self = Self(155);
    pub const QUERY_PROJECTION_NUMERIC_INPUT_REQUIRED: Self = Self(156);
    pub const QUERY_PROJECTION_TEXT_OR_BLOB_INPUT_REQUIRED: Self = Self(157);
    pub const QUERY_PROJECTION_TEXT_INPUT_REQUIRED: Self = Self(158);
    pub const QUERY_PROJECTION_TEXT_OR_NULL_ARGUMENT_REQUIRED: Self = Self(159);
    pub const QUERY_PROJECTION_INTEGER_OR_NULL_ARGUMENT_REQUIRED: Self = Self(160);
    pub const QUERY_PROJECTION_UNARY_OPERAND_INCOMPATIBLE: Self = Self(161);
    pub const QUERY_PROJECTION_BINARY_OPERANDS_INCOMPATIBLE: Self = Self(162);
    pub const QUERY_RESULT_SHAPE_MISMATCH: Self = Self(163);
    pub const QUERY_RESULT_EXPECTED_ROWS: Self = Self(164);
    pub const QUERY_RESULT_EXPECTED_GROUPED: Self = Self(165);
    pub const SQL_LOWERING_ENTITY_MISMATCH: Self = Self(166);
    pub const SQL_LOWERING_SELECT_PROJECTION_SHAPE: Self = Self(167);
    pub const SQL_LOWERING_SELECT_DISTINCT: Self = Self(168);
    pub const SQL_LOWERING_DISTINCT_ORDER_BY_PROJECTION: Self = Self(169);
    pub const SQL_LOWERING_GLOBAL_AGGREGATE_PROJECTION: Self = Self(170);
    pub const SQL_LOWERING_GLOBAL_AGGREGATE_GROUP_BY: Self = Self(171);
    pub const SQL_LOWERING_SELECT_GROUP_BY_SHAPE: Self = Self(172);
    pub const SQL_LOWERING_GROUPED_PROJECTION_EXPLICIT_LIST_REQUIRED: Self = Self(173);
    pub const SQL_LOWERING_GROUPED_PROJECTION_AGGREGATE_REQUIRED: Self = Self(174);
    pub const SQL_LOWERING_GROUPED_PROJECTION_NON_GROUP_FIELD: Self = Self(175);
    pub const SQL_LOWERING_GROUPED_PROJECTION_SCALAR_AFTER_AGGREGATE: Self = Self(176);
    pub const SQL_LOWERING_HAVING_REQUIRES_GROUP_BY: Self = Self(177);
    pub const SQL_LOWERING_SELECT_HAVING_SHAPE: Self = Self(178);
    pub const SQL_LOWERING_AGGREGATE_INPUT_EXPRESSIONS: Self = Self(179);
    pub const SQL_LOWERING_WHERE_EXPRESSION_SHAPE: Self = Self(180);
    pub const SQL_LOWERING_PARAMETER_PLACEMENT: Self = Self(181);
    pub const SQL_LOWERING_SQL_DDL_EXECUTION_UNSUPPORTED: Self = Self(182);

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
            Some(DiagnosticDetail::SqlWriteBoundary { boundary }) => {
                Self::from_sql_write_boundary(boundary)
            }
            Some(DiagnosticDetail::QueryProjection { reason }) => {
                Self::from_query_projection(reason)
            }
            Some(DiagnosticDetail::QueryResultShape { reason }) => {
                Self::from_query_result_shape(reason)
            }
            Some(DiagnosticDetail::SqlLowering { reason }) => Self::from_sql_lowering(reason),
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
            11 => DiagnosticCode::QueryUnknownAggregateTargetField,
            151..=162 => DiagnosticCode::QueryUnsupportedProjection,
            163..=165 => DiagnosticCode::QueryResultShapeMismatch,
            12 | 38..=102 | 166..=182 => DiagnosticCode::QueryUnsupportedSqlFeature,
            13 | 103..=113 => DiagnosticCode::QuerySqlSurfaceMismatch,
            14 | 114..=134 => DiagnosticCode::SchemaDdlAdmission,
            135..=150 | 183..=184 => DiagnosticCode::QuerySqlWriteBoundary,
            15 => DiagnosticCode::StoreNotFound,
            16 => DiagnosticCode::StoreCorruption,
            17 => DiagnosticCode::StoreInvariantViolation,
            18 => DiagnosticCode::RuntimeCorruption,
            19 => DiagnosticCode::RuntimeIncompatiblePersistedFormat,
            20 => DiagnosticCode::RuntimeInvariantViolation,
            21 => DiagnosticCode::RuntimeConflict,
            22 => DiagnosticCode::RuntimeNotFound,
            23 | 25..=37 | 185 => DiagnosticCode::RuntimeUnsupported,
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
            18..=24 => Self::runtime_kind_detail(self.raw()),
            25..=37 | 185 => Self::runtime_boundary_detail(self.raw()),
            38..=102 => Self::sql_feature_detail(self.raw()),
            103..=113 => Self::sql_surface_detail(self.raw()),
            114..=134 => Self::schema_ddl_detail(self.raw()),
            136..=150 | 183..=184 => Self::sql_write_boundary_detail(self.raw()),
            152..=162 => Self::query_projection_detail(self.raw()),
            164..=165 => Self::query_result_shape_detail(self.raw()),
            166..=182 => Self::sql_lowering_detail(self.raw()),
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
        match boundary {
            RuntimeBoundaryCode::SqlIntrospectionDisabled => {
                Self::RUNTIME_BOUNDARY_SQL_INTROSPECTION_DISABLED
            }
            _ => {
                Self(Self::RUNTIME_BOUNDARY_SQL_SURFACE_CONTROLLER_REQUIRED.raw() + boundary as u16)
            }
        }
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

    const fn from_sql_write_boundary(boundary: SqlWriteBoundaryCode) -> Self {
        match boundary {
            SqlWriteBoundaryCode::ReturningResponseTooLarge => {
                Self::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE
            }
            SqlWriteBoundaryCode::ReturningRowsTooMany => Self::SQL_WRITE_RETURNING_ROWS_TOO_MANY,
            _ => Self(Self::SQL_WRITE_PRIMARY_KEY_LITERAL_SHAPE.raw() + boundary as u16),
        }
    }

    const fn from_query_projection(reason: QueryProjectionCode) -> Self {
        Self(Self::QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED.raw() + reason as u16)
    }

    const fn from_query_result_shape(reason: QueryResultShapeCode) -> Self {
        Self(Self::QUERY_RESULT_EXPECTED_ROWS.raw() + reason as u16)
    }

    const fn from_sql_lowering(reason: SqlLoweringCode) -> Self {
        Self(Self::SQL_LOWERING_ENTITY_MISMATCH.raw() + reason as u16)
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
            18 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Corruption,
            }),
            19 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::IncompatiblePersistedFormat,
            }),
            20 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::InvariantViolation,
            }),
            21 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Conflict,
            }),
            22 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::NotFound,
            }),
            23 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Unsupported,
            }),
            24 => Some(DiagnosticDetail::RuntimeKind {
                kind: RuntimeErrorKind::Internal,
            }),
            _ => None,
        }
    }

    const fn runtime_boundary_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            25 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlSurfaceControllerRequired,
            }),
            26 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SchemaSurfaceControllerRequired,
            }),
            27 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
            }),
            28 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlQueryEntityNotConfigured,
            }),
            29 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlDdlTargetRequired,
            }),
            30 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlDdlEntityNotConfigured,
            }),
            31 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::QueryResponseRowsRequired,
            }),
            32 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::QueryResponseGroupedRowsRequired,
            }),
            33 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultEntityRequired,
            }),
            34 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultEntitiesRequired,
            }),
            35 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultIdRequired,
            }),
            36 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::MutationResultIdsRequired,
            }),
            37 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::RowProjectionFieldNotConfigured,
            }),
            185 => Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::SqlIntrospectionDisabled,
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
            103 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::QueryRejectsInsert,
            }),
            104 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::QueryRejectsUpdate,
            }),
            105 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::QueryRejectsDelete,
            }),
            106 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsSelect,
            }),
            107 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsExplain,
            }),
            108 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsDescribe,
            }),
            109 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowIndexes,
            }),
            110 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowColumns,
            }),
            111 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowEntities,
            }),
            112 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowStores,
            }),
            113 => Some(DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: SqlSurfaceMismatchCode::UpdateRejectsShowMemory,
            }),
            _ => None,
        }
    }

    const fn schema_ddl_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            114 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::MissingExpectedSchemaVersion,
            }),
            115 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::MissingNextSchemaVersion,
            }),
            116 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::StaleExpectedSchemaVersion,
            }),
            117 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion,
            }),
            118 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidNextSchemaVersion,
            }),
            119 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump,
            }),
            120 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::EmptyVersionBump,
            }),
            121 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::VersionGap,
            }),
            122 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::VersionRollback,
            }),
            123 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::FingerprintMethodMismatch,
            }),
            124 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::UnsupportedTransitionClass,
            }),
            125 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::PhysicalRunnerMissing,
            }),
            126 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::ValidationFailed,
            }),
            127 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::PublicationRaceLost,
            }),
            128 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidAddColumnDefault,
            }),
            129 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::InvalidAlterColumnDefault,
            }),
            130 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::GeneratedIndexDropRejected,
            }),
            131 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::RequiredDropDefaultUnsupported,
            }),
            132 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected,
            }),
            133 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected,
            }),
            134 => Some(DiagnosticDetail::SchemaDdlAdmission {
                reason: SchemaDdlAdmissionCode::SetNotNullValidationFailed,
            }),
            _ => None,
        }
    }

    const fn sql_write_boundary_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            136 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::PrimaryKeyLiteralShape,
            }),
            137 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible,
            }),
            138 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::MissingPrimaryKey,
            }),
            139 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::MissingRequiredFields,
            }),
            140 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::ExplicitManagedField,
            }),
            141 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::ExplicitGeneratedField,
            }),
            142 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::InsertSelectRequiresScalar,
            }),
            143 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::InsertSelectAggregateProjection,
            }),
            144 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::InsertSelectWidthMismatch,
            }),
            145 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::UpdatePrimaryKeyMutation,
            }),
            146 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::InvalidFieldLiteral,
            }),
            147 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::UnknownReturningField,
            }),
            148 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::DuplicateReturningField,
            }),
            149 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::UpdateMissingWherePredicate,
            }),
            150 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::WriteOrderByUnsupportedShape,
            }),
            183 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::ReturningResponseTooLarge,
            }),
            184 => Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::ReturningRowsTooMany,
            }),
            _ => None,
        }
    }

    const fn query_projection_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            152 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::NumericLiteralRequired,
            }),
            153 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::NumericScaleArguments,
            }),
            154 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::NestedFieldPathPreview,
            }),
            155 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::CaseConditionBooleanRequired,
            }),
            156 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::NumericInputRequired,
            }),
            157 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::TextOrBlobInputRequired,
            }),
            158 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::TextInputRequired,
            }),
            159 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::TextOrNullArgumentRequired,
            }),
            160 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::IntegerOrNullArgumentRequired,
            }),
            161 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::UnaryOperandIncompatible,
            }),
            162 => Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::BinaryOperandsIncompatible,
            }),
            _ => None,
        }
    }

    const fn query_result_shape_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            164 => Some(DiagnosticDetail::QueryResultShape {
                reason: QueryResultShapeCode::ExpectedRows,
            }),
            165 => Some(DiagnosticDetail::QueryResultShape {
                reason: QueryResultShapeCode::ExpectedGroupedRows,
            }),
            _ => None,
        }
    }

    const fn sql_lowering_detail(raw: u16) -> Option<DiagnosticDetail> {
        match raw {
            166 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::EntityMismatch,
            }),
            167 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::SelectProjectionShape,
            }),
            168 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::SelectDistinct,
            }),
            169 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::DistinctOrderByProjection,
            }),
            170 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::GlobalAggregateProjection,
            }),
            171 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::GlobalAggregateGroupBy,
            }),
            172 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::SelectGroupByShape,
            }),
            173 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::GroupedProjectionExplicitListRequired,
            }),
            174 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::GroupedProjectionAggregateRequired,
            }),
            175 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::GroupedProjectionNonGroupField,
            }),
            176 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::GroupedProjectionScalarAfterAggregate,
            }),
            177 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::HavingRequiresGroupBy,
            }),
            178 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::SelectHavingShape,
            }),
            179 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::AggregateInputExpressions,
            }),
            180 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::WhereExpressionShape,
            }),
            181 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::ParameterPlacement,
            }),
            182 => Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::SqlDdlExecutionUnsupported,
            }),
            _ => None,
        }
    }
}

impl fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, self.raw())
    }
}

///
/// ErrorClass
///
/// Broad diagnostic class used for recovery decisions.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
/// QueryProjectionCode
///
/// Compact query projection admission/runtime identifier.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum QueryProjectionCode {
    NumericLiteralRequired,
    NumericScaleArguments,
    NestedFieldPathPreview,
    CaseConditionBooleanRequired,
    NumericInputRequired,
    TextOrBlobInputRequired,
    TextInputRequired,
    TextOrNullArgumentRequired,
    IntegerOrNullArgumentRequired,
    UnaryOperandIncompatible,
    BinaryOperandsIncompatible,
}

///
/// QueryResultShapeCode
///
/// Compact query-result shape mismatch identifier.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum QueryResultShapeCode {
    ExpectedRows,
    ExpectedGroupedRows,
}

///
/// RuntimeErrorKind
///
/// Public runtime error category.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
    SqlIntrospectionDisabled,
}

///
/// SqlFeatureCode
///
/// Compact SQL feature identifier used by unsupported-feature diagnostics.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
    NumericScaleFunctionArguments,
    OrderByFieldNotOrderable,
}

///
/// SqlLoweringCode
///
/// Compact SQL lowering rejection identifier used after parsing succeeds but
/// before a statement becomes canonical query intent.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum SqlLoweringCode {
    EntityMismatch,
    SelectProjectionShape,
    SelectDistinct,
    DistinctOrderByProjection,
    GlobalAggregateProjection,
    GlobalAggregateGroupBy,
    SelectGroupByShape,
    GroupedProjectionExplicitListRequired,
    GroupedProjectionAggregateRequired,
    GroupedProjectionNonGroupField,
    GroupedProjectionScalarAfterAggregate,
    HavingRequiresGroupBy,
    SelectHavingShape,
    AggregateInputExpressions,
    WhereExpressionShape,
    ParameterPlacement,
    SqlDdlExecutionUnsupported,
}

///
/// SqlSurfaceMismatchCode
///
/// Compact SQL endpoint surface mismatch identifier.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
/// SqlWriteBoundaryCode
///
/// Compact SQL write fail-closed boundary identifier.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum SqlWriteBoundaryCode {
    PrimaryKeyLiteralShape,
    PrimaryKeyLiteralIncompatible,
    MissingPrimaryKey,
    MissingRequiredFields,
    ExplicitManagedField,
    ExplicitGeneratedField,
    InsertSelectRequiresScalar,
    InsertSelectAggregateProjection,
    InsertSelectWidthMismatch,
    UpdatePrimaryKeyMutation,
    InvalidFieldLiteral,
    UnknownReturningField,
    DuplicateReturningField,
    UpdateMissingWherePredicate,
    WriteOrderByUnsupportedShape,
    ReturningResponseTooLarge,
    ReturningRowsTooMany,
}

///
/// SchemaDdlAdmissionCode
///
/// Compact SQL DDL admission rejection reason.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum DiagnosticDetail {
    QueryKind { kind: QueryErrorKind },
    RuntimeKind { kind: RuntimeErrorKind },
    RuntimeBoundary { boundary: RuntimeBoundaryCode },
    SchemaDdlAdmission { reason: SchemaDdlAdmissionCode },
    UnsupportedSqlFeature { feature: SqlFeatureCode },
    SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode },
    SqlWriteBoundary { boundary: SqlWriteBoundaryCode },
    QueryProjection { reason: QueryProjectionCode },
    QueryResultShape { reason: QueryResultShapeCode },
    SqlLowering { reason: SqlLoweringCode },
}

///
/// Diagnostic
///
/// Compact public diagnostic payload.
///

#[derive(Clone, Eq, PartialEq)]
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

impl fmt::Debug for DiagnosticCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, self.error_code().raw())
    }
}

impl fmt::Debug for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for QueryErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for QueryProjectionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for QueryResultShapeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for RuntimeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for RuntimeBoundaryCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for SqlFeatureCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for SqlLoweringCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for SqlSurfaceMismatchCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for SqlWriteBoundaryCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for SchemaDdlAdmissionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

impl fmt::Debug for DiagnosticDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(
            f,
            ErrorCode::from_parts(DiagnosticCode::RuntimeInternal, Some(*self)).raw(),
        )
    }
}

impl fmt::Debug for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.error_code().raw(), self.origin as u16)
    }
}

fn fmt_compact_code(f: &mut fmt::Formatter<'_>, raw: u16) -> fmt::Result {
    write!(f, "{raw}")
}

#[cfg(test)]
mod tests {
    use super::{
        Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorClass, ErrorCode, ErrorOrigin,
        QueryProjectionCode, SqlFeatureCode, SqlLoweringCode, SqlWriteBoundaryCode,
    };

    const ORDERED_ERROR_CODES: [ErrorCode; 185] = [
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
        ErrorCode::QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD,
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
        ErrorCode::SQL_FEATURE_NUMERIC_SCALE_FUNCTION_ARGUMENTS,
        ErrorCode::SQL_FEATURE_ORDER_BY_FIELD_NOT_ORDERABLE,
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
        ErrorCode::QUERY_SQL_WRITE_BOUNDARY,
        ErrorCode::SQL_WRITE_PRIMARY_KEY_LITERAL_SHAPE,
        ErrorCode::SQL_WRITE_PRIMARY_KEY_LITERAL_INCOMPATIBLE,
        ErrorCode::SQL_WRITE_MISSING_PRIMARY_KEY,
        ErrorCode::SQL_WRITE_MISSING_REQUIRED_FIELDS,
        ErrorCode::SQL_WRITE_EXPLICIT_MANAGED_FIELD,
        ErrorCode::SQL_WRITE_EXPLICIT_GENERATED_FIELD,
        ErrorCode::SQL_WRITE_INSERT_SELECT_REQUIRES_SCALAR,
        ErrorCode::SQL_WRITE_INSERT_SELECT_AGGREGATE_PROJECTION,
        ErrorCode::SQL_WRITE_INSERT_SELECT_WIDTH_MISMATCH,
        ErrorCode::SQL_WRITE_UPDATE_PRIMARY_KEY_MUTATION,
        ErrorCode::SQL_WRITE_INVALID_FIELD_LITERAL,
        ErrorCode::SQL_WRITE_UNKNOWN_RETURNING_FIELD,
        ErrorCode::SQL_WRITE_DUPLICATE_RETURNING_FIELD,
        ErrorCode::SQL_WRITE_UPDATE_MISSING_WHERE_PREDICATE,
        ErrorCode::SQL_WRITE_ORDER_BY_UNSUPPORTED_SHAPE,
        ErrorCode::QUERY_UNSUPPORTED_PROJECTION,
        ErrorCode::QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED,
        ErrorCode::QUERY_PROJECTION_NUMERIC_SCALE_ARGUMENTS,
        ErrorCode::QUERY_PROJECTION_NESTED_FIELD_PATH_PREVIEW,
        ErrorCode::QUERY_PROJECTION_CASE_CONDITION_BOOLEAN_REQUIRED,
        ErrorCode::QUERY_PROJECTION_NUMERIC_INPUT_REQUIRED,
        ErrorCode::QUERY_PROJECTION_TEXT_OR_BLOB_INPUT_REQUIRED,
        ErrorCode::QUERY_PROJECTION_TEXT_INPUT_REQUIRED,
        ErrorCode::QUERY_PROJECTION_TEXT_OR_NULL_ARGUMENT_REQUIRED,
        ErrorCode::QUERY_PROJECTION_INTEGER_OR_NULL_ARGUMENT_REQUIRED,
        ErrorCode::QUERY_PROJECTION_UNARY_OPERAND_INCOMPATIBLE,
        ErrorCode::QUERY_PROJECTION_BINARY_OPERANDS_INCOMPATIBLE,
        ErrorCode::QUERY_RESULT_SHAPE_MISMATCH,
        ErrorCode::QUERY_RESULT_EXPECTED_ROWS,
        ErrorCode::QUERY_RESULT_EXPECTED_GROUPED,
        ErrorCode::SQL_LOWERING_ENTITY_MISMATCH,
        ErrorCode::SQL_LOWERING_SELECT_PROJECTION_SHAPE,
        ErrorCode::SQL_LOWERING_SELECT_DISTINCT,
        ErrorCode::SQL_LOWERING_DISTINCT_ORDER_BY_PROJECTION,
        ErrorCode::SQL_LOWERING_GLOBAL_AGGREGATE_PROJECTION,
        ErrorCode::SQL_LOWERING_GLOBAL_AGGREGATE_GROUP_BY,
        ErrorCode::SQL_LOWERING_SELECT_GROUP_BY_SHAPE,
        ErrorCode::SQL_LOWERING_GROUPED_PROJECTION_EXPLICIT_LIST_REQUIRED,
        ErrorCode::SQL_LOWERING_GROUPED_PROJECTION_AGGREGATE_REQUIRED,
        ErrorCode::SQL_LOWERING_GROUPED_PROJECTION_NON_GROUP_FIELD,
        ErrorCode::SQL_LOWERING_GROUPED_PROJECTION_SCALAR_AFTER_AGGREGATE,
        ErrorCode::SQL_LOWERING_HAVING_REQUIRES_GROUP_BY,
        ErrorCode::SQL_LOWERING_SELECT_HAVING_SHAPE,
        ErrorCode::SQL_LOWERING_AGGREGATE_INPUT_EXPRESSIONS,
        ErrorCode::SQL_LOWERING_WHERE_EXPRESSION_SHAPE,
        ErrorCode::SQL_LOWERING_PARAMETER_PLACEMENT,
        ErrorCode::SQL_LOWERING_SQL_DDL_EXECUTION_UNSUPPORTED,
        ErrorCode::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE,
        ErrorCode::SQL_WRITE_RETURNING_ROWS_TOO_MANY,
        ErrorCode::RUNTIME_BOUNDARY_SQL_INTROSPECTION_DISABLED,
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

    #[test]
    fn public_error_codes_reconstruct_shifted_details() {
        assert_eq!(
            ErrorCode::QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD.diagnostic_code(),
            DiagnosticCode::QueryUnknownAggregateTargetField
        );
        assert_eq!(
            ErrorCode::SQL_FEATURE_JOIN.diagnostic_detail(),
            Some(DiagnosticDetail::UnsupportedSqlFeature {
                feature: SqlFeatureCode::Join,
            })
        );
        assert_eq!(
            ErrorCode::QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED.diagnostic_detail(),
            Some(DiagnosticDetail::QueryProjection {
                reason: QueryProjectionCode::NumericLiteralRequired,
            })
        );
        assert_eq!(
            ErrorCode::SQL_LOWERING_DISTINCT_ORDER_BY_PROJECTION.diagnostic_detail(),
            Some(DiagnosticDetail::SqlLowering {
                reason: SqlLoweringCode::DistinctOrderByProjection,
            })
        );
        assert_eq!(
            ErrorCode::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE.diagnostic_detail(),
            Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::ReturningResponseTooLarge,
            })
        );
        assert_eq!(
            ErrorCode::SQL_WRITE_RETURNING_ROWS_TOO_MANY.diagnostic_detail(),
            Some(DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::ReturningRowsTooMany,
            })
        );
    }
}
