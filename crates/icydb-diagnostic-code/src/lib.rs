//! Module: lib
//! Responsibility: compact diagnostic identity and public numeric error-code mapping.
//! Does not own: rich diagnostic prose, Candid wire types, or runtime error construction.
//! Boundary: maps rich internal diagnostic categories to stable compact public codes.
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

#[remain::sorted]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum DiagnosticCode {
    QueryAccessRequirement,
    QueryIntent,
    QueryInvalidContinuationCursor,
    QueryNotFound,
    QueryNotUnique,
    QueryNumericNotRepresentable,
    QueryNumericOverflow,
    QueryPlan,
    QueryReadAdmission,
    QueryResultShapeMismatch,
    QuerySqlSurfaceMismatch,
    QuerySqlWriteBoundary,
    QueryUnknownAggregateTargetField,
    QueryUnorderedPagination,
    QueryUnsupportedProjection,
    QueryUnsupportedSqlFeature,
    QueryValidate,
    RuntimeConflict,
    RuntimeCorruption,
    RuntimeIncompatiblePersistedFormat,
    RuntimeInternal,
    RuntimeInvariantViolation,
    RuntimeNotFound,
    RuntimeUnsupported,
    SchemaDdlAdmission,
    StoreCorruption,
    StoreInvariantViolation,
    StoreNotFound,
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
            | Self::QueryReadAdmission
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
            | Self::QueryReadAdmission
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
            Self::QueryReadAdmission => ErrorCode::QUERY_READ_ADMISSION,
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

impl fmt::Debug for DiagnosticCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, self.error_code().raw())
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

mod registry;

impl ErrorCode {
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

#[remain::sorted]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum ErrorClass {
    Conflict,
    Corruption,
    IncompatiblePersistedFormat,
    Internal,
    InvariantViolation,
    NotFound,
    Query,
    Unsupported,
}

impl ErrorClass {
    /// Return the compact public wire code for this diagnostic class.
    #[must_use]
    pub const fn wire_code(self) -> u8 {
        match self {
            Self::Query => 1,
            Self::Corruption => 2,
            Self::IncompatiblePersistedFormat => 3,
            Self::NotFound => 4,
            Self::Internal => 5,
            Self::Conflict => 6,
            Self::Unsupported => 7,
            Self::InvariantViolation => 8,
        }
    }

    /// Recover a diagnostic class from its compact public wire code.
    #[must_use]
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Query),
            2 => Some(Self::Corruption),
            3 => Some(Self::IncompatiblePersistedFormat),
            4 => Some(Self::NotFound),
            5 => Some(Self::Internal),
            6 => Some(Self::Conflict),
            7 => Some(Self::Unsupported),
            8 => Some(Self::InvariantViolation),
            _ => None,
        }
    }
}

impl fmt::Debug for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, u16::from(self.wire_code()))
    }
}

///
/// ErrorOrigin
///
/// Subsystem that owns the diagnostic.
///

#[remain::sorted]
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

impl ErrorOrigin {
    /// Return the compact public wire code for this diagnostic origin.
    #[must_use]
    pub const fn wire_code(self) -> u8 {
        match self {
            Self::Cursor => 1,
            Self::Executor => 2,
            Self::Identity => 3,
            Self::Index => 4,
            Self::Interface => 5,
            Self::Planner => 6,
            Self::Query => 7,
            Self::Recovery => 8,
            Self::Response => 9,
            Self::Runtime => 10,
            Self::Serialize => 11,
            Self::Store => 12,
        }
    }

    /// Recover a known diagnostic origin from its compact public wire code.
    #[must_use]
    pub const fn from_known_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Cursor),
            2 => Some(Self::Executor),
            3 => Some(Self::Identity),
            4 => Some(Self::Index),
            5 => Some(Self::Interface),
            6 => Some(Self::Planner),
            7 => Some(Self::Query),
            8 => Some(Self::Recovery),
            9 => Some(Self::Response),
            10 => Some(Self::Runtime),
            11 => Some(Self::Serialize),
            12 => Some(Self::Store),
            _ => None,
        }
    }

    /// Recover a diagnostic origin from its compact public wire code.
    ///
    /// Unknown origin codes fail closed to `Runtime`, matching the public
    /// boundary behavior used by the Candid facade.
    #[must_use]
    pub const fn from_wire_code(code: u8) -> Self {
        match Self::from_known_wire_code(code) {
            Some(origin) => origin,
            None => Self::Runtime,
        }
    }
}

impl fmt::Debug for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, u16::from(self.wire_code()))
    }
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

impl fmt::Debug for QueryErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// QueryProjectionCode
///
/// Compact query projection admission/runtime identifier.
/// Variant order is wire-order significant for public error-code offsets.
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

impl fmt::Debug for QueryProjectionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// QueryReadAdmissionCode
///
/// Compact read-admission rejection identifier.
/// Variant order is wire-order significant for public error-code offsets.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum QueryReadAdmissionCode {
    PublicQueryRequiresLimit,
    PublicQueryRequiresIndex,
    UnboundedFullScanRejected,
    SortRequiresMaterialization,
    GroupedQueryRequiresLimits,
    GroupedQueryExceedsBudget,
    DiagnosticLaneDoesNotExecute,
    ReturnedRowBoundExceedsPolicy,
    PrimaryKeyInputExceedsPolicy,
}

impl fmt::Debug for QueryReadAdmissionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// QueryResultShapeCode
///
/// Compact query-result shape mismatch identifier.
/// Variant order is wire-order significant for public error-code offsets.
///

#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum QueryResultShapeCode {
    ExpectedRows,
    ExpectedGroupedRows,
}

impl fmt::Debug for QueryResultShapeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
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

impl fmt::Debug for RuntimeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// RuntimeBoundaryCode
///
/// Compact public-runtime boundary identifier.
/// Variant order is wire-order significant for public error-code offsets.
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
    RowProjectionFieldNotConfigured,
    SqlIntrospectionDisabled,
    /// A complete accepted mutation omitted a required field.
    MutationRequiredFieldMissing,
    /// A persisted row's stamp falls outside the accepted layout window.
    PersistedRowLayoutOutsideAcceptedWindow,
    /// A persisted row's physical slot count disagrees with its layout stamp.
    PersistedRowSlotCountMismatch,
    /// A generated field would collide with an accepted DDL-owned slot.
    GeneratedFieldAfterDdlField,
}

impl fmt::Debug for RuntimeBoundaryCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// SqlFeatureCode
///
/// Compact SQL feature identifier used by unsupported-feature diagnostics.
/// Variant order is wire-order significant for public error-code offsets.
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

impl fmt::Debug for SqlFeatureCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// SqlLoweringCode
///
/// Compact SQL lowering rejection identifier used after parsing succeeds but
/// before a statement becomes canonical query intent.
/// Variant order is wire-order significant for public error-code offsets.
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

impl fmt::Debug for SqlLoweringCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// SqlSurfaceMismatchCode
///
/// Compact SQL endpoint surface mismatch identifier.
/// Variant order is wire-order significant for public error-code offsets.
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

impl fmt::Debug for SqlSurfaceMismatchCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// SqlWriteBoundaryCode
///
/// Compact SQL write fail-closed boundary identifier.
/// Variant order is wire-order significant for public error-code offsets.
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
    StagedRowsTooMany,
    InsertDefaultRequiredField,
    UpdateDefaultRequiredField,
    UpdateDefaultDatabaseOwnedField,
}

impl fmt::Debug for SqlWriteBoundaryCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// SchemaDdlAdmissionCode
///
/// Compact SQL DDL admission rejection reason.
/// Variant order is wire-order significant for public error-code offsets.
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
    SchemaRewriteRequiresMigration,
    SchemaTransitionBudgetExceeded,
    GeneratedFieldDefaultChangeRejected,
    GeneratedFieldNullabilityChangeRejected,
    SetNotNullValidationFailed,
    RowLayoutVersionExhausted,
}

impl fmt::Debug for SchemaDdlAdmissionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

///
/// DiagnosticDetail
///
/// Small structured diagnostic payload for callers and CLI rendering.
///

#[remain::sorted]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum DiagnosticDetail {
    QueryKind { kind: QueryErrorKind },
    QueryProjection { reason: QueryProjectionCode },
    QueryReadAdmission { reason: QueryReadAdmissionCode },
    QueryResultShape { reason: QueryResultShapeCode },
    RuntimeBoundary { boundary: RuntimeBoundaryCode },
    RuntimeKind { kind: RuntimeErrorKind },
    SchemaDdlAdmission { reason: SchemaDdlAdmissionCode },
    SqlLowering { reason: SqlLoweringCode },
    SqlSurfaceMismatch { mismatch: SqlSurfaceMismatchCode },
    SqlWriteBoundary { boundary: SqlWriteBoundaryCode },
    UnsupportedSqlFeature { feature: SqlFeatureCode },
}

impl fmt::Debug for DiagnosticDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(
            f,
            ErrorCode::from_parts(self.diagnostic_code(), Some(*self)).raw(),
        )
    }
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

impl fmt::Debug for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.error_code().raw(), self.origin.wire_code())
    }
}

fn fmt_compact_code(f: &mut fmt::Formatter<'_>, raw: u16) -> fmt::Result {
    write!(f, "{raw}")
}

#[cfg(test)]
mod tests {
    use super::{
        Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorClass, ErrorCode, ErrorOrigin,
        QueryProjectionCode, QueryReadAdmissionCode, SqlFeatureCode, SqlLoweringCode,
        SqlWriteBoundaryCode,
        registry::{DETAIL_ERROR_CODES, ORDERED_ERROR_CODES},
    };

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
    fn class_and_origin_wire_codes_round_trip() {
        for (class, raw) in [
            (ErrorClass::Query, 1),
            (ErrorClass::Corruption, 2),
            (ErrorClass::IncompatiblePersistedFormat, 3),
            (ErrorClass::NotFound, 4),
            (ErrorClass::Internal, 5),
            (ErrorClass::Conflict, 6),
            (ErrorClass::Unsupported, 7),
            (ErrorClass::InvariantViolation, 8),
        ] {
            assert_eq!(class.wire_code(), raw);
            assert_eq!(ErrorClass::from_wire_code(raw), Some(class));
            assert_eq!(format!("{class:?}"), raw.to_string());
        }

        for (origin, raw) in [
            (ErrorOrigin::Cursor, 1),
            (ErrorOrigin::Executor, 2),
            (ErrorOrigin::Identity, 3),
            (ErrorOrigin::Index, 4),
            (ErrorOrigin::Interface, 5),
            (ErrorOrigin::Planner, 6),
            (ErrorOrigin::Query, 7),
            (ErrorOrigin::Recovery, 8),
            (ErrorOrigin::Response, 9),
            (ErrorOrigin::Runtime, 10),
            (ErrorOrigin::Serialize, 11),
            (ErrorOrigin::Store, 12),
        ] {
            assert_eq!(origin.wire_code(), raw);
            assert_eq!(ErrorOrigin::from_known_wire_code(raw), Some(origin));
            assert_eq!(ErrorOrigin::from_wire_code(raw), origin);
            assert_eq!(format!("{origin:?}"), raw.to_string());
        }

        assert_eq!(ErrorClass::from_wire_code(0), None);
        assert_eq!(ErrorOrigin::from_known_wire_code(0), None);
        assert_eq!(ErrorOrigin::from_wire_code(0), ErrorOrigin::Runtime);
    }

    #[test]
    fn public_error_codes_are_sequential() {
        let first = ORDERED_ERROR_CODES
            .first()
            .expect("public error-code registry is non-empty")
            .raw();

        assert_eq!(first, 1);

        for (index, code) in ORDERED_ERROR_CODES.iter().enumerate() {
            let expected = first + u16::try_from(index).expect("test error-code index fits u16");
            assert_eq!(code.raw(), expected);
            assert_eq!(ErrorCode::known(code.raw()), Some(*code));
            assert!(code.is_known());
        }

        let last = ORDERED_ERROR_CODES
            .last()
            .expect("public error-code registry is non-empty")
            .raw();

        assert_eq!(last, 199);
    }

    #[test]
    fn all_public_error_codes_round_trip_through_diagnostic_parts() {
        let first = ORDERED_ERROR_CODES
            .first()
            .expect("public error-code registry is non-empty")
            .raw();
        let last = ORDERED_ERROR_CODES
            .last()
            .expect("public error-code registry is non-empty")
            .raw();

        for raw in first..=last {
            let code = ErrorCode::from_raw(raw);
            let diagnostic_code = code.diagnostic_code();
            let diagnostic_detail = code.diagnostic_detail();
            let rebuilt = ErrorCode::from_parts(diagnostic_code, diagnostic_detail);

            assert_eq!(rebuilt.raw(), raw);

            let diagnostic = code.diagnostic(ErrorOrigin::Runtime);

            assert_eq!(diagnostic.code(), diagnostic_code);
            assert_eq!(diagnostic.detail(), diagnostic_detail.as_ref());
            assert_eq!(diagnostic.error_code().raw(), raw);
        }
    }

    #[test]
    fn invalid_raw_error_codes_fail_closed_to_runtime_internal() {
        for raw in [0, 203, u16::MAX] {
            let code = ErrorCode::from_raw(raw);

            assert_eq!(ErrorCode::known(raw), None);
            assert!(!code.is_known());
            assert_eq!(code.diagnostic_code(), DiagnosticCode::RuntimeInternal);
            assert_eq!(code.diagnostic_detail(), None);
            assert_eq!(code.class(), ErrorClass::Internal);

            let diagnostic = code.diagnostic(ErrorOrigin::Query);

            assert_eq!(diagnostic.code(), DiagnosticCode::RuntimeInternal);
            assert_eq!(diagnostic.origin(), ErrorOrigin::Query);
            assert_eq!(diagnostic.detail(), None);
            assert_eq!(diagnostic.error_code(), ErrorCode::RUNTIME_INTERNAL);
        }
    }

    #[test]
    fn from_parts_requires_detail_to_match_broad_code() {
        let detail = Some(DiagnosticDetail::UnsupportedSqlFeature {
            feature: SqlFeatureCode::Join,
        });

        assert_eq!(
            ErrorCode::from_parts(DiagnosticCode::QueryUnsupportedSqlFeature, detail),
            ErrorCode::SQL_FEATURE_JOIN
        );
        assert_eq!(
            ErrorCode::from_parts(DiagnosticCode::QueryPlan, detail),
            ErrorCode::QUERY_PLAN
        );
    }

    #[test]
    fn detail_bearing_registry_entries_round_trip_directly() {
        assert!(!DETAIL_ERROR_CODES.is_empty());

        for &(code, diagnostic_code, detail) in DETAIL_ERROR_CODES {
            assert_eq!(ErrorCode::from_parts(diagnostic_code, Some(detail)), code);
            assert_eq!(code.diagnostic_code(), diagnostic_code);
            assert_eq!(code.diagnostic_detail(), Some(detail));
            assert_eq!(detail.diagnostic_code(), diagnostic_code);
        }
    }

    #[test]
    fn diagnostic_detail_reports_generated_broad_code() {
        let detail = DiagnosticDetail::UnsupportedSqlFeature {
            feature: SqlFeatureCode::Join,
        };

        assert_eq!(
            detail.diagnostic_code(),
            DiagnosticCode::QueryUnsupportedSqlFeature
        );
        assert_eq!(format!("{detail:?}"), "65");
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
            ErrorCode::QUERY_READ_PUBLIC_REQUIRES_LIMIT.diagnostic_detail(),
            Some(DiagnosticDetail::QueryReadAdmission {
                reason: QueryReadAdmissionCode::PublicQueryRequiresLimit,
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
