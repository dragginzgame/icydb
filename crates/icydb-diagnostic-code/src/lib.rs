//! Compact diagnostic identity for IcyDB.
//!
//! This crate intentionally contains no rich diagnostic prose. Production
//! canister builds can depend on these codes and structured details without
//! linking CLI-oriented message text.

///
/// DiagnosticCode
///
/// Stable machine-readable diagnostic reason.
///

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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
}

///
/// ErrorClass
///
/// Broad diagnostic class used for recovery decisions.
///

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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
}

///
/// DiagnosticDetail
///
/// Small structured diagnostic payload for callers and CLI rendering.
///

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
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

#[cfg_attr(
    feature = "wire",
    derive(candid::CandidType, serde::Deserialize, serde::Serialize)
)]
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
}

#[cfg(test)]
mod tests {
    use super::{Diagnostic, DiagnosticCode, ErrorClass, ErrorOrigin};

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
}
