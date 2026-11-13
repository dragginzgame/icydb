//! Module: lib
//! Responsibility: compact diagnostic identity and public numeric error-code mapping.
//! Does not own: rich diagnostic prose, Wire wire types, or runtime error construction.
//! Boundary: maps rich internal diagnostic categories to stable compact public codes.
//!
//! This crate intentionally contains no rich diagnostic prose or Wire wire
//! types. Production canister builds collapse diagnostics to numeric wire
//! codes before they cross the public canister boundary. `Debug` output is
//! numeric for the same reason: host tooling can recover labels from the code
//! table without making every wasm canister retain those labels.

use std::fmt;

mod fact;
mod query_field;

pub use fact::{
    DiagnosticAggregateKind, DiagnosticBacklogResource, DiagnosticComponentKind,
    DiagnosticConstraintContext, DiagnosticConstraintKind, DiagnosticDecodeReason,
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
    DiagnosticFactSchemaMismatch, DiagnosticFactTag, DiagnosticFunctionKind,
    DiagnosticGroupBudgetResource, DiagnosticMutationOperation, DiagnosticOperatorKind,
    DiagnosticTypeFamily, MAX_PUBLIC_DIAGNOSTIC_FACTS, pack_u32_pair, unpack_u32_pair,
    validate_known_diagnostic_fact_schema, validate_raw_diagnostic_fact_schema,
};
pub use query_field::{
    MAX_PUBLIC_QUERY_FIELD_BYTES, QueryFieldRole, QueryFieldSchemaMismatch,
    validate_query_field_schema,
};

// Keep broad code identity and its default projections in one declaration.
// Detailed wire codes remain owned by the numeric registry below.
macro_rules! diagnostic_codes {
    ($($(#[$meta:meta])* $variant:ident => ($class:ident, $origin:ident, $code:ident)),+ $(,)?) => {
        /// Stable machine-readable diagnostic reason.
        #[remain::sorted]
        #[derive(Clone, Copy, Eq, Hash, PartialEq)]
        pub enum DiagnosticCode {
            $($(#[$meta])*
                #[doc = concat!("Maps to [`ErrorCode::", stringify!($code), "`] with class [`ErrorClass::", stringify!($class), "`] and default origin [`ErrorOrigin::", stringify!($origin), "`].")]
                $variant),+
        }

        impl DiagnosticCode {
            /// Return the broad diagnostic class for this code.
            #[must_use]
            pub const fn class(self) -> ErrorClass {
                match self { $(Self::$variant => ErrorClass::$class),+ }
            }

            /// Return the default diagnostic origin for this code.
            #[must_use]
            pub const fn origin(self) -> ErrorOrigin {
                match self { $(Self::$variant => ErrorOrigin::$origin),+ }
            }

            /// Return the compact public wire code for this broad diagnostic reason.
            #[must_use]
            pub const fn error_code(self) -> ErrorCode {
                match self { $(Self::$variant => ErrorCode::$code),+ }
            }
        }
    };
}

diagnostic_codes! {
    /// An admitted exact count has no available exact-cardinality metadata.
    QueryExactCountMetadataUnavailable => (Unsupported, Query, QUERY_EXACT_COUNT_METADATA_UNAVAILABLE),
    QueryIntent => (Query, Query, QUERY_INTENT),
    QueryInvalidContinuationCursor => (Query, Query, QUERY_INVALID_CONTINUATION_CURSOR),
    QueryNotFound => (NotFound, Query, QUERY_NOT_FOUND),
    QueryNotUnique => (Query, Query, QUERY_NOT_UNIQUE),
    QueryNumericNotRepresentable => (Query, Query, QUERY_NUMERIC_NOT_REPRESENTABLE),
    QueryNumericOverflow => (Query, Query, QUERY_NUMERIC_OVERFLOW),
    QueryPlan => (Query, Query, QUERY_PLAN),
    QueryReadAdmission => (Query, Query, QUERY_READ_ADMISSION),
    QuerySqlSurfaceMismatch => (Unsupported, Query, QUERY_SQL_SURFACE_MISMATCH),
    QuerySqlWriteBoundary => (Unsupported, Query, QUERY_SQL_WRITE_BOUNDARY),
    QueryUnknownAggregateTargetField => (Unsupported, Query, QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD),
    QueryUnorderedPagination => (Query, Query, QUERY_UNORDERED_PAGINATION),
    QueryUnsupportedProjection => (Unsupported, Query, QUERY_UNSUPPORTED_PROJECTION),
    QueryUnsupportedSqlFeature => (Unsupported, Query, QUERY_UNSUPPORTED_SQL_FEATURE),
    QueryValidate => (Query, Query, QUERY_VALIDATE),
    RuntimeConflict => (Conflict, Runtime, RUNTIME_CONFLICT),
    RuntimeCorruption => (Corruption, Runtime, RUNTIME_CORRUPTION),
    RuntimeIncompatiblePersistedFormat => (IncompatiblePersistedFormat, Runtime, RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT),
    RuntimeInternal => (Internal, Runtime, RUNTIME_INTERNAL),
    RuntimeInvariantViolation => (InvariantViolation, Runtime, RUNTIME_INVARIANT_VIOLATION),
    RuntimeNotFound => (NotFound, Runtime, RUNTIME_NOT_FOUND),
    RuntimeUnsupported => (Unsupported, Runtime, RUNTIME_UNSUPPORTED),
    SchemaDdlAdmission => (Query, Query, SCHEMA_DDL_ADMISSION),
    StoreCorruption => (Corruption, Store, STORE_CORRUPTION),
    StoreInvariantViolation => (InvariantViolation, Store, STORE_INVARIANT_VIOLATION),
    StoreNotFound => (NotFound, Store, STORE_NOT_FOUND),
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
/// The public Wire `icydb::Error` stores this value as native `u16` so canister
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
    /// The operation conflicts with current state or another operation.
    Conflict,
    /// Persisted or supplied storage contents violate their encoded contract.
    Corruption,
    /// Retained bytes belong to a different storage-format contract.
    IncompatiblePersistedFormat,
    /// An internal operation failed without a more specific public classification.
    Internal,
    /// An invariant required by the implementation was violated.
    InvariantViolation,
    /// The requested object or row was not found.
    NotFound,
    /// The query could not be admitted or executed.
    Query,
    /// The requested operation is outside the supported contract.
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
    /// Continuation-cursor validation or construction.
    Cursor,
    /// Physical query or mutation execution.
    Executor,
    /// Entity, key or object identity handling.
    Identity,
    /// Index-key or index-entry processing.
    Index,
    /// Public interface admission or conversion.
    Interface,
    /// Logical or physical plan construction.
    Planner,
    /// The query could not be admitted or executed.
    Query,
    /// Interrupted-operation recovery.
    Recovery,
    /// Response construction or admission.
    Response,
    /// Runtime service or execution-boundary handling.
    Runtime,
    /// Value encoding or decoding.
    Serialize,
    /// Storage access or stored-state validation.
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
    /// boundary behavior used by the Wire facade.
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
    /// Query validation rejected the request.
    Validate,
    /// The query's declared intent is invalid.
    Intent,
    /// A valid execution plan could not be produced.
    Plan,
    /// Pagination was requested without the required ordering.
    UnorderedPagination,
    /// The continuation cursor is invalid for this query.
    InvalidContinuationCursor,
    /// The requested object or row was not found.
    NotFound,
    /// The operation required a unique result but found multiple matches.
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
    /// The projection requires a numeric literal in this position.
    NumericLiteralRequired,
    /// The numeric scale function received invalid arguments.
    NumericScaleArguments,
    /// The requested nested-field projection is not supported by this surface.
    NestedFieldPathPreview,
    /// A CASE condition must evaluate to a boolean.
    CaseConditionBooleanRequired,
    /// The projection requires numeric input.
    NumericInputRequired,
    /// The projection requires text or binary input.
    TextOrBlobInputRequired,
    /// The projection requires text input.
    TextInputRequired,
    /// The argument must be text or NULL.
    TextOrNullArgumentRequired,
    /// The argument must be an integer or NULL.
    IntegerOrNullArgumentRequired,
    /// The operand is incompatible with the unary operation.
    UnaryOperandIncompatible,
    /// The operands are incompatible with the binary operation.
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
    /// The public query must declare a result limit.
    PublicQueryRequiresLimit,
    /// The public query requires an admitted index access path.
    PublicQueryRequiresIndex,
    /// The request would perform a disallowed unbounded full scan.
    UnboundedFullScanRejected,
    /// Sorting requires materialization that this lane cannot admit.
    SortRequiresMaterialization,
    /// The grouped query must declare its required limits.
    GroupedQueryRequiresLimits,
    /// The grouped query exceeds its admitted resource budget.
    GroupedQueryExceedsBudget,
    /// The selected diagnostic lane cannot execute the query.
    DiagnosticLaneDoesNotExecute,
    /// The requested returned-row bound exceeds policy.
    ReturnedRowBoundExceedsPolicy,
    /// The primary-key input exceeds admission policy.
    PrimaryKeyInputExceedsPolicy,
    /// Authored query expressions or values exceed the shared depth ceiling.
    InputDepthExceeded,
    /// Authored query components exceed the shared node ceiling.
    InputNodesExceeded,
    /// Authored query payload exceeds the shared byte ceiling.
    InputBytesExceeded,
    /// Logical explain describes a request before any cursor boundary.
    ExplainDoesNotAcceptCursor,
}

impl fmt::Debug for QueryReadAdmissionCode {
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
    /// Persisted or supplied storage contents violate their encoded contract.
    Corruption,
    /// Retained bytes belong to a different storage-format contract.
    IncompatiblePersistedFormat,
    /// An invariant required by the implementation was violated.
    InvariantViolation,
    /// The operation conflicts with current state or another operation.
    Conflict,
    /// The requested object or row was not found.
    NotFound,
    /// The requested operation is outside the supported contract.
    Unsupported,
    /// An internal operation failed without a more specific public classification.
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
    /// The generated SQL surface requires a controller caller.
    SqlSurfaceControllerRequired,
    /// The generated schema surface requires a controller caller.
    SchemaSurfaceControllerRequired,
    /// No entity is configured for the generated SQL query surface.
    SqlQueryNoConfiguredEntities,
    /// The SQL query names an entity unavailable on this surface.
    SqlQueryEntityNotFound,
    /// The DDL request must identify its target.
    SqlDdlTargetRequired,
    /// The DDL target entity is not configured for this surface.
    SqlDdlEntityNotConfigured,
    /// SQL introspection is disabled for the deployed surface.
    SqlIntrospectionDisabled,
    /// A complete accepted mutation omitted a required field.
    MutationRequiredFieldMissing,
    /// A logical write would move an accepted managed timestamp backward.
    MutationManagedTimestampRegression,
    /// A persisted row's stamp falls outside the accepted layout window.
    PersistedRowLayoutOutsideAcceptedWindow,
    /// A persisted row's physical slot count disagrees with its layout stamp.
    PersistedRowSlotCountMismatch,
    /// A generated field would collide with an accepted DDL-owned slot.
    GeneratedFieldAfterDdlField,
    /// A journaled mutation cannot reserve a representable post-commit revision.
    JournalMutationRevisionExhausted,
    /// A final canonical after-image violates one accepted row constraint or gate.
    ConstraintViolation,
    /// Accepted row-constraint metadata or its compiled program is inconsistent.
    AcceptedRowConstraintProgramCorrupt,
    /// A write conflicts with one incomplete accepted constraint activation.
    ConstraintActivationWriteBlocked,
    /// A live generated constraint activation no longer matches its proposal.
    GeneratedConstraintActivationStale,
    /// A caller explicitly authored a field owned by accepted database policy.
    MutationDatabaseOwnedFieldExplicit,
    /// A mixed structural mutation batch contained no operations.
    MutationBatchEmpty,
    /// A mixed structural mutation batch exceeded its operation-count bound.
    MutationBatchTooManyItems,
    /// A mixed structural mutation batch exceeded its staged-byte bound.
    MutationBatchStagedBytesExceeded,
    /// A mixed structural mutation result exceeded its encoded response bound.
    MutationBatchResultBytesExceeded,
    /// A mixed structural mutation batch crossed an accepted store boundary.
    MutationBatchStoreMismatch,
    /// A mixed structural mutation batch exceeded its distinct-entity bound.
    MutationBatchTooManyEntities,
    /// More than one mixed structural operation targeted the same accepted key.
    MutationBatchDuplicateKey,
    /// An operational report or reset endpoint requires a controller caller.
    OperationalSurfaceControllerRequired,
    /// An exact-key batch exceeded its admitted input item count.
    ExactKeyBatchTooManyItems,
    /// An exact-key batch exceeded its admitted encoded input-key bytes.
    ExactKeyBatchInputBytesExceeded,
    /// An exact-key batch exceeded its admitted distinct stored-row bytes.
    ExactKeyBatchStoredBytesExceeded,
    /// An exact-key batch exceeded its admitted logical result bytes.
    ExactKeyBatchResultBytesExceeded,
    /// One charged execution resource exceeded its absolute safety ceiling.
    ExecutionBudgetExceeded,
    /// One indivisible scalar-page unit cannot fit in an otherwise empty page envelope.
    PageUnitTooLarge,
    /// Database access was attempted without an active request-execution scope.
    RequestExecutionScopeRequired,
    /// An explicit request root conflicts with the root already active for this request.
    RequestExecutionRootMismatch,
    /// A successful generated SQL query reply exceeds the deployed IC query-response limit.
    SqlQueryReplyBytesExceeded,
    /// The explain output exceeds its admitted size bound.
    QueryExplainOutputExceeded,
    /// A derived logical explain access tree exceeds the diagnostic depth limit.
    QueryExplainDepthExceeded,
    /// Startup recovery remains incomplete and ordinary database work must retry later.
    DatabaseStartupRecoveryPending,
    /// An application guard denied access to the generated SQL read surface.
    SqlSurfacePolicyDenied,
    /// An application guard denied access to the generated accepted-schema surface.
    SchemaSurfacePolicyDenied,
    /// A structural mutation batch exceeded its canonical prepared-commit work bound.
    MutationBatchCommitWorkExceeded,
    /// Retained journal debt leaves insufficient bounded convergence capacity.
    ConvergenceBacklogPressure,
    /// Requested manager bucket pages differ from the existing memory layout.
    MemoryBucketSizeMismatch,
    /// Host grants cannot resolve or authorize current logical allocations.
    MemoryAllocationResolutionFailed,
    /// Current declarations omitted a historical IcyDB database namespace.
    MemoryNamespaceRemoved,
    /// Current database controls or store allocations lack a complete role set.
    MemoryAllocationRolesIncomplete,
    /// Current memory declarations violate their bounded identity/role contract.
    MemoryDeclarationInvalid,
    /// Current declaration requirements differ from established memory authority.
    MemoryDeclarationSnapshotMismatch,
    /// A historical allocation required for journal recovery cannot be selected.
    MemoryHistoricalJournalUnavailable,
    /// A typed batch handle belongs to another result owner.
    TypedAdapterBatchHandleMismatch,
    /// A typed batch result row has already been consumed.
    TypedAdapterBatchRowConsumed,
    /// Generated source descriptors cannot bind to accepted schema authority.
    TypedAdapterBindingUnavailable,
    /// The output row belongs to another accepted entity.
    TypedAdapterEntityMismatch,
    /// The requested immutable field source is absent from the binding.
    TypedAdapterFieldUnavailable,
    /// A generated field contract disagrees with accepted schema authority.
    TypedAdapterIncompatibleField,
    /// A projected output row omits the field needed by its typed adapter.
    TypedAdapterRowFieldUnavailable,
    /// Output column and value cardinalities disagree.
    TypedAdapterRowShapeMismatch,
    /// A typed binding no longer represents current accepted authority.
    TypedAdapterStaleBinding,
    /// An output value cannot be represented by the generated Rust field type.
    TypedAdapterValueShapeMismatch,
    /// A successful accepted-schema report exceeds the IC query reply byte cap.
    SchemaReplyBytesExceeded,
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
    /// FILTER on an aggregate.
    AggregateFilterClause,
    /// An ALTER statement other than ALTER TABLE.
    AlterStatementBeyondAlterTable,
    /// More than one DEFAULT clause on ADD COLUMN.
    AlterTableAddColumnDuplicateDefault,
    /// Unsupported ADD COLUMN modifiers.
    AlterTableAddColumnModifiers,
    /// An unsupported ALTER TABLE ADD form.
    AlterTableAddStatementBeyondAddColumn,
    /// An unsupported ALTER COLUMN DROP action.
    AlterTableAlterColumnDropUnsupportedAction,
    /// Unsupported ALTER COLUMN modifiers.
    AlterTableAlterColumnModifiers,
    /// An unsupported ALTER COLUMN SET action.
    AlterTableAlterColumnSetUnsupportedAction,
    /// An unsupported ALTER COLUMN action.
    AlterTableAlterColumnUnsupportedAction,
    /// An unsupported ALTER TABLE ALTER form.
    AlterTableAlterStatementBeyondAlterColumn,
    /// IF EXISTS on DROP COLUMN.
    AlterTableDropColumnIfExistsSyntax,
    /// Unsupported DROP COLUMN modifiers.
    AlterTableDropColumnModifiers,
    /// An unsupported ALTER TABLE DROP form.
    AlterTableDropStatementBeyondDropColumn,
    /// RENAME COLUMN without the required TO keyword.
    AlterTableRenameColumnMissingTo,
    /// Unsupported RENAME COLUMN modifiers.
    AlterTableRenameColumnModifiers,
    /// An unsupported ALTER TABLE RENAME form.
    AlterTableRenameStatementBeyondRenameColumn,
    /// An unsupported ALTER TABLE operation.
    AlterTableUnsupportedOperation,
    /// A column alias in an unsupported position.
    ColumnAlias,
    /// IF NOT EXISTS on CREATE INDEX.
    CreateIndexIfNotExistsSyntax,
    /// Ordering modifiers on CREATE INDEX keys.
    CreateIndexKeyOrderingModifiers,
    /// Unsupported CREATE INDEX modifiers.
    CreateIndexModifiers,
    /// An unsupported CREATE statement other than CREATE INDEX.
    CreateStatementBeyondCreateIndex,
    /// An unsupported DESCRIBE modifier.
    DescribeModifier,
    /// Repeated expected-version clauses in DDL.
    DdlSchemaVersionDuplicateExpectedClause,
    /// Repeated next-version clauses in DDL.
    DdlSchemaVersionDuplicateSetClause,
    /// Unsupported DROP INDEX modifiers.
    DropIndexModifiers,
    /// IF EXISTS on DROP INDEX.
    DropIndexIfExistsSyntax,
    /// An unsupported DROP statement other than DROP INDEX.
    DropStatementBeyondDropIndex,
    /// An unsupported function in an expression index.
    ExpressionIndexUnsupportedFunction,
    /// A HAVING clause on a surface that does not support it.
    Having,
    /// An INSERT form unsupported by this parser or surface.
    Insert,
    /// A JOIN expression.
    Join,
    /// A LIKE pattern more general than a trailing-prefix match.
    LikePatternBeyondTrailingPrefix,
    /// An unsupported LOWER field predicate.
    LowerFieldPredicateUnsupported,
    /// Multiple SQL statements in one request.
    MultiStatementSql,
    /// An aggregate nested inside another aggregate's input.
    NestedAggregateInput,
    /// A projection function nested in unsupported arithmetic.
    NestedProjectionFunctionInArithmetic,
    /// An unsupported ORDER BY form.
    OrderByUnsupportedForm,
    /// Unsupported SQL syntax without a more specific feature identifier.
    Other,
    /// An unsupported first argument to STARTS_WITH.
    PredicateStartsWithFirstArgument,
    /// Quoted SQL identifiers.
    QuotedIdentifiers,
    /// An unsupported RETURNING projection shape.
    ReturningUnsupportedShape,
    /// A scalar function in an unsupported expression position.
    ScalarFunctionExpressionPosition,
    /// A scale-taking numeric function in an unsupported position.
    ScaleTakingNumericFunctionExpressionPosition,
    /// Unsupported SHOW COLUMNS modifiers.
    ShowColumnsModifiers,
    /// Unsupported SHOW ENTITIES modifiers.
    ShowEntitiesModifiers,
    /// Unsupported SHOW INDEXES modifiers.
    ShowIndexesModifiers,
    /// Unsupported SHOW MEMORY modifiers.
    ShowMemoryModifiers,
    /// Unsupported SHOW STORES modifiers.
    ShowStoresModifiers,
    /// An unsupported SHOW command.
    ShowUnsupportedCommand,
    /// A simple CASE expression rather than the supported searched form.
    SimpleCaseExpression,
    /// A standalone literal projection item on an unsupported path.
    StandaloneLiteralProjectionItem,
    /// A UNION, INTERSECT or EXCEPT set operation.
    UnionIntersectExcept,
    /// A function namespace not supported by this surface.
    UnsupportedFunctionNamespace,
    /// An UPDATE form unsupported by this parser or surface.
    Update,
    /// An unsupported UPPER field predicate.
    UpperFieldPredicateUnsupported,
    /// A window function.
    WindowFunction,
    /// A WITH clause.
    With,
    /// Invalid arguments to a numeric scale function.
    NumericScaleFunctionArguments,
    /// ORDER BY on a field without supported ordering.
    OrderByFieldNotOrderable,
    /// Unsupported SHOW CONSTRAINTS modifiers.
    ShowConstraintsModifiers,
    /// An unsupported ADD CONSTRAINT form beyond CHECK.
    AlterTableAddConstraintBeyondCheck,
    /// Unsupported ADD CONSTRAINT modifiers.
    AlterTableAddConstraintModifiers,
    /// IF EXISTS on DROP CONSTRAINT.
    AlterTableDropConstraintIfExistsSyntax,
    /// Unsupported DROP CONSTRAINT modifiers.
    AlterTableDropConstraintModifiers,
    /// An unsupported ALTER TABLE VALIDATE form.
    AlterTableValidateBeyondConstraint,
    /// Unsupported VALIDATE CONSTRAINT modifiers.
    AlterTableValidateConstraintModifiers,
    /// Unsupported SHOW RELATIONS modifiers.
    ShowRelationsModifiers,
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
    /// The lowered statement targets a different entity than the selected surface.
    EntityMismatch,
    /// The SELECT projection has an unsupported shape.
    SelectProjectionShape,
    /// The DISTINCT selection cannot be lowered on this path.
    SelectDistinct,
    /// The DISTINCT ordering does not agree with the projected columns.
    DistinctOrderByProjection,
    /// The global aggregate has an invalid projection shape.
    GlobalAggregateProjection,
    /// The global aggregate has an incompatible GROUP BY clause.
    GlobalAggregateGroupBy,
    /// The GROUP BY clause cannot be lowered in this form.
    SelectGroupByShape,
    /// A grouped projection must explicitly list its items.
    GroupedProjectionExplicitListRequired,
    /// The grouped projection requires an aggregate item.
    GroupedProjectionAggregateRequired,
    /// A projected scalar field is not a grouping field.
    GroupedProjectionNonGroupField,
    /// A scalar projection appears after an aggregate where ordering forbids it.
    GroupedProjectionScalarAfterAggregate,
    /// HAVING requires a GROUP BY clause on this surface.
    HavingRequiresGroupBy,
    /// The HAVING expression has an unsupported shape.
    SelectHavingShape,
    /// The aggregate input expression cannot be lowered.
    AggregateInputExpressions,
    /// The WHERE expression cannot be lowered in this form.
    WhereExpressionShape,
    /// A parameter appears in an unsupported position.
    ParameterPlacement,
    /// This execution surface does not admit the DDL statement.
    SqlDdlExecutionUnsupported,
    /// The supplied parameter count does not match the statement.
    BindingCount,
    /// A parameter's value family is incompatible with its binding.
    BindingFamily,
    /// The supplied parameter bindings exceed their admitted bound.
    BindingLimit,
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
    /// The query surface rejects INSERT statements.
    QueryRejectsInsert,
    /// The query surface rejects UPDATE statements.
    QueryRejectsUpdate,
    /// The query surface rejects DELETE statements.
    QueryRejectsDelete,
    /// The mutation surface rejects SELECT statements.
    MutationRejectsSelect,
    /// The mutation surface rejects EXPLAIN statements.
    MutationRejectsExplain,
    /// The mutation surface rejects DESCRIBE statements.
    MutationRejectsDescribe,
    /// The mutation surface rejects SHOW INDEXES statements.
    MutationRejectsShowIndexes,
    /// The mutation surface rejects SHOW COLUMNS statements.
    MutationRejectsShowColumns,
    /// The mutation surface rejects SHOW ENTITIES statements.
    MutationRejectsShowEntities,
    /// The mutation surface rejects SHOW STORES statements.
    MutationRejectsShowStores,
    /// The mutation surface rejects SHOW MEMORY statements.
    MutationRejectsShowMemory,
    /// The mutation surface requires explicit update intent.
    MutationRequiresExplicitUpdateIntent,
    /// The mutation surface rejects SHOW CONSTRAINTS statements.
    MutationRejectsShowConstraints,
    /// The mutation surface rejects SHOW RELATIONS statements.
    MutationRejectsShowRelations,
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
    /// The supplied literal cannot represent the accepted primary key.
    PrimaryKeyLiteralIncompatible,
    /// The write omits a required primary key.
    MissingPrimaryKey,
    /// The write omits required fields.
    MissingRequiredFields,
    /// The write explicitly assigns a managed field.
    ExplicitManagedField,
    /// The write explicitly assigns a generated field.
    ExplicitGeneratedField,
    /// INSERT SELECT requires a scalar projection on this path.
    InsertSelectRequiresScalar,
    /// INSERT SELECT cannot use the supplied aggregate projection.
    InsertSelectAggregateProjection,
    /// The SELECT width does not match the INSERT target list.
    InsertSelectWidthMismatch,
    /// The UPDATE attempts to change a primary-key field.
    UpdatePrimaryKeyMutation,
    /// A field literal is incompatible with its accepted value contract.
    InvalidFieldLiteral,
    /// RETURNING names a field absent from the accepted entity.
    UnknownReturningField,
    /// RETURNING names the same field more than once.
    DuplicateReturningField,
    /// The UPDATE requires a WHERE predicate.
    UpdateMissingWherePredicate,
    /// The write's ORDER BY clause has an unsupported shape.
    WriteOrderByUnsupportedShape,
    /// The RETURNING response exceeds its byte bound.
    ReturningResponseTooLarge,
    /// The RETURNING result exceeds its row bound.
    ReturningRowsTooMany,
    /// The write stages more rows than its bound permits.
    StagedRowsTooMany,
    /// INSERT DEFAULT cannot supply a required field's value.
    InsertDefaultRequiredField,
    /// UPDATE DEFAULT cannot supply a required field's value.
    UpdateDefaultRequiredField,
    /// UPDATE DEFAULT targets a database-owned field.
    UpdateDefaultDatabaseOwnedField,
    /// The exact UPDATE requires an affected-row assertion.
    ExactUpdateAssertionRequired,
    /// The affected-row assertion exceeds its admitted ceiling.
    ExactUpdateAssertionTooHigh,
    /// The UPDATE would exceed its asserted affected-row count.
    ExactUpdateAffectedRowsExceeded,
    /// The exact UPDATE uses an unsupported execution window.
    ExactUpdateWindowUnsupported,
    /// The write exceeds its scan budget.
    WriteScanBudgetExceeded,
    /// The resumable UPDATE uses an unsupported execution window.
    ResumableUpdateWindowUnsupported,
    /// RETURNING is unsupported for this resumable UPDATE.
    ResumableUpdateReturningUnsupported,
    /// The resumable UPDATE contains an unsupported assignment expression.
    ResumableUpdateExpressionUnsupported,
    /// The resumable UPDATE requires journal-backed storage.
    ResumableUpdateRequiresJournaledStore,
    /// An assigned field participates in a global constraint.
    ResumableUpdateAssignedFieldHasGlobalConstraint,
    /// The UPDATE scope depends on a field it assigns.
    ResumableUpdateScopeDependsOnAssignedField,
    /// The UPDATE scope's field dependencies cannot be established.
    ResumableUpdateScopeDependencyUnknown,
    /// The resumable UPDATE continuation is malformed.
    ResumableUpdateContinuationMalformed,
    /// The continuation belongs to a different UPDATE target.
    ResumableUpdateContinuationTargetMismatch,
    /// The continuation's schema authority no longer matches.
    ResumableUpdateContinuationSchemaMismatch,
    /// The continuation belongs to a different selection scope.
    ResumableUpdateContinuationScopeMismatch,
    /// The continuation belongs to a different assignment patch.
    ResumableUpdateContinuationPatchMismatch,
    /// The continuation's batch policy differs from this request.
    ResumableUpdateContinuationBatchPolicyMismatch,
    /// A managed field affected by the UPDATE has a global constraint.
    ResumableUpdateManagedFieldHasGlobalConstraint,
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
    /// The request omits the expected current schema version.
    MissingExpectedSchemaVersion,
    /// The request omits the proposed next schema version.
    MissingNextSchemaVersion,
    /// The expected schema version does not match current authority.
    StaleExpectedSchemaVersion,
    /// The expected schema version is invalid.
    InvalidExpectedSchemaVersion,
    /// The proposed next schema version is invalid.
    InvalidNextSchemaVersion,
    /// An accepted-schema change lacks its required version increment.
    AcceptedSchemaChangeWithoutVersionBump,
    /// The schema version advances without a corresponding schema change.
    EmptyVersionBump,
    /// The requested version transition skips an intervening version.
    VersionGap,
    /// The requested schema version moves backwards.
    VersionRollback,
    /// The schema fingerprint uses a different method than expected.
    FingerprintMethodMismatch,
    /// The requested schema transition class is unsupported.
    UnsupportedTransitionClass,
    /// The transition requires a physical migration runner unavailable on this path.
    PhysicalRunnerMissing,
    /// Candidate schema validation failed.
    ValidationFailed,
    /// Current authority changed before the candidate could be published.
    PublicationRaceLost,
    /// The added column's default violates its value contract.
    InvalidAddColumnDefault,
    /// The altered column's default violates its value contract.
    InvalidAlterColumnDefault,
    /// DDL cannot drop an index owned by generated schema declarations.
    GeneratedIndexDropRejected,
    /// The requested schema rewrite requires an explicit migration.
    SchemaRewriteRequiresMigration,
    /// The schema transition exceeds its work bound.
    SchemaTransitionBudgetExceeded,
    /// DDL cannot change a generated field's owned default.
    GeneratedFieldDefaultChangeRejected,
    /// DDL cannot change a generated field's owned nullability.
    GeneratedFieldNullabilityChangeRejected,
    /// The row-layout version cannot advance further.
    RowLayoutVersionExhausted,
}

impl fmt::Debug for SchemaDdlAdmissionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_code(f, *self as u16)
    }
}

/// Machine-readable source-migration rejection or lifecycle finding.
#[repr(u16)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum SchemaMigrationCode {
    /// The database has not adopted the required migration authority.
    Unadopted,
    /// The schema change lacks its required explicit migration.
    MissingMigration,
    /// The requested version transition skips an intervening version.
    VersionGap,
    /// The migration attempts to decrease an entity version.
    Downgrade,
    /// An entity version advances without a corresponding entity change.
    EmptyEntityVersionBump,
    /// The supplied accepted head is no longer current.
    StaleAcceptedHead,
    /// The supplied migration plan differs from retained migration authority.
    PlanChanged,
    /// A predecessor object named by the migration does not exist.
    UnknownFromObject,
    /// A successor object named by the migration does not exist.
    UnknownToObject,
    /// A migration binding joins objects of incompatible kinds.
    KindMismatch,
    /// A migration identity assignment conflicts with another binding.
    IdentityConflict,
    /// The proposal changes schema without an explaining migration operation.
    UnexplainedSchemaDifference,
    /// The requested row or schema transformation is unsupported.
    UnsupportedTransform,
    /// The transition requires a physical migration runner unavailable on this path.
    PhysicalRunnerMissing,
    /// A retained migration currently owns schema advancement.
    MigrationInProgress,
    /// The migration has crossed its pre-rewrite abort boundary.
    AbortTooLate,
    /// The retained migration progress violates its contract.
    ProgressCorrupt,
    /// The reconstructed candidate differs from the retained candidate authority.
    CandidateMismatch,
    /// Current authority changed before the candidate could be published.
    PublicationRaceLost,
}

impl SchemaMigrationCode {
    /// Return the broad diagnostic category for this migration result.
    #[must_use]
    pub const fn diagnostic_code(self) -> DiagnosticCode {
        match self {
            Self::StaleAcceptedHead
            | Self::PlanChanged
            | Self::IdentityConflict
            | Self::MigrationInProgress
            | Self::AbortTooLate
            | Self::PublicationRaceLost => DiagnosticCode::RuntimeConflict,
            Self::ProgressCorrupt | Self::CandidateMismatch => DiagnosticCode::RuntimeCorruption,
            Self::Unadopted
            | Self::MissingMigration
            | Self::VersionGap
            | Self::Downgrade
            | Self::EmptyEntityVersionBump
            | Self::UnknownFromObject
            | Self::UnknownToObject
            | Self::KindMismatch
            | Self::UnexplainedSchemaDifference
            | Self::UnsupportedTransform
            | Self::PhysicalRunnerMissing => DiagnosticCode::RuntimeUnsupported,
        }
    }
}

impl fmt::Debug for SchemaMigrationCode {
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
    /// Detailed classification carried as [`QueryErrorKind`].
    QueryKind {
        /// The typed discriminator for this diagnostic detail.
        kind: QueryErrorKind,
    },
    /// Detailed classification carried as [`QueryProjectionCode`].
    QueryProjection {
        /// The typed discriminator for this diagnostic detail.
        reason: QueryProjectionCode,
    },
    /// Detailed classification carried as [`QueryReadAdmissionCode`].
    QueryReadAdmission {
        /// The typed discriminator for this diagnostic detail.
        reason: QueryReadAdmissionCode,
    },
    /// Detailed classification carried as [`RuntimeBoundaryCode`].
    RuntimeBoundary {
        /// The typed discriminator for this diagnostic detail.
        boundary: RuntimeBoundaryCode,
    },
    /// Detailed classification carried as [`RuntimeErrorKind`].
    RuntimeKind {
        /// The typed discriminator for this diagnostic detail.
        kind: RuntimeErrorKind,
    },
    /// Detailed classification carried as [`SchemaDdlAdmissionCode`].
    SchemaDdlAdmission {
        /// The typed discriminator for this diagnostic detail.
        reason: SchemaDdlAdmissionCode,
    },
    /// Detailed classification carried as [`SchemaMigrationCode`].
    SchemaMigration {
        /// The typed discriminator for this diagnostic detail.
        reason: SchemaMigrationCode,
    },
    /// Detailed classification carried as [`SqlLoweringCode`].
    SqlLowering {
        /// The typed discriminator for this diagnostic detail.
        reason: SqlLoweringCode,
    },
    /// Detailed classification carried as [`SqlSurfaceMismatchCode`].
    SqlSurfaceMismatch {
        /// The typed discriminator for this diagnostic detail.
        mismatch: SqlSurfaceMismatchCode,
    },
    /// Detailed classification carried as [`SqlWriteBoundaryCode`].
    SqlWriteBoundary {
        /// The typed discriminator for this diagnostic detail.
        boundary: SqlWriteBoundaryCode,
    },
    /// Detailed classification carried as [`SqlFeatureCode`].
    UnsupportedSqlFeature {
        /// The typed discriminator for this diagnostic detail.
        feature: SqlFeatureCode,
    },
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
mod tests;
