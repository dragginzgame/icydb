//! Module: error
//!
//! Defines the canonical runtime error taxonomy for `icydb-core`.
//! This module owns the shared error classes, origins, details, and
//! constructor entry points used across storage, planning, execution, and
//! serialization boundaries.

#[cfg(test)]
mod tests;

use candid::CandidType;
use icydb_diagnostic_code as diagnostic_code;
use serde::Deserialize;
use std::fmt;

pub(crate) const COMPACT_QUERY_DIAGNOSTIC_MESSAGE: &str = "query diagnostic";
const COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE: &str = "runtime diagnostic";
const COMPACT_STORE_DIAGNOSTIC_MESSAGE: &str = "store diagnostic";
const COMPACT_INDEX_DIAGNOSTIC_MESSAGE: &str = "index diagnostic";
const COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE: &str = "serialize diagnostic";
const COMPACT_IDENTITY_DIAGNOSTIC_MESSAGE: &str = "identity diagnostic";

const fn compact_message_for(_class: ErrorClass, origin: ErrorOrigin) -> &'static str {
    match origin {
        ErrorOrigin::Serialize => COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE,
        ErrorOrigin::Store => COMPACT_STORE_DIAGNOSTIC_MESSAGE,
        ErrorOrigin::Index => COMPACT_INDEX_DIAGNOSTIC_MESSAGE,
        ErrorOrigin::Identity => COMPACT_IDENTITY_DIAGNOSTIC_MESSAGE,
        ErrorOrigin::Query | ErrorOrigin::Planner | ErrorOrigin::Response => {
            COMPACT_QUERY_DIAGNOSTIC_MESSAGE
        }
        ErrorOrigin::Cursor
        | ErrorOrigin::Recovery
        | ErrorOrigin::Executor
        | ErrorOrigin::Interface => COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
    }
}

// ============================================================================
// INTERNAL ERROR TAXONOMY — ARCHITECTURAL CONTRACT
// ============================================================================
//
// This file defines the canonical runtime error classification system for
// icydb-core. It is the single source of truth for:
//
//   • ErrorClass   (semantic domain)
//   • ErrorOrigin  (subsystem boundary)
//   • Structured detail payloads
//   • Canonical constructor entry points
//
// -----------------------------------------------------------------------------
// DESIGN INTENT
// -----------------------------------------------------------------------------
//
// 1. InternalError is a *taxonomy carrier*, not a formatting utility.
//
//    - ErrorClass represents semantic meaning (corruption, invariant_violation,
//      unsupported, etc).
//    - ErrorOrigin represents the subsystem boundary (store, index, query,
//      executor, serialize, interface, etc).
//    - The (class, origin) pair must remain stable and intentional.
//
// 2. Call sites MUST prefer canonical constructors.
//
//    Do NOT construct errors manually via:
//        InternalError::new(class, origin)
//    unless you are defining a new canonical helper here.
//
//    If a pattern appears more than once, centralize it here.
//
// 3. Constructors in this file must represent real architectural boundaries.
//
//    Add a new helper ONLY if it:
//
//      • Encodes a cross-cutting invariant,
//      • Represents a subsystem boundary,
//      • Or prevents taxonomy drift across call sites.
//
//    Do NOT add feature-specific helpers.
//    Do NOT add one-off formatting helpers.
//    Do NOT turn this file into a generic message factory.
//
// 4. ErrorDetail must align with ErrorOrigin.
//
//    If detail is present, it MUST correspond to the origin.
//    Do not attach mismatched detail variants.
//
// 5. Plan-layer errors are NOT runtime failures.
//
//    PlanError and CursorPlanError must be translated into
//    executor/query invariants via the canonical mapping functions.
//    Do not leak plan-layer error types across execution boundaries.
//
// 6. Preserve taxonomy stability.
//
//    Do NOT:
//      • Merge error classes.
//      • Reclassify corruption as internal.
//      • Downgrade invariant violations.
//      • Introduce ambiguous class/origin combinations.
//
//    Any change to ErrorClass or ErrorOrigin is an architectural change
//    and must be reviewed accordingly.
//
// -----------------------------------------------------------------------------
// NON-GOALS
// -----------------------------------------------------------------------------
//
// This is NOT:
//
//   • A public API contract.
//   • A generic error abstraction layer.
//   • A feature-specific message builder.
//   • A dumping ground for temporary error conversions.
//
// -----------------------------------------------------------------------------
// MAINTENANCE GUIDELINES
// -----------------------------------------------------------------------------
//
// When modifying this file:
//
//   1. Ensure classification semantics remain consistent.
//   2. Avoid constructor proliferation.
//   3. Prefer narrow, origin-specific helpers over ad-hoc new(...).
//   4. Keep formatting minimal and standardized.
//   5. Keep this file boring and stable.
//
// If this file grows rapidly, something is wrong at the call sites.
//
// ============================================================================

/// Safe accepted mutation identity retained only when constructing a failure.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MutationDiagnosticContext {
    entity_tag: u64,
    operation: diagnostic_code::DiagnosticMutationOperation,
    batch_position: Option<u32>,
}

impl MutationDiagnosticContext {
    /// Bind one mutation failure to its accepted entity, operation, and input.
    #[must_use]
    pub(crate) const fn new(
        entity_tag: u64,
        operation: diagnostic_code::DiagnosticMutationOperation,
        batch_position: u32,
    ) -> Self {
        Self {
            entity_tag,
            operation,
            batch_position: Some(batch_position),
        }
    }

    /// Bind a failure to an operation before any concrete input row is selected.
    #[must_use]
    pub(crate) const fn operation_only(
        entity_tag: u64,
        operation: diagnostic_code::DiagnosticMutationOperation,
    ) -> Self {
        Self {
            entity_tag,
            operation,
            batch_position: None,
        }
    }

    fn facts(self, field_id: Option<u32>) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        let mut facts = Vec::with_capacity(
            2 + usize::from(field_id.is_some()) + usize::from(self.batch_position.is_some()),
        );
        facts.push((
            diagnostic_code::DiagnosticFactTag::EntityTag,
            self.entity_tag,
        ));
        if let Some(field_id) = field_id {
            facts.push((
                diagnostic_code::DiagnosticFactTag::FieldId,
                u64::from(field_id),
            ));
        }
        facts.push((
            diagnostic_code::DiagnosticFactTag::MutationOperation,
            self.operation.raw(),
        ));
        if let Some(batch_position) = self.batch_position {
            facts.push((
                diagnostic_code::DiagnosticFactTag::BatchPosition,
                u64::from(batch_position),
            ));
        }
        facts
    }

    #[must_use]
    pub(crate) const fn entity_tag(self) -> u64 {
        self.entity_tag
    }

    fn append_operation_facts(self, facts: &mut Vec<(diagnostic_code::DiagnosticFactTag, u64)>) {
        facts.push((
            diagnostic_code::DiagnosticFactTag::MutationOperation,
            self.operation.raw(),
        ));
        if let Some(batch_position) = self.batch_position {
            facts.push((
                diagnostic_code::DiagnosticFactTag::BatchPosition,
                u64::from(batch_position),
            ));
        }
    }
}

/// Numeric context retained behind one thin error-only allocation.
pub struct DiagnosticFactDetail {
    diagnostic: diagnostic_code::Diagnostic,
    facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
}

///
/// InternalError
///
/// Structured runtime error with a stable internal classification.
/// Not a stable API; intended for internal use and may change without notice.
///

pub struct InternalError {
    pub(crate) class: ErrorClass,
    pub(crate) origin: ErrorOrigin,

    /// Optional structured error detail.
    /// The variant (if present) must correspond to `origin`.
    pub(crate) detail: Option<ErrorDetail>,
}

#[expect(
    clippy::missing_const_for_fn,
    reason = "internal error constructors stay non-const so compact diagnostic construction does not force const churn across subsystem helper seams"
)]
impl InternalError {
    /// Construct an InternalError with optional origin-specific detail.
    /// This constructor provides default StoreError details for certain
    /// (class, origin) combinations but does not guarantee a detail payload.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub fn new(class: ErrorClass, origin: ErrorOrigin) -> Self {
        let detail = match (class, origin) {
            (ErrorClass::Corruption, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::Corrupt))
            }
            (ErrorClass::InvariantViolation, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::InvariantViolation))
            }
            _ => None,
        };

        Self {
            class,
            origin,
            detail,
        }
    }

    /// Return the internal error class taxonomy.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        self.class
    }

    /// Return the internal error origin taxonomy.
    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        self.origin
    }

    /// Return the rendered internal error message.
    #[must_use]
    pub const fn message(&self) -> &'static str {
        compact_message_for(self.class, self.origin)
    }

    /// Return the optional structured detail payload.
    #[must_use]
    pub const fn detail(&self) -> Option<&ErrorDetail> {
        self.detail.as_ref()
    }

    /// Return compact diagnostic identity for this internal error.
    #[must_use]
    pub fn diagnostic(&self) -> diagnostic_code::Diagnostic {
        diagnostic_code::Diagnostic::new(
            self.diagnostic_code(),
            self.origin.diagnostic_origin(),
            self.detail
                .as_ref()
                .and_then(ErrorDetail::diagnostic_detail),
        )
    }

    /// Project typed internal context into canonical public numeric facts.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub fn diagnostic_facts(&self) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        self.detail
            .as_ref()
            .map_or_else(Vec::new, ErrorDetail::diagnostic_facts)
    }

    /// Return the compact diagnostic code for this internal error.
    #[must_use]
    pub fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        self.detail.as_ref().map_or_else(
            || self.class.diagnostic_code(self.origin),
            ErrorDetail::diagnostic_code,
        )
    }

    /// Consume and return the rendered internal error message.
    #[must_use]
    pub fn into_message(self) -> String {
        self.message().to_string()
    }

    /// Construct an error while preserving an explicit class/origin taxonomy pair.
    #[cold]
    #[inline(never)]
    pub(crate) fn classified(class: ErrorClass, origin: ErrorOrigin) -> Self {
        Self::new(class, origin)
    }

    #[cold]
    #[inline(never)]
    fn with_diagnostic_facts(
        class: ErrorClass,
        origin: ErrorOrigin,
        detail: Option<diagnostic_code::DiagnosticDetail>,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        let code = match detail {
            Some(detail) => detail.diagnostic_code(),
            None => class.diagnostic_code(origin),
        };
        let diagnostic = diagnostic_code::Diagnostic::new(code, origin.diagnostic_origin(), detail);
        if diagnostic_code::validate_known_diagnostic_fact_schema(
            diagnostic.error_code(),
            facts.as_slice(),
        )
        .is_err()
        {
            return Self::new(ErrorClass::InvariantViolation, origin);
        }
        Self {
            class,
            origin,
            detail: Some(ErrorDetail::DiagnosticFacts(Box::new(
                DiagnosticFactDetail { diagnostic, facts },
            ))),
        }
    }

    #[cold]
    #[inline(never)]
    fn mutation_boundary_with_facts(
        class: ErrorClass,
        boundary: diagnostic_code::RuntimeBoundaryCode,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        Self::with_diagnostic_facts(
            class,
            ErrorOrigin::Executor,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary { boundary }),
            facts,
        )
    }

    #[cold]
    #[inline(never)]
    fn exact_key_batch_boundary_with_facts(
        boundary: diagnostic_code::RuntimeBoundaryCode,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Unsupported,
            ErrorOrigin::Query,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary { boundary }),
            facts,
        )
    }

    /// Construct a query-boundary error for a named entity absent from accepted schema authority.
    pub(crate) fn sql_query_entity_not_found() -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::NotFound,
            ErrorOrigin::Interface,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: diagnostic_code::RuntimeBoundaryCode::SqlQueryEntityNotFound,
            }),
            Vec::new(),
        )
    }

    /// Construct an executor-origin hard execution-budget rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn execution_budget_exceeded(
        resource: diagnostic_code::DiagnosticExecutionBudgetResource,
        limit: u64,
        observed: u64,
        scope: diagnostic_code::DiagnosticExecutionBudgetScope,
        lane: diagnostic_code::DiagnosticExecutionLane,
        normalized_shape_fingerprint_prefix: u64,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Unsupported,
            ErrorOrigin::Executor,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
            }),
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::BudgetResource,
                    resource.raw(),
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit),
                (diagnostic_code::DiagnosticFactTag::Actual, observed),
                (
                    diagnostic_code::DiagnosticFactTag::ExecutionBudgetScope,
                    scope.raw(),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ExecutionLane,
                    lane.raw(),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::QueryShapeFingerprintPrefix,
                    normalized_shape_fingerprint_prefix,
                ),
            ],
        )
    }

    /// Construct an executor-origin rejection for one indivisible page unit.
    #[cold]
    #[inline(never)]
    pub(crate) fn page_unit_too_large(
        resource: diagnostic_code::DiagnosticExecutionBudgetResource,
        limit: u64,
        attempted: u64,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Unsupported,
            ErrorOrigin::Executor,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: diagnostic_code::RuntimeBoundaryCode::PageUnitTooLarge,
            }),
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::BudgetResource,
                    resource.raw(),
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit),
                (diagnostic_code::DiagnosticFactTag::Actual, attempted),
            ],
        )
    }

    /// Rebuild this error with a new origin while preserving class taxonomy.
    ///
    /// Numeric facts are origin-independent and remain safe after recovery
    /// relabeling. Other origin-scoped detail payloads are dropped.
    #[cold]
    #[inline(never)]
    pub(crate) fn with_origin(self, origin: ErrorOrigin) -> Self {
        match self.detail {
            Some(ErrorDetail::DiagnosticFacts(detail)) => Self::with_diagnostic_facts(
                self.class,
                origin,
                detail.diagnostic.detail().copied(),
                detail.facts,
            ),
            _ => Self::classified(self.class, origin),
        }
    }

    /// Construct an index-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Index)
    }

    /// Construct the canonical index field-count invariant for key building.
    pub(crate) fn index_key_field_count_exceeds_max(
        entity_tag: u64,
        physical_generation: u64,
        field_count: usize,
        max_fields: usize,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            None,
            vec![
                (diagnostic_code::DiagnosticFactTag::EntityTag, entity_tag),
                (
                    diagnostic_code::DiagnosticFactTag::PhysicalGeneration,
                    physical_generation,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ComponentKind,
                    diagnostic_code::DiagnosticComponentKind::IndexKey.raw(),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualArity,
                    field_count as u64,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::Maximum,
                    max_fields as u64,
                ),
            ],
        )
    }

    /// Construct the canonical index-expression source-type mismatch invariant.
    pub(crate) fn index_expression_source_type_mismatch(
        _index_name: &str,
        _expression: impl Sized,
        _expected: impl Sized,
        _source_label: &str,
    ) -> Self {
        Self::index_invariant()
    }

    /// Construct a planner-origin invariant violation for executor-boundary
    /// contract drift.
    #[cold]
    #[inline(never)]
    pub(crate) fn planner_executor_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Planner)
    }

    /// Construct a query-origin invariant violation for executor-boundary
    /// contract drift.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_executor_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Query)
    }

    /// Construct a cursor-origin invariant violation for executor-boundary
    /// contract drift.
    #[cold]
    #[inline(never)]
    pub(crate) fn cursor_executor_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Cursor)
    }

    /// Construct an executor-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Executor)
    }

    /// Construct an executor-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_internal() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Executor)
    }

    /// Construct an executor-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_unsupported() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Executor)
    }

    /// Construct an executor-origin database-owned-field authorship rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_database_owned_field_explicit(
        context: MutationDiagnosticContext,
        field_id: u32,
    ) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationDatabaseOwnedFieldExplicit,
            context.facts(Some(field_id)),
        )
    }

    /// Construct an executor-origin required-field omission rejection.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_required_field_missing(
        context: MutationDiagnosticContext,
        field_id: u32,
    ) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationRequiredFieldMissing,
            context.facts(Some(field_id)),
        )
    }

    /// Construct an executor-origin managed-timestamp clock regression.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_managed_timestamp_regression(
        context: MutationDiagnosticContext,
    ) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::InvariantViolation,
            diagnostic_code::RuntimeBoundaryCode::MutationManagedTimestampRegression,
            context.facts(None),
        )
    }

    /// Construct an executor-origin accepted constraint or activation-gate violation.
    pub(crate) fn mutation_constraint_violation(context: AcceptedConstraintFactContext) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::InvariantViolation,
            diagnostic_code::RuntimeBoundaryCode::ConstraintViolation,
            context.facts(),
        )
    }

    /// Construct an executor-origin corruption failure for row-constraint authority.
    pub(crate) fn accepted_row_constraint_program_corrupt() -> Self {
        Self {
            class: ErrorClass::Corruption,
            origin: ErrorOrigin::Executor,
            detail: Some(ErrorDetail::Executor(
                ExecutorErrorDetail::AcceptedRowConstraintProgramCorrupt,
            )),
        }
    }

    /// Construct one typed migration conflict for an incomplete activation gate.
    pub(crate) fn mutation_constraint_activation_write_blocked(
        context: AcceptedConstraintFactContext,
    ) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Conflict,
            diagnostic_code::RuntimeBoundaryCode::ConstraintActivationWriteBlocked,
            context.facts(),
        )
    }

    /// Construct a query-origin scalar page invariant for missing order at the cursor boundary.
    pub(crate) fn scalar_page_cursor_boundary_order_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin scalar page invariant for cursor-before-ordering drift.
    pub(crate) fn scalar_page_cursor_boundary_after_ordering_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin scalar page invariant for pagination-before-ordering drift.
    pub(crate) fn scalar_page_pagination_after_ordering_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin fast-stream invariant for route kind/request mismatch.
    pub(crate) fn fast_stream_route_kind_request_match_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin scan invariant for missing index-prefix executable specs.
    pub(crate) fn secondary_index_prefix_spec_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin scan invariant for missing index-range executable specs.
    pub(crate) fn index_range_limit_spec_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct an executor-origin mutation conflict for duplicate atomic save keys.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_atomic_save_duplicate_key(
        entity_tag: u64,
        first_position: u32,
        duplicate_position: u32,
    ) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Conflict,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchDuplicateKey,
            vec![
                (diagnostic_code::DiagnosticFactTag::EntityTag, entity_tag),
                (
                    diagnostic_code::DiagnosticFactTag::FirstBatchPosition,
                    u64::from(first_position),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::DuplicateBatchPosition,
                    u64::from(duplicate_position),
                ),
            ],
        )
    }

    /// Construct an executor-origin empty mixed-mutation batch rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_batch_empty() -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchEmpty,
            vec![(diagnostic_code::DiagnosticFactTag::ActualCount, 0)],
        )
    }

    /// Construct an executor-origin mixed-mutation item-bound rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_batch_too_many_items(actual_count: usize, limit: usize) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchTooManyItems,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ActualCount,
                    actual_count as u64,
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit as u64),
            ],
        )
    }

    /// Construct an executor-origin mixed-mutation staged-byte-bound rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_batch_staged_bytes_exceeded(
        actual_bytes: Option<usize>,
        limit: usize,
    ) -> Self {
        let mut facts = Vec::with_capacity(1 + usize::from(actual_bytes.is_some()));
        if let Some(actual_bytes) = actual_bytes {
            facts.push((
                diagnostic_code::DiagnosticFactTag::ActualLength,
                actual_bytes as u64,
            ));
        }
        facts.push((diagnostic_code::DiagnosticFactTag::Limit, limit as u64));
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchStagedBytesExceeded,
            facts,
        )
    }

    /// Construct an executor-origin mixed-mutation result-byte-bound rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_batch_result_bytes_exceeded(actual_bytes: usize, limit: usize) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchResultBytesExceeded,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ActualLength,
                    actual_bytes as u64,
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit as u64),
            ],
        )
    }

    /// Construct an executor-origin prepared-commit work-bound rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_batch_commit_work_exceeded(
        actual_units: Option<usize>,
        limit: usize,
    ) -> Self {
        let mut facts = Vec::with_capacity(1 + usize::from(actual_units.is_some()));
        if let Some(actual_units) = actual_units {
            facts.push((
                diagnostic_code::DiagnosticFactTag::ActualCount,
                actual_units as u64,
            ));
        }
        facts.push((diagnostic_code::DiagnosticFactTag::Limit, limit as u64));
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchCommitWorkExceeded,
            facts,
        )
    }

    /// Construct the retryable cumulative journal-backlog pressure boundary.
    pub(crate) fn convergence_backlog_pressure(
        resource: diagnostic_code::DiagnosticBacklogResource,
        current: u64,
        proposed: u64,
        limit: u64,
    ) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Conflict,
            diagnostic_code::RuntimeBoundaryCode::ConvergenceBacklogPressure,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::BacklogResource,
                    resource.raw(),
                ),
                (diagnostic_code::DiagnosticFactTag::CurrentCount, current),
                (diagnostic_code::DiagnosticFactTag::ProposedCount, proposed),
                (diagnostic_code::DiagnosticFactTag::Limit, limit),
            ],
        )
    }

    /// Construct a query-origin exact-key item-bound rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn exact_key_batch_too_many_items(actual_count: usize, limit: usize) -> Self {
        Self::exact_key_batch_boundary_with_facts(
            diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchTooManyItems,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ActualCount,
                    actual_count as u64,
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit as u64),
            ],
        )
    }

    /// Construct a query-origin exact-key input-byte rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn exact_key_batch_input_bytes_exceeded(actual_bytes: usize, limit: usize) -> Self {
        Self::exact_key_batch_bytes_exceeded(
            diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchInputBytesExceeded,
            actual_bytes,
            limit,
        )
    }

    /// Construct a query-origin exact-key stored-row-byte rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn exact_key_batch_stored_bytes_exceeded(actual_bytes: usize, limit: usize) -> Self {
        Self::exact_key_batch_bytes_exceeded(
            diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchStoredBytesExceeded,
            actual_bytes,
            limit,
        )
    }

    /// Construct a query-origin exact-key result-byte rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn exact_key_batch_result_bytes_exceeded(actual_bytes: usize, limit: usize) -> Self {
        Self::exact_key_batch_bytes_exceeded(
            diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchResultBytesExceeded,
            actual_bytes,
            limit,
        )
    }

    #[cold]
    #[inline(never)]
    fn exact_key_batch_bytes_exceeded(
        boundary: diagnostic_code::RuntimeBoundaryCode,
        actual_bytes: usize,
        limit: usize,
    ) -> Self {
        Self::exact_key_batch_boundary_with_facts(
            boundary,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ActualLength,
                    actual_bytes as u64,
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit as u64),
            ],
        )
    }

    /// Construct an executor-origin cross-store batch rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_batch_store_mismatch(
        batch_position: u32,
        expected_entity_tag: u64,
        actual_entity_tag: u64,
    ) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Conflict,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchStoreMismatch,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::BatchPosition,
                    u64::from(batch_position),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ExpectedEntityTag,
                    expected_entity_tag,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualEntityTag,
                    actual_entity_tag,
                ),
            ],
        )
    }

    /// Construct an executor-origin distinct-entity-bound rejection.
    #[cold]
    #[inline(never)]
    pub(crate) fn mutation_batch_too_many_entities(actual_count: usize, limit: usize) -> Self {
        Self::mutation_boundary_with_facts(
            ErrorClass::Unsupported,
            diagnostic_code::RuntimeBoundaryCode::MutationBatchTooManyEntities,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ActualCount,
                    actual_count as u64,
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit as u64),
            ],
        )
    }

    /// Construct an executor-origin mutation invariant for index-store generation drift.
    pub(crate) fn mutation_index_store_generation_changed(
        _expected_generation: u64,
        _observed_generation: u64,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct a planner-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn planner_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Planner)
    }

    /// Construct a planner-origin invalid-logical-plan invariant.
    pub(crate) fn query_invalid_logical_plan() -> Self {
        Self::planner_invariant()
    }

    /// Construct a store-origin invariant violation.
    pub(crate) fn store_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Store)
    }

    /// Construct a store-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_internal() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Store)
    }

    /// Construct the canonical unconfigured commit-memory id internal error.
    pub(crate) fn commit_memory_id_unconfigured() -> Self {
        Self::store_internal()
    }

    /// Construct the canonical initialized commit-store lookup invariant.
    pub(crate) fn commit_store_uninitialized() -> Self {
        Self::store_invariant()
    }

    /// Construct the canonical commit-memory id mismatch internal error.
    pub(crate) fn commit_memory_id_mismatch(cached_id: u8, configured_id: u8) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            None,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ExpectedMemoryId,
                    u64::from(cached_id),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualMemoryId,
                    u64::from(configured_id),
                ),
            ],
        )
    }

    /// Construct the canonical commit-memory stable-key mismatch internal error.
    pub(crate) fn commit_memory_stable_key_mismatch(
        _cached_key: &str,
        _configured_key: &str,
    ) -> Self {
        Self::store_internal()
    }

    /// Construct the canonical database-incarnation generation failure.
    pub(crate) fn database_incarnation_generation_failed() -> Self {
        Self::store_internal()
    }

    /// Construct the canonical zero database-incarnation corruption error.
    pub(crate) fn database_incarnation_invalid() -> Self {
        Self::store_corruption()
    }

    /// Construct a recovery-origin incompatible store-format error.
    pub(crate) fn recovery_unsupported_database_format(found: Option<u16>, required: u16) -> Self {
        Self {
            class: ErrorClass::IncompatiblePersistedFormat,
            origin: ErrorOrigin::Recovery,
            detail: Some(ErrorDetail::Recovery(
                RecoveryErrorDetail::UnsupportedFormatVersion { found, required },
            )),
        }
    }

    /// Construct a recovery-origin malformed store-format marker error.
    pub(crate) fn recovery_malformed_database_format_marker(
        reason: RecoveryFormatMarkerError,
    ) -> Self {
        Self {
            class: ErrorClass::Corruption,
            origin: ErrorOrigin::Recovery,
            detail: Some(ErrorDetail::Recovery(
                RecoveryErrorDetail::MalformedFormatMarker { reason },
            )),
        }
    }

    /// Construct a recovery-origin boot control-memory failure.
    pub(crate) fn recovery_database_format_control_unavailable() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Recovery)
    }

    /// Construct the retryable internal boundary returned while bounded startup recovery remains.
    pub(crate) fn recovery_pending() -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Conflict,
            ErrorOrigin::Recovery,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: diagnostic_code::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
            }),
            Vec::new(),
        )
    }

    /// Construct fail-closed corruption for the bounded startup control cell.
    pub(crate) fn startup_control_corruption() -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Recovery)
    }

    /// Construct a commit control-memory growth failure.
    pub(crate) fn commit_control_memory_growth_failed() -> Self {
        Self::store_internal()
    }

    /// Construct a store-format memory registration failure.
    #[cfg(not(test))]
    pub(crate) fn database_format_memory_registration_failed(_err: impl Sized) -> Self {
        Self::store_internal()
    }

    /// Construct the canonical recovered-effect verification failure.
    pub(crate) fn recovery_effect_verification_failed() -> Self {
        Self::store_corruption()
    }

    /// Construct an index-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_internal() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Index)
    }

    /// Construct the canonical missing old entity-key internal error for structural index removal.
    pub(crate) fn structural_index_removal_entity_key_required() -> Self {
        Self::index_internal()
    }

    /// Construct the canonical missing new entity-key internal error for structural index insertion.
    pub(crate) fn structural_index_insertion_entity_key_required() -> Self {
        Self::index_internal()
    }

    /// Construct the canonical missing old entity-key internal error for index commit-op removal.
    pub(crate) fn index_commit_op_old_entity_key_required() -> Self {
        Self::index_internal()
    }

    /// Construct the canonical missing new entity-key internal error for index commit-op insertion.
    pub(crate) fn index_commit_op_new_entity_key_required() -> Self {
        Self::index_internal()
    }

    /// Construct a query-origin internal error.
    #[cfg(test)]
    pub(crate) fn query_internal() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Query)
    }

    /// Construct a query-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_unsupported() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Query)
    }

    /// Construct a query-origin conflict for execution against a superseded
    /// accepted schema revision.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_stale_accepted_schema_revision(
        expected_revision: u64,
        current_revision: Option<u64>,
    ) -> Self {
        let mut facts = Vec::with_capacity(1 + usize::from(current_revision.is_some()));
        facts.push((
            diagnostic_code::DiagnosticFactTag::ExpectedRevision,
            expected_revision,
        ));
        if let Some(current_revision) = current_revision {
            facts.push((
                diagnostic_code::DiagnosticFactTag::CurrentRevision,
                current_revision,
            ));
        }
        Self::with_diagnostic_facts(ErrorClass::Conflict, ErrorOrigin::Query, None, facts)
    }

    /// Construct a query-origin SQL DDL admission error with structured detail.
    #[cold]
    #[inline(never)]
    #[cfg(feature = "sql")]
    pub(crate) fn query_schema_ddl_admission(error: SchemaDdlAdmissionError) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SchemaDdlAdmission {
                error,
            })),
        }
    }

    /// Construct a query-origin numeric overflow error with structured detail.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_numeric_overflow() -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(QueryErrorDetail::NumericOverflow)),
        }
    }

    /// Construct a query-origin non-representable numeric result error with
    /// structured detail.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_numeric_not_representable() -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::NumericNotRepresentable,
            )),
        }
    }

    /// Construct a serialize-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn serialize_internal() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Serialize)
    }

    /// Construct the canonical persisted-row encode internal error.
    pub(crate) fn persisted_row_encode_failed(_detail: impl Sized) -> Self {
        Self::persisted_row_encode_internal()
    }

    /// Construct the compact persisted-row encode internal error.
    pub(crate) fn persisted_row_encode_internal() -> Self {
        Self::serialize_internal()
    }

    /// Construct the compact persisted-row field encode internal error.
    pub(crate) fn persisted_row_field_encode_internal(_field_name: &str) -> Self {
        Self::persisted_row_encode_internal()
    }

    /// Construct a store-origin corruption error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_corruption() -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Store)
    }

    /// Construct a store-origin commit-marker corruption error.
    pub(crate) fn commit_corruption() -> Self {
        Self::store_corruption()
    }

    /// Construct a store-origin commit-marker component corruption error.
    pub(crate) fn commit_component_corruption() -> Self {
        Self::commit_corruption()
    }

    /// Construct the canonical commit-marker id generation internal error.
    pub(crate) fn commit_id_generation_failed() -> Self {
        Self::store_internal()
    }

    /// Construct the canonical commit-marker payload u32-length-limit error.
    pub(crate) fn commit_marker_payload_exceeds_u32_length_limit() -> Self {
        Self::store_unsupported()
    }

    /// Construct the canonical commit-marker component invalid-length corruption error.
    pub(crate) fn commit_component_length_invalid(actual_length: usize, limit: usize) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            None,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ComponentKind,
                    diagnostic_code::DiagnosticComponentKind::CommitDataKey.raw(),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualLength,
                    actual_length as u64,
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit as u64),
            ],
        )
    }

    /// Construct the canonical commit-marker max-size corruption error.
    pub(crate) fn commit_marker_exceeds_max_size() -> Self {
        Self::commit_corruption()
    }

    /// Construct the canonical commit-control slot max-size unsupported error.
    pub(crate) fn commit_control_slot_exceeds_max_size() -> Self {
        Self::store_unsupported()
    }

    /// Construct the canonical commit-control marker-bytes length-limit error.
    pub(crate) fn commit_control_slot_marker_bytes_exceed_u32_length_limit() -> Self {
        Self::store_unsupported()
    }

    /// Construct an index-origin corruption error.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_corruption() -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Index)
    }

    /// Construct the canonical unique-validation corruption wrapper.
    pub(crate) fn index_unique_validation_corruption() -> Self {
        Self::index_plan_index_corruption()
    }

    /// Construct the canonical structural index-entry corruption wrapper.
    pub(crate) fn structural_index_entry_corruption() -> Self {
        Self::index_plan_index_corruption()
    }

    /// Construct the canonical missing new entity-key invariant during unique validation.
    pub(crate) fn index_unique_validation_entity_key_required() -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical unique-validation structural row-decode corruption error.
    pub(crate) fn index_unique_validation_row_deserialize_failed() -> Self {
        Self::index_plan_serialize_corruption()
    }

    /// Construct the canonical unique-validation primary-key slot decode corruption error.
    pub(crate) fn index_unique_validation_primary_key_decode_failed() -> Self {
        Self::index_plan_serialize_corruption()
    }

    /// Construct the canonical unique-validation stored key rebuild corruption error.
    pub(crate) fn index_unique_validation_key_rebuild_failed() -> Self {
        Self::index_plan_serialize_corruption()
    }

    /// Construct the canonical unique-validation missing-row corruption error.
    pub(crate) fn index_unique_validation_row_required() -> Self {
        Self::index_plan_store_corruption()
    }

    /// Construct the canonical index-only predicate missing-component invariant.
    pub(crate) fn index_only_predicate_component_required() -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical index-scan continuation-envelope invariant.
    pub(crate) fn index_scan_continuation_anchor_within_envelope_required() -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical index-scan continuation-advancement invariant.
    pub(crate) fn index_scan_continuation_advancement_required() -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical index-scan key-decode corruption error.
    pub(crate) fn index_scan_key_corrupted_during(
        _context: &'static str,
        _err: impl Sized,
    ) -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical index-scan missing projection-component invariant.
    pub(crate) fn index_projection_component_required(
        _index_name: &str,
        _component_index: usize,
    ) -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical scan-time index-entry decode corruption error.
    pub(crate) fn index_entry_decode_failed() -> Self {
        Self::index_corruption()
    }

    /// Construct a serialize-origin corruption error.
    pub(crate) fn serialize_corruption() -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Serialize)
    }

    /// Construct the compact persisted-row decode corruption error.
    pub(crate) fn persisted_row_decode_corruption() -> Self {
        Self::serialize_corruption()
    }

    /// Construct a persisted-row layout-window corruption error.
    pub(crate) fn persisted_row_layout_outside_accepted_window(
        row_layout: u32,
        history_floor: u32,
        current_layout: u32,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Corruption,
            ErrorOrigin::Serialize,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary:
                    diagnostic_code::RuntimeBoundaryCode::PersistedRowLayoutOutsideAcceptedWindow,
            }),
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::RowLayout,
                    u64::from(row_layout),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::HistoryFloor,
                    u64::from(history_floor),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::CurrentLayout,
                    u64::from(current_layout),
                ),
            ],
        )
    }

    /// Construct a persisted-row stamped-layout slot-count corruption error.
    pub(crate) fn persisted_row_slot_count_mismatch(
        row_layout: u32,
        expected_slot_count: usize,
        actual_slot_count: usize,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Corruption,
            ErrorOrigin::Serialize,
            Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: diagnostic_code::RuntimeBoundaryCode::PersistedRowSlotCountMismatch,
            }),
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::RowLayout,
                    u64::from(row_layout),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ExpectedSlotCount,
                    expected_slot_count as u64,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualSlotCount,
                    actual_slot_count as u64,
                ),
            ],
        )
    }

    /// Construct the canonical persisted-row field decode corruption error.
    pub(crate) fn persisted_row_field_decode_failed(field_name: &str, _detail: impl Sized) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the compact persisted-row field decode corruption error.
    pub(crate) fn persisted_row_field_decode_corruption(_field_name: &str) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the canonical persisted-row field-kind decode corruption error.
    pub(crate) fn persisted_row_field_kind_decode_failed(
        field_name: &str,
        _field_kind: impl fmt::Debug,
        _detail: impl Sized,
    ) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row scalar-payload length corruption error.
    pub(crate) fn persisted_row_field_payload_exact_len_required(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row scalar-payload empty-body corruption error.
    pub(crate) fn persisted_row_field_payload_must_be_empty(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row scalar-payload invalid-byte corruption error.
    pub(crate) fn persisted_row_field_payload_invalid_byte(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row scalar-payload non-finite corruption error.
    pub(crate) fn persisted_row_field_payload_non_finite(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row invalid text payload corruption error.
    pub(crate) fn persisted_row_field_text_payload_invalid_utf8(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row structural slot-lookup invariant.
    pub(crate) fn persisted_row_slot_lookup_out_of_bounds(_model_path: &str, _slot: usize) -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical persisted-row structural slot-cache invariant.
    pub(crate) fn persisted_row_slot_cache_lookup_out_of_bounds(
        _model_path: &str,
        _slot: usize,
    ) -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical persisted-row primary-key decode corruption error.
    pub(crate) fn persisted_row_primary_key_not_primary_key_encodable(
        _data_key: impl fmt::Debug,
        _detail: impl Sized,
    ) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the canonical persisted-row missing primary-key slot corruption error.
    pub(crate) fn persisted_row_primary_key_slot_missing(_data_key: impl fmt::Debug) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the canonical persisted-row key mismatch corruption error.
    pub(crate) fn persisted_row_key_mismatch() -> Self {
        Self::store_corruption()
    }

    /// Construct the canonical persisted-row missing declared-field corruption error.
    pub(crate) fn persisted_row_declared_field_missing(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical reverse-index entry corruption error.
    pub(crate) fn reverse_index_entry_corrupted(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _index_key: impl fmt::Debug,
        _detail: impl Sized,
    ) -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical relation-target store missing internal error.
    pub(crate) fn relation_target_store_missing(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _store_path: &str,
        _detail: impl Sized,
    ) -> Self {
        Self::executor_internal()
    }

    /// Construct one accepted relation target primary-key arity mismatch.
    pub(crate) fn relation_target_primary_key_arity_mismatch(
        expected_arity: usize,
        actual_arity: usize,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Internal,
            ErrorOrigin::Executor,
            None,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ComponentKind,
                    diagnostic_code::DiagnosticComponentKind::RelationTargetPrimaryKey.raw(),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ExpectedArity,
                    expected_arity as u64,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualArity,
                    actual_arity as u64,
                ),
            ],
        )
    }

    /// Construct the canonical relation-target key decode corruption error.
    pub(crate) fn relation_target_key_decode_failed(
        _context_label: &str,
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _detail: impl Sized,
    ) -> Self {
        Self::identity_corruption()
    }

    /// Construct the canonical relation-target entity mismatch corruption error.
    pub(crate) fn relation_target_entity_mismatch(
        _context_label: &str,
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _target_entity_name: &str,
        expected_tag: u64,
        actual_tag: u64,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            None,
            vec![
                (
                    diagnostic_code::DiagnosticFactTag::ExpectedEntityTag,
                    expected_tag,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualEntityTag,
                    actual_tag,
                ),
            ],
        )
    }

    /// Construct the canonical relation-source row decode corruption error.
    pub(crate) fn relation_source_row_decode_failed(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _detail: impl Sized,
    ) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the canonical relation-source unsupported scalar relation-key corruption error.
    pub(crate) fn relation_source_row_unsupported_scalar_relation_key(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
    ) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the canonical unsupported relation key-kind corruption error.
    pub(crate) fn relation_source_row_unsupported_key_kind(_field_kind: impl fmt::Debug) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the canonical covering-component empty-payload corruption error.
    pub(crate) fn bytes_covering_component_payload_empty() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component truncated bool corruption error.
    pub(crate) fn bytes_covering_bool_payload_truncated() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component invalid-length corruption error.
    pub(crate) fn bytes_covering_component_payload_invalid_length() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component invalid-bool corruption error.
    pub(crate) fn bytes_covering_bool_payload_invalid_value() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component invalid text terminator corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_terminator() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component trailing-text corruption error.
    pub(crate) fn bytes_covering_text_payload_trailing_bytes() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component invalid-UTF-8 text corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_utf8() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component invalid text escape corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_escape_byte() -> Self {
        Self::index_corruption()
    }

    /// Construct the canonical covering-component missing text terminator corruption error.
    pub(crate) fn bytes_covering_text_payload_missing_terminator() -> Self {
        Self::index_corruption()
    }

    /// Construct an identity-origin corruption error.
    pub(crate) fn identity_corruption() -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Identity)
    }

    /// Construct the canonical identity-control-state corruption error.
    pub(crate) fn identity_state_corruption() -> Self {
        Self::identity_corruption()
    }

    /// Construct the typed stale high-water conflict for identity publication.
    pub(crate) fn identity_state_conflict() -> Self {
        Self::new(ErrorClass::Conflict, ErrorOrigin::Identity)
    }

    /// Construct the bounded identity-state inventory exhaustion error.
    pub(crate) fn identity_state_capacity_exhausted() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Identity)
    }

    /// Construct the exact unsigned identity-domain exhaustion error.
    pub(crate) fn identity_exhausted() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Identity)
    }

    /// Construct the bounded pre-key candidate-count exhaustion error.
    pub(crate) fn identity_candidate_count_exhausted() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Identity)
    }

    /// Construct a store-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_unsupported() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Store)
    }

    /// Construct the typed optimistic/idempotency conflict for schema application.
    pub(crate) fn schema_application_conflict() -> Self {
        Self::new(ErrorClass::Conflict, ErrorOrigin::Store)
    }

    /// Construct one typed source-migration lifecycle or planning result.
    pub(crate) fn schema_migration(reason: diagnostic_code::SchemaMigrationCode) -> Self {
        let class = match reason.diagnostic_code() {
            diagnostic_code::DiagnosticCode::RuntimeConflict => ErrorClass::Conflict,
            diagnostic_code::DiagnosticCode::RuntimeCorruption => ErrorClass::Corruption,
            diagnostic_code::DiagnosticCode::RuntimeUnsupported => ErrorClass::Unsupported,
            _ => ErrorClass::Internal,
        };
        Self {
            class,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(StoreError::SchemaMigration { reason })),
        }
    }

    /// Construct the canonical schema DDL publication race error.
    pub(crate) fn schema_ddl_publication_race_lost(_entity_path: &str) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(StoreError::SchemaDdlPublicationRaceLost)),
        }
    }

    /// Construct the canonical current physical-rewrite migration rejection.
    #[cfg(feature = "sql")]
    pub(crate) fn schema_ddl_rewrite_requires_migration(_entity_path: &str) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(
                StoreError::SchemaDdlRewriteRequiresMigration,
            )),
        }
    }

    /// Construct the fail-closed journal mutation-revision exhaustion error.
    pub(crate) fn journal_mutation_revision_exhausted() -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(
                StoreError::JournalMutationRevisionExhausted,
            )),
        }
    }

    /// Construct a bounded schema-transition resource rejection.
    pub(crate) fn schema_transition_budget_exceeded(
        resource: SchemaTransitionBudgetResource,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(
                StoreError::SchemaTransitionBudgetExceeded { resource },
            )),
        }
    }

    /// Construct the canonical unsupported persisted entity-tag store error.
    pub(crate) fn unsupported_entity_tag_in_data_store(
        _entity_tag: crate::types::EntityTag,
    ) -> Self {
        Self::store_unsupported()
    }

    /// Construct the canonical commit-memory id registration failure.
    #[cfg(not(test))]
    pub(crate) fn commit_memory_id_registration_failed(_err: impl Sized) -> Self {
        Self::store_internal()
    }

    /// Construct an index-origin unsupported error.
    pub(crate) fn index_unsupported() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Index)
    }

    /// Construct the canonical index-key component size-limit unsupported error.
    pub(crate) fn index_component_exceeds_max_size_at(
        entity_tag: u64,
        physical_generation: u64,
        component_index: usize,
        actual_length: usize,
        limit: usize,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Unsupported,
            ErrorOrigin::Index,
            None,
            vec![
                (diagnostic_code::DiagnosticFactTag::EntityTag, entity_tag),
                (
                    diagnostic_code::DiagnosticFactTag::PhysicalGeneration,
                    physical_generation,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ComponentIndex,
                    component_index as u64,
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ComponentKind,
                    diagnostic_code::DiagnosticComponentKind::IndexKeyComponent.raw(),
                ),
                (
                    diagnostic_code::DiagnosticFactTag::ActualLength,
                    actual_length as u64,
                ),
                (diagnostic_code::DiagnosticFactTag::Limit, limit as u64),
            ],
        )
    }

    /// Construct the canonical index-key component size-limit error when the
    /// generic caller has not retained one accepted index identity.
    pub(crate) fn index_component_exceeds_max_size() -> Self {
        Self::index_unsupported()
    }

    /// Construct a serialize-origin unsupported error.
    pub(crate) fn serialize_unsupported() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Serialize)
    }

    /// Construct a cursor-origin invalid-continuation error.
    pub(crate) fn cursor_invalid_continuation() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Cursor)
    }

    /// Construct a serialize-origin incompatible persisted-format error.
    pub(crate) fn serialize_incompatible_persisted_format() -> Self {
        Self::new(
            ErrorClass::IncompatiblePersistedFormat,
            ErrorOrigin::Serialize,
        )
    }

    /// Construct a query-origin unsupported error preserving one SQL parser
    /// unsupported-feature code in structured error detail.
    #[cfg(feature = "sql")]
    pub(crate) fn query_unsupported_sql_feature(feature: diagnostic_code::SqlFeatureCode) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::UnsupportedSqlFeature { feature },
            )),
        }
    }

    /// Construct a query-origin unsupported SQL lowering error preserving one
    /// compact lowering reason in structured error detail.
    #[cfg(feature = "sql")]
    pub(crate) fn query_sql_lowering(reason: diagnostic_code::SqlLoweringCode) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SqlLowering { reason })),
        }
    }

    /// Construct one query-origin SQL lowering error with bounded numeric context.
    #[cfg(feature = "sql")]
    pub(crate) fn query_sql_lowering_with_facts(
        reason: diagnostic_code::SqlLoweringCode,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Unsupported,
            ErrorOrigin::Query,
            Some(diagnostic_code::DiagnosticDetail::SqlLowering { reason }),
            facts,
        )
    }

    /// Construct a query-origin unsupported projection error preserving one
    /// compact projection reason in structured error detail.
    pub(crate) fn query_unsupported_projection(
        reason: diagnostic_code::QueryProjectionCode,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::UnsupportedProjection { reason },
            )),
        }
    }

    /// Construct a query-origin unsupported error preserving one SQL endpoint
    /// surface mismatch in structured error detail.
    #[cfg(feature = "sql")]
    pub(crate) fn query_sql_surface_mismatch(
        mismatch: diagnostic_code::SqlSurfaceMismatchCode,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SqlSurfaceMismatch {
                mismatch,
            })),
        }
    }

    /// Construct a query-origin unsupported SQL write boundary error.
    pub(crate) fn query_sql_write_boundary(
        boundary: diagnostic_code::SqlWriteBoundaryCode,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SqlWriteBoundary {
                boundary,
            })),
        }
    }

    /// Construct one query-origin SQL write-boundary error with bounded numeric context.
    pub(crate) fn query_sql_write_boundary_with_facts(
        boundary: diagnostic_code::SqlWriteBoundaryCode,
        facts: Vec<(diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        Self::with_diagnostic_facts(
            ErrorClass::Unsupported,
            ErrorOrigin::Query,
            Some(diagnostic_code::DiagnosticDetail::SqlWriteBoundary { boundary }),
            facts,
        )
    }

    pub fn store_not_found(_key: impl Sized) -> Self {
        Self {
            class: ErrorClass::NotFound,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(StoreError::NotFound)),
        }
    }

    /// Construct a standardized unsupported-entity-path error.
    pub fn unsupported_entity_path(_path: impl Sized) -> Self {
        Self::store_unsupported()
    }

    /// Construct an index-plan corruption error with a canonical prefix.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_corruption(origin: ErrorOrigin) -> Self {
        Self::new(ErrorClass::Corruption, origin)
    }

    /// Construct an index-plan corruption error for index-origin failures.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_index_corruption() -> Self {
        Self::index_plan_corruption(ErrorOrigin::Index)
    }

    /// Construct an index-plan corruption error for store-origin failures.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_store_corruption() -> Self {
        Self::index_plan_corruption(ErrorOrigin::Store)
    }

    /// Construct an index-plan corruption error for serialize-origin failures.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_serialize_corruption() -> Self {
        Self::index_plan_corruption(ErrorOrigin::Serialize)
    }

    /// Construct an index-plan invariant violation error with a canonical prefix.
    #[cfg(test)]
    pub(crate) fn index_plan_invariant(origin: ErrorOrigin) -> Self {
        Self::new(ErrorClass::InvariantViolation, origin)
    }

    /// Construct an index-plan invariant violation error for store-origin failures.
    #[cfg(test)]
    pub(crate) fn index_plan_store_invariant() -> Self {
        Self::index_plan_invariant(ErrorOrigin::Store)
    }

    /// Construct an index-origin conflict without claiming accepted identity.
    ///
    /// Live accepted uniqueness violations use compact accepted-constraint facts.
    /// Schema-domain staging and activation findings use this compact
    /// classification before an accepted write-admission diagnostic exists.
    pub(crate) fn index_conflict() -> Self {
        Self::new(ErrorClass::Conflict, ErrorOrigin::Index)
    }
}

impl From<diagnostic_code::QueryReadAdmissionCode> for InternalError {
    fn from(reason: diagnostic_code::QueryReadAdmissionCode) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(QueryErrorDetail::QueryReadAdmission {
                reason,
            })),
        }
    }
}

impl fmt::Debug for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(
            f,
            self.diagnostic_code(),
            self.detail
                .as_ref()
                .and_then(ErrorDetail::diagnostic_detail),
        )
    }
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for InternalError {}

///
/// ConstraintValuePathComponent
///
/// Stable accepted identity or finite-value coordinate in one targeted-rule
/// violation. Display names are deliberately absent so renames cannot change
/// the diagnostic identity.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ConstraintValuePathComponent {
    /// Persisted root field whose admitted value was traversed.
    RootField { field_id: u32 },

    /// Accepted record member selected by immutable composite/member identity.
    RecordMember {
        composite_type_id: u32,
        member_id: u32,
    },

    /// Tuple element selected by accepted composite identity and ordinal.
    TupleElement {
        composite_type_id: u32,
        ordinal: u32,
    },

    /// Transparent accepted newtype boundary.
    Newtype { composite_type_id: u32 },

    /// Selected accepted enum variant.
    EnumVariant { enum_type_id: u32, variant_id: u32 },

    /// List element in admitted order.
    ListElement { index: u32 },

    /// Set element in canonical admitted order.
    SetElement { index: u32 },

    /// Map key in canonical entry order.
    MapEntryKey { index: u32 },

    /// Map value in canonical entry order.
    MapEntryValue { index: u32 },
}

impl fmt::Display for ConstraintValuePathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootField { field_id } => write!(f, "field#{field_id}"),
            Self::RecordMember {
                composite_type_id,
                member_id,
            } => write!(f, "record#{composite_type_id}.member#{member_id}"),
            Self::TupleElement {
                composite_type_id,
                ordinal,
            } => write!(f, "tuple#{composite_type_id}[{ordinal}]"),
            Self::Newtype { composite_type_id } => write!(f, "newtype#{composite_type_id}"),
            Self::EnumVariant {
                enum_type_id,
                variant_id,
            } => write!(f, "enum#{enum_type_id}.variant#{variant_id}"),
            Self::ListElement { index } => write!(f, "list[{index}]"),
            Self::SetElement { index } => write!(f, "set[{index}]"),
            Self::MapEntryKey { index } => write!(f, "map[{index}].key"),
            Self::MapEntryValue { index } => write!(f, "map[{index}].value"),
        }
    }
}

///
/// ConstraintValuePath
///
/// Bounded typed path to the first deterministic failing value occurrence.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ConstraintValuePath {
    components: Vec<ConstraintValuePathComponent>,
}

impl ConstraintValuePath {
    /// Build one already-bounded accepted occurrence path.
    #[must_use]
    pub(crate) const fn new(components: Vec<ConstraintValuePathComponent>) -> Self {
        Self { components }
    }

    /// Borrow the stable accepted components.
    #[must_use]
    pub const fn components(&self) -> &[ConstraintValuePathComponent] {
        self.components.as_slice()
    }
}

impl fmt::Display for ConstraintValuePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (ordinal, component) in self.components.iter().enumerate() {
            if ordinal != 0 {
                f.write_str("/")?;
            }
            component.fmt(f)?;
        }
        Ok(())
    }
}

///
/// ConstraintValidationFindingOutput
///
/// Bounded historical validation evidence returned only by explicit schema
/// validation operations. Names are resolved by host tooling from the exact
/// accepted fingerprint and immutable numeric identities.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ConstraintValidationFindingOutput {
    accepted_schema_fingerprint: [u8; 16],
    entity_tag: u64,
    constraint_id: u32,
    primary_key: Vec<u8>,
    field_ids: Vec<u32>,
    value_path: Option<ConstraintValuePath>,
    error_code: u16,
}

impl ConstraintValidationFindingOutput {
    /// Build one already-bounded historical validation finding.
    #[must_use]
    pub(crate) const fn new(
        accepted_schema_fingerprint: [u8; 16],
        entity_tag: u64,
        constraint_id: u32,
        primary_key: Vec<u8>,
        field_ids: Vec<u32>,
        value_path: Option<ConstraintValuePath>,
        error_code: u16,
    ) -> Self {
        Self {
            accepted_schema_fingerprint,
            entity_tag,
            constraint_id,
            primary_key,
            field_ids,
            value_path,
            error_code,
        }
    }

    /// Return the exact accepted-schema fingerprint that binds every numeric identity.
    #[must_use]
    pub const fn accepted_schema_fingerprint(&self) -> [u8; 16] {
        self.accepted_schema_fingerprint
    }

    /// Return the stable accepted entity identity.
    #[must_use]
    pub const fn entity_tag(&self) -> u64 {
        self.entity_tag
    }

    /// Return the stable accepted constraint identity.
    #[must_use]
    pub const fn constraint_id(&self) -> u32 {
        self.constraint_id
    }

    /// Borrow the bounded canonical persisted primary-key locator.
    #[must_use]
    pub const fn primary_key(&self) -> &[u8] {
        self.primary_key.as_slice()
    }

    /// Borrow immutable accepted field identities implicated by the finding.
    #[must_use]
    pub const fn field_ids(&self) -> &[u32] {
        self.field_ids.as_slice()
    }

    /// Borrow the typed concrete value path for a targeted-rule violation.
    #[must_use]
    pub const fn value_path(&self) -> Option<&ConstraintValuePath> {
        self.value_path.as_ref()
    }

    /// Return the compact stable error code for this exact failure.
    #[must_use]
    pub const fn error_code(&self) -> diagnostic_code::ErrorCode {
        diagnostic_code::ErrorCode::from_raw(self.error_code)
    }

    /// Return the broad public error class derived from the compact code.
    #[must_use]
    pub const fn error_class(&self) -> diagnostic_code::ErrorClass {
        self.error_code().class()
    }
}

/// Complete bounded numeric authority needed to publish E210 or E212 facts.
#[derive(Clone)]
pub(crate) struct AcceptedConstraintFactContext {
    fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
    entity_tag: u64,
    constraint_id: u32,
    constraint_kind: diagnostic_code::DiagnosticConstraintKind,
    mutation: Option<MutationDiagnosticContext>,
    value_path: Option<ConstraintValuePath>,
}

impl AcceptedConstraintFactContext {
    #[must_use]
    pub(crate) fn write_admission(
        fingerprint_method: u8,
        accepted_schema_fingerprint: [u8; 16],
        entity_tag: u64,
        constraint_id: u32,
        constraint_kind: diagnostic_code::DiagnosticConstraintKind,
        mutation: Option<MutationDiagnosticContext>,
        value_path: Option<ConstraintValuePath>,
    ) -> Self {
        debug_assert!(mutation.is_none_or(|context| context.entity_tag() == entity_tag));
        Self {
            fingerprint_method,
            accepted_schema_fingerprint,
            entity_tag,
            constraint_id,
            constraint_kind,
            mutation,
            value_path,
        }
    }

    fn facts(self) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        let high = u64::from_be_bytes([
            self.accepted_schema_fingerprint[0],
            self.accepted_schema_fingerprint[1],
            self.accepted_schema_fingerprint[2],
            self.accepted_schema_fingerprint[3],
            self.accepted_schema_fingerprint[4],
            self.accepted_schema_fingerprint[5],
            self.accepted_schema_fingerprint[6],
            self.accepted_schema_fingerprint[7],
        ]);
        let low = u64::from_be_bytes([
            self.accepted_schema_fingerprint[8],
            self.accepted_schema_fingerprint[9],
            self.accepted_schema_fingerprint[10],
            self.accepted_schema_fingerprint[11],
            self.accepted_schema_fingerprint[12],
            self.accepted_schema_fingerprint[13],
            self.accepted_schema_fingerprint[14],
            self.accepted_schema_fingerprint[15],
        ]);
        let path_len = self
            .value_path
            .as_ref()
            .map_or(0, |path| path.components().len());
        let mutation_fact_count = self.mutation.map_or(0, |mutation| {
            1 + usize::from(mutation.batch_position.is_some())
        });
        let mut facts = Vec::with_capacity(7 + mutation_fact_count + path_len);
        facts.push((
            diagnostic_code::DiagnosticFactTag::AcceptedSchemaFingerprintMethod,
            u64::from(self.fingerprint_method),
        ));
        facts.push((
            diagnostic_code::DiagnosticFactTag::AcceptedSchemaFingerprintHigh,
            high,
        ));
        facts.push((
            diagnostic_code::DiagnosticFactTag::AcceptedSchemaFingerprintLow,
            low,
        ));
        facts.push((
            diagnostic_code::DiagnosticFactTag::EntityTag,
            self.entity_tag,
        ));
        facts.push((
            diagnostic_code::DiagnosticFactTag::ConstraintId,
            u64::from(self.constraint_id),
        ));
        facts.push((
            diagnostic_code::DiagnosticFactTag::ConstraintKind,
            self.constraint_kind.raw(),
        ));
        facts.push((
            diagnostic_code::DiagnosticFactTag::ConstraintContext,
            diagnostic_code::DiagnosticConstraintContext::WriteAdmission.raw(),
        ));
        if let Some(mutation) = self.mutation {
            mutation.append_operation_facts(&mut facts);
        }
        if let Some(path) = self.value_path {
            for component in path.components {
                facts.push(constraint_value_path_fact(component));
            }
        }
        debug_assert!(facts.len() <= diagnostic_code::MAX_PUBLIC_DIAGNOSTIC_FACTS);
        facts
    }
}

fn constraint_value_path_fact(
    component: ConstraintValuePathComponent,
) -> (diagnostic_code::DiagnosticFactTag, u64) {
    use diagnostic_code::DiagnosticFactTag;
    match component {
        ConstraintValuePathComponent::RootField { field_id } => {
            (DiagnosticFactTag::RootField, u64::from(field_id))
        }
        ConstraintValuePathComponent::RecordMember {
            composite_type_id,
            member_id,
        } => (
            DiagnosticFactTag::RecordMember,
            diagnostic_code::pack_u32_pair(composite_type_id, member_id),
        ),
        ConstraintValuePathComponent::TupleElement {
            composite_type_id,
            ordinal,
        } => (
            DiagnosticFactTag::TupleElement,
            diagnostic_code::pack_u32_pair(composite_type_id, ordinal),
        ),
        ConstraintValuePathComponent::Newtype { composite_type_id } => {
            (DiagnosticFactTag::Newtype, u64::from(composite_type_id))
        }
        ConstraintValuePathComponent::EnumVariant {
            enum_type_id,
            variant_id,
        } => (
            DiagnosticFactTag::EnumVariant,
            diagnostic_code::pack_u32_pair(enum_type_id, variant_id),
        ),
        ConstraintValuePathComponent::ListElement { index } => {
            (DiagnosticFactTag::ListElement, u64::from(index))
        }
        ConstraintValuePathComponent::SetElement { index } => {
            (DiagnosticFactTag::SetElement, u64::from(index))
        }
        ConstraintValuePathComponent::MapEntryKey { index } => {
            (DiagnosticFactTag::MapEntryKey, u64::from(index))
        }
        ConstraintValuePathComponent::MapEntryValue { index } => {
            (DiagnosticFactTag::MapEntryValue, u64::from(index))
        }
    }
}

///
/// ErrorDetail
///
/// Structured, origin-specific error detail carried by [`InternalError`].
/// This enum is intentionally extensible.
///

pub enum ErrorDetail {
    /// Compact code/detail plus safe numeric context for one public failure.
    DiagnosticFacts(Box<DiagnosticFactDetail>),
    /// Executor-owned mutation and query execution details.
    Executor(ExecutorErrorDetail),
    Store(StoreError),
    Query(QueryErrorDetail),
    Recovery(RecoveryErrorDetail),
    // Future-proofing:
    // Index(IndexError),
}

/// Executor-specific structured error detail.
pub enum ExecutorErrorDetail {
    /// A complete insert or replacement omitted one or more required fields.
    MutationRequiredFieldMissing,
    /// A logical mutation would move accepted managed time backward.
    MutationManagedTimestampRegression,
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
    /// Accepted row-constraint metadata or compiled state was inconsistent.
    AcceptedRowConstraintProgramCorrupt,
}

///
/// RecoveryErrorDetail
///
/// Recovery-origin structured error detail payload.
///

pub enum RecoveryErrorDetail {
    UnsupportedFormatVersion { found: Option<u16>, required: u16 },

    MalformedFormatMarker { reason: RecoveryFormatMarkerError },
}

/// Store boot-marker corruption classification.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RecoveryFormatMarkerError {
    Magic,
    Checksum,
    State,
}

impl RecoveryFormatMarkerError {
    const fn diagnostic_decode_reason(self) -> diagnostic_code::DiagnosticDecodeReason {
        match self {
            Self::Magic => diagnostic_code::DiagnosticDecodeReason::RecoveryMarkerMagic,
            Self::Checksum => diagnostic_code::DiagnosticDecodeReason::RecoveryMarkerChecksum,
            Self::State => diagnostic_code::DiagnosticDecodeReason::RecoveryMarkerState,
        }
    }
}

///
/// StoreError
///
/// Store-specific structured error detail.
/// Never returned directly; always wrapped in [`ErrorDetail::Store`].
///

pub enum StoreError {
    NotFound,

    Corrupt,

    InvariantViolation,

    SchemaDdlPublicationRaceLost,

    SchemaDdlRewriteRequiresMigration,

    SchemaMigration {
        reason: diagnostic_code::SchemaMigrationCode,
    },

    SchemaRowLayoutVersionExhausted,

    JournalMutationRevisionExhausted,

    SchemaTransitionBudgetExceeded {
        resource: SchemaTransitionBudgetResource,
    },

    /// A generated field would collide with an accepted DDL-owned slot.
    SchemaGeneratedFieldAfterDdlField,

    /// A live generated constraint activation no longer matches its proposal.
    SchemaGeneratedConstraintActivationStale,
}

///
/// QueryErrorDetail
///
/// Query-origin structured error detail payload.
///

pub enum QueryErrorDetail {
    NumericOverflow,

    NumericNotRepresentable,

    UnsupportedSqlFeature {
        feature: diagnostic_code::SqlFeatureCode,
    },

    SqlLowering {
        reason: diagnostic_code::SqlLoweringCode,
    },

    UnsupportedProjection {
        reason: diagnostic_code::QueryProjectionCode,
    },

    UnknownAggregateTargetField,

    QueryReadAdmission {
        reason: diagnostic_code::QueryReadAdmissionCode,
    },

    SqlSurfaceMismatch {
        mismatch: diagnostic_code::SqlSurfaceMismatchCode,
    },

    SqlWriteBoundary {
        boundary: diagnostic_code::SqlWriteBoundaryCode,
    },

    SchemaDdlAdmission {
        error: SchemaDdlAdmissionError,
    },

    StaleSchemaRevision,
}

impl fmt::Display for QueryErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }
}

impl std::error::Error for QueryErrorDetail {}

///
/// SchemaTransitionBudgetResource
///
/// Query-visible identity of the exact schema-transition resource cap that
/// rejected a complete validation or derived-state stage.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaTransitionBudgetResource {
    /// Number of physical deletion keys retained for replacement.
    DeletionKeys,
    /// Number of row-derived projection entries retained for validation.
    ProjectionEntries,
    /// Deterministic projection and physical-classification work units.
    ProjectionWorkUnits,
    /// Number of authoritative source rows.
    SourceRows,
    /// Cumulative bytes of authoritative source rows.
    SourceRowBytes,
    /// Retained raw payloads plus deterministic-sort workspace bytes.
    StagedRawBytes,
}

///
/// SchemaDdlAdmissionError
///
/// Stable query-visible SQL DDL admission reason. Human diagnostics may carry
/// extra version, fingerprint, and target facts beside this machine-readable
/// variant.
///

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum SchemaDdlAdmissionError {
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

    RowLayoutVersionExhausted,

    GeneratedIndexDropRejected,

    SchemaRewriteRequiresMigration,

    SchemaTransitionBudgetExceeded {
        resource: SchemaTransitionBudgetResource,
    },

    GeneratedFieldDefaultChangeRejected,

    GeneratedFieldNullabilityChangeRejected,
}

impl fmt::Display for SchemaDdlAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }
}

impl std::error::Error for SchemaDdlAdmissionError {}

impl fmt::Debug for ErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(f, self.diagnostic_code(), self.diagnostic_detail())
    }
}

impl fmt::Debug for ExecutorErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(f, self.diagnostic_code(), self.diagnostic_detail())
    }
}

impl fmt::Debug for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(f, self.diagnostic_code(), self.diagnostic_detail())
    }
}

impl fmt::Debug for QueryErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(f, self.diagnostic_code(), self.diagnostic_detail())
    }
}

impl fmt::Debug for RecoveryErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(f, self.diagnostic_code(), self.diagnostic_detail())
    }
}

impl fmt::Debug for RecoveryFormatMarkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(
            f,
            diagnostic_code::DiagnosticCode::RuntimeCorruption,
            Some(diagnostic_code::DiagnosticDetail::RuntimeKind {
                kind: diagnostic_code::RuntimeErrorKind::Corruption,
            }),
        )
    }
}

impl fmt::Debug for SchemaDdlAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_compact_diagnostic(
            f,
            diagnostic_code::DiagnosticCode::SchemaDdlAdmission,
            Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                reason: self.diagnostic_code(),
            }),
        )
    }
}

fn fmt_compact_diagnostic(
    f: &mut fmt::Formatter<'_>,
    code: diagnostic_code::DiagnosticCode,
    detail: Option<diagnostic_code::DiagnosticDetail>,
) -> fmt::Result {
    write!(
        f,
        "{}",
        diagnostic_code::ErrorCode::from_parts(code, detail).raw()
    )
}

impl ErrorDetail {
    /// Return the compact diagnostic code for this structured detail.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::DiagnosticFacts(detail) => detail.diagnostic.code(),
            Self::Executor(error) => error.diagnostic_code(),
            Self::Store(error) => error.diagnostic_code(),
            Self::Query(error) => error.diagnostic_code(),
            Self::Recovery(error) => error.diagnostic_code(),
        }
    }

    /// Return compact structured diagnostic detail when the payload carries one.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        match self {
            Self::DiagnosticFacts(detail) => detail.diagnostic.detail().copied(),
            Self::Executor(error) => error.diagnostic_detail(),
            Self::Store(error) => error.diagnostic_detail(),
            Self::Query(error) => error.diagnostic_detail(),
            Self::Recovery(error) => error.diagnostic_detail(),
        }
    }

    /// Project safe typed detail into canonical public numeric facts.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub fn diagnostic_facts(&self) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        match self {
            Self::DiagnosticFacts(detail) => detail.facts.clone(),
            Self::Executor(error) => error.diagnostic_facts(),
            Self::Query(error) => error.diagnostic_facts(),
            Self::Recovery(error) => error.diagnostic_facts(),
            Self::Store(_) => Vec::new(),
        }
    }
}

impl ExecutorErrorDetail {
    /// Return the compact diagnostic code for this executor detail.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::MutationRequiredFieldMissing
            | Self::MutationDatabaseOwnedFieldExplicit
            | Self::MutationBatchEmpty
            | Self::MutationBatchTooManyItems
            | Self::MutationBatchTooManyEntities
            | Self::MutationBatchStagedBytesExceeded
            | Self::MutationBatchResultBytesExceeded => {
                diagnostic_code::DiagnosticCode::RuntimeUnsupported
            }
            Self::MutationBatchStoreMismatch | Self::MutationBatchDuplicateKey => {
                diagnostic_code::DiagnosticCode::RuntimeConflict
            }
            Self::MutationManagedTimestampRegression => {
                diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
            }
            Self::AcceptedRowConstraintProgramCorrupt => {
                diagnostic_code::DiagnosticCode::RuntimeCorruption
            }
        }
    }

    /// Return compact structured diagnostic detail for this executor detail.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        match self {
            Self::MutationRequiredFieldMissing => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: diagnostic_code::RuntimeBoundaryCode::MutationRequiredFieldMissing,
                })
            }
            Self::MutationDatabaseOwnedFieldExplicit => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        diagnostic_code::RuntimeBoundaryCode::MutationDatabaseOwnedFieldExplicit,
                })
            }
            Self::MutationBatchEmpty => Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: diagnostic_code::RuntimeBoundaryCode::MutationBatchEmpty,
            }),
            Self::MutationBatchTooManyItems => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: diagnostic_code::RuntimeBoundaryCode::MutationBatchTooManyItems,
                })
            }
            Self::MutationBatchStagedBytesExceeded => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        diagnostic_code::RuntimeBoundaryCode::MutationBatchStagedBytesExceeded,
                })
            }
            Self::MutationBatchResultBytesExceeded => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        diagnostic_code::RuntimeBoundaryCode::MutationBatchResultBytesExceeded,
                })
            }
            Self::MutationBatchStoreMismatch => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: diagnostic_code::RuntimeBoundaryCode::MutationBatchStoreMismatch,
                })
            }
            Self::MutationBatchTooManyEntities => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: diagnostic_code::RuntimeBoundaryCode::MutationBatchTooManyEntities,
                })
            }
            Self::MutationBatchDuplicateKey => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: diagnostic_code::RuntimeBoundaryCode::MutationBatchDuplicateKey,
                })
            }
            Self::MutationManagedTimestampRegression => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        diagnostic_code::RuntimeBoundaryCode::MutationManagedTimestampRegression,
                })
            }
            Self::AcceptedRowConstraintProgramCorrupt => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        diagnostic_code::RuntimeBoundaryCode::AcceptedRowConstraintProgramCorrupt,
                })
            }
        }
    }

    /// Project safe mutation detail into canonical public numeric facts.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub const fn diagnostic_facts(&self) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        Vec::new()
    }
}

impl RecoveryErrorDetail {
    /// Return the compact diagnostic code for this recovery detail.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::UnsupportedFormatVersion { .. } => {
                diagnostic_code::DiagnosticCode::RuntimeIncompatiblePersistedFormat
            }
            Self::MalformedFormatMarker { .. } => {
                diagnostic_code::DiagnosticCode::RuntimeCorruption
            }
        }
    }

    /// Return compact structured diagnostic detail for this recovery detail.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        let kind = match self {
            Self::UnsupportedFormatVersion { .. } => {
                diagnostic_code::RuntimeErrorKind::IncompatiblePersistedFormat
            }
            Self::MalformedFormatMarker { .. } => diagnostic_code::RuntimeErrorKind::Corruption,
        };

        Some(diagnostic_code::DiagnosticDetail::RuntimeKind { kind })
    }

    /// Project database-format recovery context without retaining marker bytes.
    #[must_use]
    pub fn diagnostic_facts(&self) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        match self {
            Self::UnsupportedFormatVersion { found, required } => {
                let mut facts = Vec::with_capacity(usize::from(found.is_some()) + 1);
                facts.push((
                    diagnostic_code::DiagnosticFactTag::ExpectedVersion,
                    u64::from(*required),
                ));
                if let Some(found) = found {
                    facts.push((
                        diagnostic_code::DiagnosticFactTag::ActualVersion,
                        u64::from(*found),
                    ));
                }
                facts
            }
            Self::MalformedFormatMarker { reason } => vec![(
                diagnostic_code::DiagnosticFactTag::DecodeReason,
                reason.diagnostic_decode_reason().raw(),
            )],
        }
    }
}

impl StoreError {
    /// Return the compact diagnostic code for this store detail.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::NotFound => diagnostic_code::DiagnosticCode::StoreNotFound,
            Self::Corrupt => diagnostic_code::DiagnosticCode::StoreCorruption,
            Self::InvariantViolation => diagnostic_code::DiagnosticCode::StoreInvariantViolation,
            Self::SchemaDdlPublicationRaceLost
            | Self::SchemaDdlRewriteRequiresMigration
            | Self::SchemaRowLayoutVersionExhausted
            | Self::SchemaTransitionBudgetExceeded { .. } => {
                diagnostic_code::DiagnosticCode::SchemaDdlAdmission
            }
            Self::JournalMutationRevisionExhausted | Self::SchemaGeneratedFieldAfterDdlField => {
                diagnostic_code::DiagnosticCode::RuntimeUnsupported
            }
            Self::SchemaGeneratedConstraintActivationStale => {
                diagnostic_code::DiagnosticCode::RuntimeConflict
            }
            Self::SchemaMigration { reason } => reason.diagnostic_code(),
        }
    }

    /// Return compact structured diagnostic detail when the store error has one.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        match self {
            Self::SchemaDdlPublicationRaceLost => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
                })
            }
            Self::SchemaDdlRewriteRequiresMigration => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: diagnostic_code::SchemaDdlAdmissionCode::SchemaRewriteRequiresMigration,
                })
            }
            Self::SchemaMigration { reason } => {
                Some(diagnostic_code::DiagnosticDetail::SchemaMigration { reason: *reason })
            }
            Self::SchemaRowLayoutVersionExhausted => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: diagnostic_code::SchemaDdlAdmissionCode::RowLayoutVersionExhausted,
                })
            }
            Self::JournalMutationRevisionExhausted => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        diagnostic_code::RuntimeBoundaryCode::JournalMutationRevisionExhausted,
                })
            }
            Self::SchemaTransitionBudgetExceeded { .. } => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: diagnostic_code::SchemaDdlAdmissionCode::SchemaTransitionBudgetExceeded,
                })
            }
            Self::SchemaGeneratedFieldAfterDdlField => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: diagnostic_code::RuntimeBoundaryCode::GeneratedFieldAfterDdlField,
                })
            }
            Self::SchemaGeneratedConstraintActivationStale => {
                Some(diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        diagnostic_code::RuntimeBoundaryCode::GeneratedConstraintActivationStale,
                })
            }
            Self::NotFound | Self::Corrupt | Self::InvariantViolation => None,
        }
    }
}

impl QueryErrorDetail {
    /// Return the compact diagnostic code for this query detail.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::NumericOverflow => diagnostic_code::DiagnosticCode::QueryNumericOverflow,
            Self::NumericNotRepresentable => {
                diagnostic_code::DiagnosticCode::QueryNumericNotRepresentable
            }
            Self::UnsupportedSqlFeature { .. } => {
                diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature
            }
            Self::SqlLowering { .. } => diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature,
            Self::UnsupportedProjection { .. } => {
                diagnostic_code::DiagnosticCode::QueryUnsupportedProjection
            }
            Self::UnknownAggregateTargetField => {
                diagnostic_code::DiagnosticCode::QueryUnknownAggregateTargetField
            }
            Self::QueryReadAdmission { .. } => diagnostic_code::DiagnosticCode::QueryReadAdmission,
            Self::SqlSurfaceMismatch { .. } => {
                diagnostic_code::DiagnosticCode::QuerySqlSurfaceMismatch
            }
            Self::SqlWriteBoundary { .. } => diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
            Self::SchemaDdlAdmission { .. } => diagnostic_code::DiagnosticCode::SchemaDdlAdmission,
            Self::StaleSchemaRevision => diagnostic_code::DiagnosticCode::RuntimeConflict,
        }
    }

    /// Return compact structured diagnostic detail when the query detail has one.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        match self {
            Self::UnsupportedSqlFeature { feature } => {
                Some(diagnostic_code::DiagnosticDetail::UnsupportedSqlFeature { feature: *feature })
            }
            Self::SqlLowering { reason } => {
                Some(diagnostic_code::DiagnosticDetail::SqlLowering { reason: *reason })
            }
            Self::UnsupportedProjection { reason } => {
                Some(diagnostic_code::DiagnosticDetail::QueryProjection { reason: *reason })
            }
            Self::QueryReadAdmission { reason } => {
                Some(diagnostic_code::DiagnosticDetail::QueryReadAdmission { reason: *reason })
            }
            Self::SqlSurfaceMismatch { mismatch } => {
                Some(diagnostic_code::DiagnosticDetail::SqlSurfaceMismatch {
                    mismatch: *mismatch,
                })
            }
            Self::SqlWriteBoundary { boundary } => {
                Some(diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
                    boundary: *boundary,
                })
            }
            Self::SchemaDdlAdmission { error } => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: error.diagnostic_code(),
                })
            }
            Self::NumericOverflow
            | Self::NumericNotRepresentable
            | Self::UnknownAggregateTargetField
            | Self::StaleSchemaRevision => None,
        }
    }

    /// Project safe query detail into canonical public numeric facts.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub const fn diagnostic_facts(&self) -> Vec<(diagnostic_code::DiagnosticFactTag, u64)> {
        Vec::new()
    }
}

impl SchemaDdlAdmissionError {
    /// Return the compact diagnostic code for this SQL DDL admission reason.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::SchemaDdlAdmissionCode {
        match self {
            Self::MissingExpectedSchemaVersion => {
                diagnostic_code::SchemaDdlAdmissionCode::MissingExpectedSchemaVersion
            }
            Self::MissingNextSchemaVersion => {
                diagnostic_code::SchemaDdlAdmissionCode::MissingNextSchemaVersion
            }
            Self::StaleExpectedSchemaVersion => {
                diagnostic_code::SchemaDdlAdmissionCode::StaleExpectedSchemaVersion
            }
            Self::InvalidExpectedSchemaVersion => {
                diagnostic_code::SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion
            }
            Self::InvalidNextSchemaVersion => {
                diagnostic_code::SchemaDdlAdmissionCode::InvalidNextSchemaVersion
            }
            Self::AcceptedSchemaChangeWithoutVersionBump => {
                diagnostic_code::SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump
            }
            Self::EmptyVersionBump => diagnostic_code::SchemaDdlAdmissionCode::EmptyVersionBump,
            Self::VersionGap => diagnostic_code::SchemaDdlAdmissionCode::VersionGap,
            Self::VersionRollback => diagnostic_code::SchemaDdlAdmissionCode::VersionRollback,
            Self::FingerprintMethodMismatch => {
                diagnostic_code::SchemaDdlAdmissionCode::FingerprintMethodMismatch
            }
            Self::UnsupportedTransitionClass => {
                diagnostic_code::SchemaDdlAdmissionCode::UnsupportedTransitionClass
            }
            Self::PhysicalRunnerMissing => {
                diagnostic_code::SchemaDdlAdmissionCode::PhysicalRunnerMissing
            }
            Self::ValidationFailed => diagnostic_code::SchemaDdlAdmissionCode::ValidationFailed,
            Self::PublicationRaceLost => {
                diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost
            }
            Self::InvalidAddColumnDefault => {
                diagnostic_code::SchemaDdlAdmissionCode::InvalidAddColumnDefault
            }
            Self::InvalidAlterColumnDefault => {
                diagnostic_code::SchemaDdlAdmissionCode::InvalidAlterColumnDefault
            }
            Self::GeneratedIndexDropRejected => {
                diagnostic_code::SchemaDdlAdmissionCode::GeneratedIndexDropRejected
            }
            Self::SchemaRewriteRequiresMigration => {
                diagnostic_code::SchemaDdlAdmissionCode::SchemaRewriteRequiresMigration
            }
            Self::SchemaTransitionBudgetExceeded { .. } => {
                diagnostic_code::SchemaDdlAdmissionCode::SchemaTransitionBudgetExceeded
            }
            Self::GeneratedFieldDefaultChangeRejected => {
                diagnostic_code::SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected
            }
            Self::GeneratedFieldNullabilityChangeRejected => {
                diagnostic_code::SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected
            }
            Self::RowLayoutVersionExhausted => {
                diagnostic_code::SchemaDdlAdmissionCode::RowLayoutVersionExhausted
            }
        }
    }
}

///
/// ErrorClass
/// Internal error taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ErrorClass {
    Corruption,
    IncompatiblePersistedFormat,
    NotFound,
    Internal,
    Conflict,
    Unsupported,
    InvariantViolation,
}

impl ErrorClass {
    /// Return a compact diagnostic code for this broad class and origin pair.
    #[must_use]
    pub const fn diagnostic_code(self, origin: ErrorOrigin) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::Corruption if matches!(origin, ErrorOrigin::Store) => {
                diagnostic_code::DiagnosticCode::StoreCorruption
            }
            Self::Corruption => diagnostic_code::DiagnosticCode::RuntimeCorruption,
            Self::IncompatiblePersistedFormat => {
                diagnostic_code::DiagnosticCode::RuntimeIncompatiblePersistedFormat
            }
            Self::NotFound if matches!(origin, ErrorOrigin::Store) => {
                diagnostic_code::DiagnosticCode::StoreNotFound
            }
            Self::NotFound => diagnostic_code::DiagnosticCode::RuntimeNotFound,
            Self::Internal => diagnostic_code::DiagnosticCode::RuntimeInternal,
            Self::Conflict => diagnostic_code::DiagnosticCode::RuntimeConflict,
            Self::Unsupported if matches!(origin, ErrorOrigin::Cursor) => {
                diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor
            }
            Self::Unsupported => diagnostic_code::DiagnosticCode::RuntimeUnsupported,
            Self::InvariantViolation if matches!(origin, ErrorOrigin::Store) => {
                diagnostic_code::DiagnosticCode::StoreInvariantViolation
            }
            Self::InvariantViolation => diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
        }
    }
}

impl fmt::Debug for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

///
/// ErrorOrigin
/// Internal origin taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ErrorOrigin {
    Serialize,
    Store,
    Index,
    Identity,
    Query,
    Planner,
    Cursor,
    Recovery,
    Response,
    Executor,
    Interface,
}

impl ErrorOrigin {
    /// Return the compact diagnostic origin for this internal origin.
    #[must_use]
    pub const fn diagnostic_origin(self) -> diagnostic_code::ErrorOrigin {
        match self {
            Self::Serialize => diagnostic_code::ErrorOrigin::Serialize,
            Self::Store => diagnostic_code::ErrorOrigin::Store,
            Self::Index => diagnostic_code::ErrorOrigin::Index,
            Self::Identity => diagnostic_code::ErrorOrigin::Identity,
            Self::Query => diagnostic_code::ErrorOrigin::Query,
            Self::Planner => diagnostic_code::ErrorOrigin::Planner,
            Self::Cursor => diagnostic_code::ErrorOrigin::Cursor,
            Self::Recovery => diagnostic_code::ErrorOrigin::Recovery,
            Self::Response => diagnostic_code::ErrorOrigin::Response,
            Self::Executor => diagnostic_code::ErrorOrigin::Executor,
            Self::Interface => diagnostic_code::ErrorOrigin::Interface,
        }
    }
}

impl fmt::Debug for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}
