//! Module: error
//!
//! Defines the canonical runtime error taxonomy for `icydb-core`.
//! This module owns the shared error classes, origins, details, and
//! constructor entry points used across storage, planning, execution, and
//! serialization boundaries.

#[cfg(test)]
mod tests;

use icydb_diagnostic_code as diagnostic_code;
use std::fmt;
use thiserror::Error as ThisError;

const COMPACT_QUERY_DIAGNOSTIC_MESSAGE: &str = "query diagnostic";
const COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE: &str = "runtime diagnostic";
const COMPACT_SCHEMA_DDL_STORE_MESSAGE: &str = "schema DDL diagnostic";
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
//        InternalError::new(class, origin, ...)
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

///
/// InternalError
///
/// Structured runtime error with a stable internal classification.
/// Not a stable API; intended for internal use and may change without notice.
///

#[derive(Debug, ThisError)]
#[error("{message}")]
pub struct InternalError {
    pub(crate) class: ErrorClass,
    pub(crate) origin: ErrorOrigin,
    pub(crate) message: String,

    /// Optional structured error detail.
    /// The variant (if present) must correspond to `origin`.
    pub(crate) detail: Option<ErrorDetail>,
}

impl InternalError {
    /// Construct an InternalError with optional origin-specific detail.
    /// This constructor provides default StoreError details for certain
    /// (class, origin) combinations but does not guarantee a detail payload.
    #[cold]
    #[inline(never)]
    pub fn new(class: ErrorClass, origin: ErrorOrigin, _message: impl Into<String>) -> Self {
        let message = compact_message_for(class, origin);

        let detail = match (class, origin) {
            (ErrorClass::Corruption, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::Corrupt {
                    message: message.to_string(),
                }))
            }
            (ErrorClass::InvariantViolation, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::InvariantViolation {
                    message: message.to_string(),
                }))
            }
            _ => None,
        };

        Self {
            class,
            origin,
            message: message.to_string(),
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
    pub fn message(&self) -> &str {
        &self.message
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
        self.message
    }

    /// Construct an error while preserving an explicit class/origin taxonomy pair.
    #[cold]
    #[inline(never)]
    pub(crate) fn classified(
        class: ErrorClass,
        origin: ErrorOrigin,
        message: impl Into<String>,
    ) -> Self {
        Self::new(class, origin, message)
    }

    /// Preserve taxonomy while discarding legacy prose relabeling.
    #[cold]
    #[inline(never)]
    pub(crate) fn with_message(self, _message: impl Into<String>) -> Self {
        self
    }

    /// Rebuild this error with a new origin while preserving class taxonomy.
    ///
    /// Origin-scoped detail payloads are intentionally dropped when re-origining.
    #[cold]
    #[inline(never)]
    pub(crate) fn with_origin(self, origin: ErrorOrigin) -> Self {
        Self::classified(self.class, origin, "")
    }

    /// Construct an index-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_invariant(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            COMPACT_INDEX_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical index field-count invariant for key building.
    pub(crate) fn index_key_field_count_exceeds_max(
        _index_name: &str,
        _field_count: usize,
        _max_fields: usize,
    ) -> Self {
        Self::index_invariant(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical index-key source-field-missing-on-model invariant.
    pub(crate) fn index_key_item_field_missing_on_entity_model(_field: &str) -> Self {
        Self::index_invariant(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical index-key source-field-missing-on-row invariant.
    pub(crate) fn index_key_item_field_missing_on_lookup_row(_field: &str) -> Self {
        Self::index_invariant(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical index-expression source-type mismatch invariant.
    pub(crate) fn index_expression_source_type_mismatch(
        _index_name: &str,
        _expression: impl fmt::Display,
        _expected: &str,
        _source_label: &str,
    ) -> Self {
        Self::index_invariant(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a planner-origin invariant violation for executor-boundary
    /// contract drift.
    #[cold]
    #[inline(never)]
    pub(crate) fn planner_executor_invariant(_reason: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Planner,
            COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a query-origin invariant violation for executor-boundary
    /// contract drift.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_executor_invariant(_reason: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            COMPACT_QUERY_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a cursor-origin invariant violation for executor-boundary
    /// contract drift.
    #[cold]
    #[inline(never)]
    pub(crate) fn cursor_executor_invariant(_reason: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Cursor,
            COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct an executor-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_invariant(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Executor,
            COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct an executor-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_internal(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Internal,
            ErrorOrigin::Executor,
            COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct an executor-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_unsupported(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Executor,
            COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct an executor-origin save-preflight primary-key missing invariant.
    pub(crate) fn mutation_entity_primary_key_missing(
        _entity_path: &str,
        _field_name: &str,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight primary-key invalid-value invariant.
    pub(crate) fn mutation_entity_primary_key_invalid_value(
        _entity_path: &str,
        _field_name: &str,
        _value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight primary-key type mismatch invariant.
    pub(crate) fn mutation_entity_primary_key_type_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight primary-key identity mismatch invariant.
    pub(crate) fn mutation_entity_primary_key_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _field_value: &crate::value::Value,
        _identity_key: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight field-missing invariant.
    pub(crate) fn mutation_entity_field_missing(
        _entity_path: &str,
        _field_name: &str,
        _indexed: bool,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin sparse structural patch required-field invariant.
    pub(crate) fn mutation_structural_patch_required_field_missing(
        _entity_path: &str,
        _field_name: &str,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight field-type mismatch invariant.
    pub(crate) fn mutation_entity_field_type_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin generated-field authored-write rejection.
    pub(crate) fn mutation_generated_field_explicit(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_unsupported(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin typed create omission rejection.
    #[must_use]
    pub fn mutation_create_missing_authored_fields(_entity_path: &str, _field_names: &str) -> Self {
        Self::executor_unsupported(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin mutation result invariant.
    ///
    /// This constructor lands ahead of the public structural mutation surface,
    /// so the library target may not route through it until that caller exists.
    pub(crate) fn mutation_structural_after_image_invalid(
        _entity_path: &str,
        _data_key: impl fmt::Display,
        _detail: impl AsRef<str>,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin mutation unknown-field invariant.
    pub(crate) fn mutation_structural_field_unknown(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight decimal-scale unsupported error.
    pub(crate) fn mutation_decimal_scale_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _expected_scale: impl fmt::Display,
        _actual_scale: impl fmt::Display,
    ) -> Self {
        Self::executor_unsupported(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight text-length unsupported error.
    pub(crate) fn mutation_text_max_len_exceeded(
        _entity_path: &str,
        _field_name: &str,
        _max_len: impl fmt::Display,
        _actual_len: impl fmt::Display,
    ) -> Self {
        Self::executor_unsupported(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight set-encoding invariant.
    pub(crate) fn mutation_set_field_list_required(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight set-canonicality invariant.
    pub(crate) fn mutation_set_field_not_canonical(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight map-encoding invariant.
    pub(crate) fn mutation_map_field_map_required(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight map-entry invariant.
    pub(crate) fn mutation_map_field_entries_invalid(
        _entity_path: &str,
        _field_name: &str,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin save-preflight map-canonicality invariant.
    pub(crate) fn mutation_map_field_entries_not_canonical(
        _entity_path: &str,
        _field_name: &str,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin scalar page invariant for ordering before filtering.
    pub(crate) fn scalar_page_ordering_after_filtering_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin scalar page invariant for missing order at the cursor boundary.
    pub(crate) fn scalar_page_cursor_boundary_order_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin scalar page invariant for cursor-before-ordering drift.
    pub(crate) fn scalar_page_cursor_boundary_after_ordering_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin scalar page invariant for pagination-before-ordering drift.
    pub(crate) fn scalar_page_pagination_after_ordering_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin scalar page invariant for delete-limit-before-ordering drift.
    pub(crate) fn scalar_page_delete_limit_after_ordering_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin load-runtime invariant for scalar-mode payload mismatch.
    pub(crate) fn load_runtime_scalar_payload_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin load-runtime invariant for grouped-mode payload mismatch.
    pub(crate) fn load_runtime_grouped_payload_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin load-surface invariant for scalar-page payload mismatch.
    pub(crate) fn load_runtime_scalar_surface_payload_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin load-surface invariant for grouped-page payload mismatch.
    pub(crate) fn load_runtime_grouped_surface_payload_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin load-entrypoint invariant for non-load plans.
    pub(crate) fn load_executor_load_plan_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin delete-entrypoint unsupported grouped-mode error.
    pub(crate) fn delete_executor_grouped_unsupported() -> Self {
        Self::executor_unsupported(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin delete-entrypoint invariant for non-delete plans.
    pub(crate) fn delete_executor_delete_plan_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin aggregate kernel invariant for fold-mode contract drift.
    pub(crate) fn aggregate_fold_mode_terminal_contract_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin fast-stream invariant for route kind/request mismatch.
    pub(crate) fn fast_stream_route_kind_request_match_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin scan invariant for missing index-prefix executable specs.
    pub(crate) fn secondary_index_prefix_spec_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a query-origin scan invariant for missing index-range executable specs.
    pub(crate) fn index_range_limit_spec_required() -> Self {
        Self::query_executor_invariant(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin mutation unsupported error for duplicate atomic save keys.
    pub(crate) fn mutation_atomic_save_duplicate_key(
        _entity_path: &str,
        _key: impl fmt::Display,
    ) -> Self {
        Self::executor_unsupported(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an executor-origin mutation invariant for index-store generation drift.
    pub(crate) fn mutation_index_store_generation_changed(
        _expected_generation: u64,
        _observed_generation: u64,
    ) -> Self {
        Self::executor_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a planner-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn planner_invariant(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Planner,
            COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a planner-origin invalid-logical-plan invariant.
    pub(crate) fn query_invalid_logical_plan(_reason: impl Into<String>) -> Self {
        Self::planner_invariant(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a store-origin invariant violation.
    pub(crate) fn store_invariant() -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Store,
            COMPACT_STORE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical duplicate runtime-hook entity-tag invariant.
    pub(crate) fn duplicate_runtime_hooks_for_entity_tag(
        _entity_tag: crate::types::EntityTag,
    ) -> Self {
        Self::store_invariant()
    }

    /// Construct the canonical duplicate runtime-hook entity-path invariant.
    pub(crate) fn duplicate_runtime_hooks_for_entity_path(_entity_path: &str) -> Self {
        Self::store_invariant()
    }

    /// Construct a store-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_internal(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            COMPACT_STORE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical unconfigured commit-memory id internal error.
    pub(crate) fn commit_memory_id_unconfigured() -> Self {
        Self::store_internal(
            "commit memory id is not configured; initialize recovery before commit store access",
        )
    }

    /// Construct the canonical commit-memory id mismatch internal error.
    pub(crate) fn commit_memory_id_mismatch(_cached_id: u8, _configured_id: u8) -> Self {
        Self::store_internal(COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical commit-memory stable-key mismatch internal error.
    pub(crate) fn commit_memory_stable_key_mismatch(
        _cached_key: &str,
        _configured_key: &str,
    ) -> Self {
        Self::store_internal(COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical missing rollback-row invariant for delete execution.
    pub(crate) fn delete_rollback_row_required() -> Self {
        Self::store_internal("missing raw row for delete rollback")
    }

    /// Construct the canonical recovery-integrity totals corruption error.
    pub(crate) fn recovery_integrity_validation_failed(
        _missing_index_entries: u64,
        _divergent_index_entries: u64,
        _orphan_index_references: u64,
    ) -> Self {
        Self::store_corruption()
    }

    /// Construct an index-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_internal(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Internal,
            ErrorOrigin::Index,
            COMPACT_INDEX_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical missing old entity-key internal error for structural index removal.
    pub(crate) fn structural_index_removal_entity_key_required() -> Self {
        Self::index_internal("missing old entity key for structural index removal")
    }

    /// Construct the canonical missing new entity-key internal error for structural index insertion.
    pub(crate) fn structural_index_insertion_entity_key_required() -> Self {
        Self::index_internal("missing new entity key for structural index insertion")
    }

    /// Construct the canonical missing old entity-key internal error for index commit-op removal.
    pub(crate) fn index_commit_op_old_entity_key_required() -> Self {
        Self::index_internal("missing old entity key for index removal")
    }

    /// Construct the canonical missing new entity-key internal error for index commit-op insertion.
    pub(crate) fn index_commit_op_new_entity_key_required() -> Self {
        Self::index_internal("missing new entity key for index insertion")
    }

    /// Construct a query-origin internal error.
    #[cfg(test)]
    pub(crate) fn query_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Query, message.into())
    }

    /// Construct a query-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_unsupported(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Query,
            COMPACT_QUERY_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a query-origin SQL DDL admission error with structured detail.
    #[cold]
    #[inline(never)]
    #[cfg(feature = "sql")]
    pub(crate) fn query_schema_ddl_admission(error: SchemaDdlAdmissionError) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
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
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
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
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::NumericNotRepresentable,
            )),
        }
    }

    /// Construct a serialize-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn serialize_internal(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Internal,
            ErrorOrigin::Serialize,
            COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical persisted-row encode internal error.
    pub(crate) fn persisted_row_encode_failed(_detail: impl fmt::Display) -> Self {
        Self::persisted_row_encode_internal()
    }

    /// Construct the compact persisted-row encode internal error.
    pub(crate) fn persisted_row_encode_internal() -> Self {
        Self::serialize_internal("row encode failed")
    }

    /// Construct the canonical persisted-row field encode internal error.
    pub(crate) fn persisted_row_field_encode_failed(
        field_name: &str,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::persisted_row_field_encode_internal(field_name)
    }

    /// Construct the compact persisted-row field encode internal error.
    pub(crate) fn persisted_row_field_encode_internal(_field_name: &str) -> Self {
        Self::persisted_row_encode_internal()
    }

    /// Construct the canonical bytes(field) value encode internal error.
    pub(crate) fn bytes_field_value_encode_failed(_detail: impl fmt::Display) -> Self {
        Self::serialize_internal(COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a store-origin corruption error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_corruption() -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            COMPACT_STORE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a store-origin commit-marker corruption error.
    pub(crate) fn commit_corruption(_detail: impl fmt::Display) -> Self {
        Self::store_corruption()
    }

    /// Construct a store-origin commit-marker component corruption error.
    pub(crate) fn commit_component_corruption(
        _component: &str,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::commit_corruption(COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical commit-marker id generation internal error.
    pub(crate) fn commit_id_generation_failed(_detail: impl fmt::Display) -> Self {
        Self::store_internal(COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical commit-marker payload u32-length-limit error.
    pub(crate) fn commit_marker_payload_exceeds_u32_length_limit(
        _label: &str,
        _len: usize,
    ) -> Self {
        Self::store_unsupported()
    }

    /// Construct the canonical commit-marker component invalid-length corruption error.
    pub(crate) fn commit_component_length_invalid(
        _component: &str,
        _len: usize,
        _expected: impl fmt::Display,
    ) -> Self {
        Self::commit_corruption(COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical commit-marker max-size corruption error.
    pub(crate) fn commit_marker_exceeds_max_size(_size: usize, _max_size: u32) -> Self {
        Self::commit_corruption(COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical commit-control slot max-size unsupported error.
    pub(crate) fn commit_control_slot_exceeds_max_size(_size: usize, _max_size: u32) -> Self {
        Self::store_unsupported()
    }

    /// Construct the canonical commit-control marker-bytes length-limit error.
    pub(crate) fn commit_control_slot_marker_bytes_exceed_u32_length_limit(_size: usize) -> Self {
        Self::store_unsupported()
    }

    /// Construct the canonical startup index-rebuild invalid-data-key corruption error.
    pub(crate) fn startup_index_rebuild_invalid_data_key(
        _store_path: &str,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::store_corruption()
    }

    /// Construct an index-origin corruption error.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_corruption(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            COMPACT_INDEX_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical unique-validation corruption wrapper.
    pub(crate) fn index_unique_validation_corruption() -> Self {
        Self::index_plan_index_corruption("unique index corrupted")
    }

    /// Construct the canonical structural index-entry corruption wrapper.
    pub(crate) fn structural_index_entry_corruption() -> Self {
        Self::index_plan_index_corruption("index entry corrupted")
    }

    /// Construct the canonical missing new entity-key invariant during unique validation.
    pub(crate) fn index_unique_validation_entity_key_required() -> Self {
        Self::index_invariant("missing entity key during unique validation")
    }

    /// Construct the canonical unique-validation structural row-decode corruption error.
    pub(crate) fn index_unique_validation_row_deserialize_failed() -> Self {
        Self::index_plan_serialize_corruption("unique row decode failed")
    }

    /// Construct the canonical unique-validation primary-key slot decode corruption error.
    pub(crate) fn index_unique_validation_primary_key_decode_failed() -> Self {
        Self::index_plan_serialize_corruption("unique primary key decode failed")
    }

    /// Construct the canonical unique-validation stored key rebuild corruption error.
    pub(crate) fn index_unique_validation_key_rebuild_failed() -> Self {
        Self::index_plan_serialize_corruption("unique key rebuild failed")
    }

    /// Construct the canonical unique-validation missing-row corruption error.
    pub(crate) fn index_unique_validation_row_required() -> Self {
        Self::index_plan_store_corruption()
    }

    /// Construct the canonical index-only predicate missing-component invariant.
    pub(crate) fn index_only_predicate_component_required() -> Self {
        Self::index_invariant("index-only predicate program referenced missing index component")
    }

    /// Construct the canonical index-scan continuation-envelope invariant.
    pub(crate) fn index_scan_continuation_anchor_within_envelope_required() -> Self {
        Self::index_invariant(
            "index-range continuation anchor is outside the requested range envelope",
        )
    }

    /// Construct the canonical index-scan continuation-advancement invariant.
    pub(crate) fn index_scan_continuation_advancement_required() -> Self {
        Self::index_invariant("index-range continuation scan did not advance beyond the anchor")
    }

    /// Construct the canonical index-scan key-decode corruption error.
    pub(crate) fn index_scan_key_corrupted_during(
        _context: &'static str,
        _err: impl fmt::Display,
    ) -> Self {
        Self::index_corruption("index key corrupted")
    }

    /// Construct the canonical index-scan missing projection-component invariant.
    pub(crate) fn index_projection_component_required(
        _index_name: &str,
        _component_index: usize,
    ) -> Self {
        Self::index_invariant(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical scan-time index-entry decode corruption error.
    pub(crate) fn index_entry_decode_failed(_err: impl fmt::Display) -> Self {
        Self::index_corruption("index entry decode failed")
    }

    /// Construct a serialize-origin corruption error.
    pub(crate) fn serialize_corruption(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Serialize,
            COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical persisted-row decode corruption error.
    pub(crate) fn persisted_row_decode_failed(_detail: impl fmt::Display) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the compact persisted-row decode corruption error.
    pub(crate) fn persisted_row_decode_corruption() -> Self {
        Self::serialize_corruption("row decode failed")
    }

    /// Construct the canonical persisted-row field decode corruption error.
    pub(crate) fn persisted_row_field_decode_failed(
        field_name: &str,
        _detail: impl fmt::Display,
    ) -> Self {
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
        _detail: impl fmt::Display,
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

    /// Construct the canonical persisted-row scalar-payload out-of-range corruption error.
    pub(crate) fn persisted_row_field_payload_out_of_range(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row invalid text payload corruption error.
    pub(crate) fn persisted_row_field_text_payload_invalid_utf8(field_name: &str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct the canonical persisted-row structural slot-lookup invariant.
    pub(crate) fn persisted_row_slot_lookup_out_of_bounds(_model_path: &str, _slot: usize) -> Self {
        Self::index_invariant(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical persisted-row structural slot-cache invariant.
    pub(crate) fn persisted_row_slot_cache_lookup_out_of_bounds(
        _model_path: &str,
        _slot: usize,
    ) -> Self {
        Self::index_invariant(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical persisted-row primary-key decode corruption error.
    pub(crate) fn persisted_row_primary_key_not_primary_key_encodable(
        _data_key: impl fmt::Debug,
        _detail: impl fmt::Display,
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

    /// Construct the canonical data-key entity mismatch corruption error.
    pub(crate) fn data_key_entity_mismatch() -> Self {
        Self::store_corruption()
    }

    /// Construct the canonical reverse-index ordinal overflow internal error.
    pub(crate) fn reverse_index_ordinal_overflow(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::index_internal(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical reverse-index entry corruption error.
    pub(crate) fn reverse_index_entry_corrupted(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _index_key: impl fmt::Debug,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::index_corruption(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical relation-target store missing internal error.
    pub(crate) fn relation_target_store_missing(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _store_path: &str,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::executor_internal(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical relation-target key decode corruption error.
    pub(crate) fn relation_target_key_decode_failed(
        _context_label: &str,
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _detail: impl fmt::Display,
    ) -> Self {
        Self::identity_corruption(COMPACT_IDENTITY_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical relation-target entity mismatch corruption error.
    pub(crate) fn relation_target_entity_mismatch(
        _context_label: &str,
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _target_entity_name: &str,
        _expected_tag: impl fmt::Display,
        _actual_tag: impl fmt::Display,
    ) -> Self {
        Self::store_corruption()
    }

    /// Construct the canonical relation-source row decode corruption error.
    pub(crate) fn relation_source_row_decode_failed(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _detail: impl fmt::Display,
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

    /// Construct the canonical unsupported strong-relation key-kind corruption error.
    pub(crate) fn relation_source_row_unsupported_key_kind(_field_kind: impl fmt::Debug) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the canonical reverse-index relation-target decode invariant failure.
    pub(crate) fn reverse_index_relation_target_decode_invariant_violated(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
    ) -> Self {
        Self::executor_internal(COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE)
    }

    /// Construct the canonical covering-component empty-payload corruption error.
    pub(crate) fn bytes_covering_component_payload_empty() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component truncated bool corruption error.
    pub(crate) fn bytes_covering_bool_payload_truncated() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component invalid-length corruption error.
    pub(crate) fn bytes_covering_component_payload_invalid_length(_payload_kind: &str) -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component invalid-bool corruption error.
    pub(crate) fn bytes_covering_bool_payload_invalid_value() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component invalid text terminator corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_terminator() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component trailing-text corruption error.
    pub(crate) fn bytes_covering_text_payload_trailing_bytes() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component invalid-UTF-8 text corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_utf8() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component invalid text escape corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_escape_byte() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical covering-component missing text terminator corruption error.
    pub(crate) fn bytes_covering_text_payload_missing_terminator() -> Self {
        Self::index_corruption("covering component corrupted")
    }

    /// Construct the canonical missing persisted-field decode error.
    #[must_use]
    pub fn missing_persisted_slot(field_name: &'static str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct an identity-origin corruption error.
    pub(crate) fn identity_corruption(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Identity,
            COMPACT_IDENTITY_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a store-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_unsupported() -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Store,
            COMPACT_STORE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical schema DDL publication race error.
    pub(crate) fn schema_ddl_publication_race_lost(entity_path: &'static str) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            message: COMPACT_SCHEMA_DDL_STORE_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Store(
                StoreError::SchemaDdlPublicationRaceLost {
                    entity_path: entity_path.to_string(),
                },
            )),
        }
    }

    /// Construct the canonical SQL DDL SET NOT NULL validation failure.
    #[cfg(feature = "sql")]
    pub(crate) fn schema_ddl_set_not_null_validation_failed(
        entity_path: &'static str,
        column_name: &str,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            message: COMPACT_SCHEMA_DDL_STORE_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Store(
                StoreError::SchemaDdlSetNotNullValidationFailed {
                    entity_path: entity_path.to_string(),
                    column_name: column_name.to_string(),
                },
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
    #[cfg_attr(test, expect(dead_code))]
    pub(crate) fn commit_memory_id_registration_failed(_err: impl fmt::Display) -> Self {
        Self::store_internal(COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an index-origin unsupported error.
    pub(crate) fn index_unsupported(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Index,
            COMPACT_INDEX_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct the canonical index-key component size-limit unsupported error.
    pub(crate) fn index_component_exceeds_max_size(
        _key_item: impl fmt::Display,
        _len: usize,
        _max_component_size: usize,
    ) -> Self {
        Self::index_unsupported(COMPACT_INDEX_DIAGNOSTIC_MESSAGE)
    }

    /// Construct a serialize-origin unsupported error.
    pub(crate) fn serialize_unsupported(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Serialize,
            COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a cursor-origin unsupported error.
    pub(crate) fn cursor_unsupported(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Cursor,
            COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a serialize-origin incompatible persisted-format error.
    pub(crate) fn serialize_incompatible_persisted_format(_message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::IncompatiblePersistedFormat,
            ErrorOrigin::Serialize,
            COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE,
        )
    }

    /// Construct a query-origin unsupported error preserving one SQL parser
    /// unsupported-feature code in structured error detail.
    #[cfg(feature = "sql")]
    pub(crate) fn query_unsupported_sql_feature(feature: diagnostic_code::SqlFeatureCode) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
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
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SqlLowering { reason })),
        }
    }

    /// Construct a query-origin unsupported projection error preserving one
    /// compact projection reason in structured error detail.
    pub(crate) fn query_unsupported_projection(
        reason: diagnostic_code::QueryProjectionCode,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::UnsupportedProjection { reason },
            )),
        }
    }

    /// Construct a query-origin unsupported aggregate target-field error.
    pub(crate) fn query_unknown_aggregate_target_field() -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::UnknownAggregateTargetField,
            )),
        }
    }

    /// Construct a query-origin result-shape mismatch error preserving one
    /// compact result-shape reason in structured error detail.
    pub(crate) fn query_result_shape_mismatch(
        reason: diagnostic_code::QueryResultShapeCode,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Query(QueryErrorDetail::ResultShapeMismatch {
                reason,
            })),
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
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SqlSurfaceMismatch {
                mismatch,
            })),
        }
    }

    /// Construct a query-origin unsupported SQL write boundary error.
    #[cfg(feature = "sql")]
    pub(crate) fn query_sql_write_boundary(
        boundary: diagnostic_code::SqlWriteBoundaryCode,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message: COMPACT_QUERY_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SqlWriteBoundary {
                boundary,
            })),
        }
    }

    pub fn store_not_found(key: impl Into<String>) -> Self {
        let key = key.into();

        Self {
            class: ErrorClass::NotFound,
            origin: ErrorOrigin::Store,
            message: COMPACT_STORE_DIAGNOSTIC_MESSAGE.to_string(),
            detail: Some(ErrorDetail::Store(StoreError::NotFound { key })),
        }
    }

    /// Construct a standardized unsupported-entity-path error.
    pub fn unsupported_entity_path(_path: impl Into<String>) -> Self {
        Self::store_unsupported()
    }

    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(
            self.detail,
            Some(ErrorDetail::Store(StoreError::NotFound { .. }))
        )
    }

    #[must_use]
    pub fn display_with_class(&self) -> String {
        format!("{}:{}: {}", self.origin, self.class, self.message)
    }

    /// Construct an index-plan corruption error with a canonical prefix.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_corruption(origin: ErrorOrigin, _message: impl Into<String>) -> Self {
        let message = match origin {
            ErrorOrigin::Index => COMPACT_INDEX_DIAGNOSTIC_MESSAGE,
            ErrorOrigin::Store => COMPACT_STORE_DIAGNOSTIC_MESSAGE,
            ErrorOrigin::Serialize => COMPACT_SERIALIZE_DIAGNOSTIC_MESSAGE,
            _ => COMPACT_RUNTIME_DIAGNOSTIC_MESSAGE,
        };
        Self::new(ErrorClass::Corruption, origin, message)
    }

    /// Construct an index-plan corruption error for index-origin failures.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_index_corruption(message: impl Into<String>) -> Self {
        Self::index_plan_corruption(ErrorOrigin::Index, message)
    }

    /// Construct an index-plan corruption error for store-origin failures.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_store_corruption() -> Self {
        Self::index_plan_corruption(ErrorOrigin::Store, COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an index-plan corruption error for serialize-origin failures.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_serialize_corruption(message: impl Into<String>) -> Self {
        Self::index_plan_corruption(ErrorOrigin::Serialize, message)
    }

    /// Construct an index-plan invariant violation error with a canonical prefix.
    #[cfg(test)]
    pub(crate) fn index_plan_invariant(origin: ErrorOrigin, _message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            origin,
            compact_message_for(ErrorClass::InvariantViolation, origin),
        )
    }

    /// Construct an index-plan invariant violation error for store-origin failures.
    #[cfg(test)]
    pub(crate) fn index_plan_store_invariant() -> Self {
        Self::index_plan_invariant(ErrorOrigin::Store, COMPACT_STORE_DIAGNOSTIC_MESSAGE)
    }

    /// Construct an index uniqueness violation conflict error.
    pub(crate) fn index_violation(_path: &str, _index_fields: &[&str]) -> Self {
        Self::new(
            ErrorClass::Conflict,
            ErrorOrigin::Index,
            COMPACT_INDEX_DIAGNOSTIC_MESSAGE,
        )
    }
}

///
/// ErrorDetail
///
/// Structured, origin-specific error detail carried by [`InternalError`].
/// This enum is intentionally extensible.
///

#[derive(ThisError)]
pub enum ErrorDetail {
    #[error("{0}")]
    Store(StoreError),
    #[error("{0}")]
    Query(QueryErrorDetail),
    // Future-proofing:
    // #[error("{0}")]
    // Index(IndexError),
    //
    // #[error("{0}")]
    // Executor(ExecutorErrorDetail),
}

///
/// StoreError
///
/// Store-specific structured error detail.
/// Never returned directly; always wrapped in [`ErrorDetail::Store`].
///

#[derive(ThisError)]
pub enum StoreError {
    #[error("key not found: {key}")]
    NotFound { key: String },

    #[error("store corruption: {message}")]
    Corrupt { message: String },

    #[error("store invariant violation: {message}")]
    InvariantViolation { message: String },

    #[error("schema DDL diagnostic")]
    SchemaDdlPublicationRaceLost { entity_path: String },

    #[error("schema DDL diagnostic")]
    SchemaDdlSetNotNullValidationFailed {
        entity_path: String,
        column_name: String,
    },
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

    ResultShapeMismatch {
        reason: diagnostic_code::QueryResultShapeCode,
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
}

impl fmt::Display for QueryErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
    }
}

impl std::error::Error for QueryErrorDetail {}

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

    GeneratedIndexDropRejected,

    RequiredDropDefaultUnsupported,

    GeneratedFieldDefaultChangeRejected,

    GeneratedFieldNullabilityChangeRejected,

    SetNotNullValidationFailed,
}

impl fmt::Display for SchemaDdlAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("schema DDL admission")
    }
}

impl std::error::Error for SchemaDdlAdmissionError {}

impl fmt::Debug for ErrorDetail {
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
            Self::Store(error) => error.diagnostic_code(),
            Self::Query(error) => error.diagnostic_code(),
        }
    }

    /// Return compact structured diagnostic detail when the payload carries one.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        match self {
            Self::Store(error) => error.diagnostic_detail(),
            Self::Query(error) => error.diagnostic_detail(),
        }
    }
}

impl StoreError {
    /// Return the compact diagnostic code for this store detail.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::NotFound { .. } => diagnostic_code::DiagnosticCode::StoreNotFound,
            Self::Corrupt { .. } => diagnostic_code::DiagnosticCode::StoreCorruption,
            Self::InvariantViolation { .. } => {
                diagnostic_code::DiagnosticCode::StoreInvariantViolation
            }
            Self::SchemaDdlPublicationRaceLost { .. }
            | Self::SchemaDdlSetNotNullValidationFailed { .. } => {
                diagnostic_code::DiagnosticCode::SchemaDdlAdmission
            }
        }
    }

    /// Return compact structured diagnostic detail when the store error has one.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        match self {
            Self::SchemaDdlPublicationRaceLost { .. } => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
                })
            }
            Self::SchemaDdlSetNotNullValidationFailed { .. } => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: diagnostic_code::SchemaDdlAdmissionCode::SetNotNullValidationFailed,
                })
            }
            Self::NotFound { .. } | Self::Corrupt { .. } | Self::InvariantViolation { .. } => None,
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
            Self::ResultShapeMismatch { .. } => {
                diagnostic_code::DiagnosticCode::QueryResultShapeMismatch
            }
            Self::SqlSurfaceMismatch { .. } => {
                diagnostic_code::DiagnosticCode::QuerySqlSurfaceMismatch
            }
            Self::SqlWriteBoundary { .. } => diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
            Self::SchemaDdlAdmission { .. } => diagnostic_code::DiagnosticCode::SchemaDdlAdmission,
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
            Self::ResultShapeMismatch { reason } => {
                Some(diagnostic_code::DiagnosticDetail::QueryResultShape { reason: *reason })
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
            | Self::UnknownAggregateTargetField => None,
        }
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
            Self::RequiredDropDefaultUnsupported => {
                diagnostic_code::SchemaDdlAdmissionCode::RequiredDropDefaultUnsupported
            }
            Self::GeneratedFieldDefaultChangeRejected => {
                diagnostic_code::SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected
            }
            Self::GeneratedFieldNullabilityChangeRejected => {
                diagnostic_code::SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected
            }
            Self::SetNotNullValidationFailed => {
                diagnostic_code::SchemaDdlAdmissionCode::SetNotNullValidationFailed
            }
        }
    }
}

///
/// ErrorClass
/// Internal error taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[repr(u16)]
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
            Self::Unsupported => diagnostic_code::DiagnosticCode::RuntimeUnsupported,
            Self::InvariantViolation if matches!(origin, ErrorOrigin::Store) => {
                diagnostic_code::DiagnosticCode::StoreInvariantViolation
            }
            Self::InvariantViolation => diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
        }
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Corruption => "corruption",
            Self::IncompatiblePersistedFormat => "incompatible_persisted_format",
            Self::NotFound => "not_found",
            Self::Internal => "internal",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::InvariantViolation => "invariant_violation",
        };
        write!(f, "{label}")
    }
}

impl fmt::Debug for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u16)
    }
}

///
/// ErrorOrigin
/// Internal origin taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[repr(u16)]
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

impl fmt::Display for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Serialize => "serialize",
            Self::Store => "store",
            Self::Index => "index",
            Self::Identity => "identity",
            Self::Query => "query",
            Self::Planner => "planner",
            Self::Cursor => "cursor",
            Self::Recovery => "recovery",
            Self::Response => "response",
            Self::Executor => "executor",
            Self::Interface => "interface",
        };
        write!(f, "{label}")
    }
}

impl fmt::Debug for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u16)
    }
}
