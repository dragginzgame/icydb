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

    /// Rebuild this error with a new origin while preserving class taxonomy.
    ///
    /// Origin-scoped detail payloads are intentionally dropped when re-origining.
    #[cold]
    #[inline(never)]
    pub(crate) fn with_origin(self, origin: ErrorOrigin) -> Self {
        Self::classified(self.class, origin)
    }

    /// Construct an index-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_invariant() -> Self {
        Self::new(ErrorClass::InvariantViolation, ErrorOrigin::Index)
    }

    /// Construct the canonical index field-count invariant for key building.
    pub(crate) fn index_key_field_count_exceeds_max(
        _index_name: &str,
        _field_count: usize,
        _max_fields: usize,
    ) -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical index-key source-field-missing-on-model invariant.
    pub(crate) fn index_key_item_field_missing_on_entity_model(_field: &str) -> Self {
        Self::index_invariant()
    }

    /// Construct the canonical index-key source-field-missing-on-row invariant.
    pub(crate) fn index_key_item_field_missing_on_lookup_row(_field: &str) -> Self {
        Self::index_invariant()
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

    /// Construct an executor-origin conflict.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_conflict() -> Self {
        Self::new(ErrorClass::Conflict, ErrorOrigin::Executor)
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

    /// Construct an executor-origin save-preflight primary-key missing invariant.
    pub(crate) fn mutation_entity_primary_key_missing(
        _entity_path: &str,
        _field_name: &str,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight primary-key invalid-value invariant.
    pub(crate) fn mutation_entity_primary_key_invalid_value(
        _entity_path: &str,
        _field_name: &str,
        _value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight primary-key type mismatch invariant.
    pub(crate) fn mutation_entity_primary_key_type_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight primary-key identity mismatch invariant.
    pub(crate) fn mutation_entity_primary_key_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _field_value: &crate::value::Value,
        _identity_key: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight field-missing invariant.
    pub(crate) fn mutation_entity_field_missing(
        _entity_path: &str,
        _field_name: &str,
        _indexed: bool,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin sparse structural patch required-field invariant.
    pub(crate) fn mutation_structural_patch_required_field_missing(
        _entity_path: &str,
        _field_name: &str,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight field-type mismatch invariant.
    pub(crate) fn mutation_entity_field_type_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin generated-field authored-write rejection.
    pub(crate) fn mutation_generated_field_explicit(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_unsupported()
    }

    /// Construct an executor-origin typed create omission rejection.
    #[must_use]
    pub fn mutation_create_missing_authored_fields(_entity_path: &str, _field_names: &str) -> Self {
        Self::executor_unsupported()
    }

    /// Construct an executor-origin mutation result invariant.
    ///
    /// This constructor lands ahead of the public structural mutation surface,
    /// so the library target may not route through it until that caller exists.
    pub(crate) fn mutation_structural_after_image_invalid(
        _entity_path: &str,
        _data_key: impl Sized,
        _detail: impl Sized,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin mutation unknown-field invariant.
    pub(crate) fn mutation_structural_field_unknown(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight decimal-scale unsupported error.
    pub(crate) fn mutation_decimal_scale_mismatch(
        _entity_path: &str,
        _field_name: &str,
        _expected_scale: impl Sized,
        _actual_scale: impl Sized,
    ) -> Self {
        Self::executor_unsupported()
    }

    /// Construct an executor-origin save-preflight text-length unsupported error.
    pub(crate) fn mutation_text_max_len_exceeded(
        _entity_path: &str,
        _field_name: &str,
        _max_len: impl Sized,
        _actual_len: impl Sized,
    ) -> Self {
        Self::executor_unsupported()
    }

    /// Construct an executor-origin save-preflight set-encoding invariant.
    pub(crate) fn mutation_set_field_list_required(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight set-canonicality invariant.
    pub(crate) fn mutation_set_field_not_canonical(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight map-encoding invariant.
    pub(crate) fn mutation_map_field_map_required(_entity_path: &str, _field_name: &str) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight map-entry invariant.
    pub(crate) fn mutation_map_field_entries_invalid(
        _entity_path: &str,
        _field_name: &str,
        _detail: impl Sized,
    ) -> Self {
        Self::executor_invariant()
    }

    /// Construct an executor-origin save-preflight map-canonicality invariant.
    pub(crate) fn mutation_map_field_entries_not_canonical(
        _entity_path: &str,
        _field_name: &str,
    ) -> Self {
        Self::executor_invariant()
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

    /// Construct a query-origin load-runtime invariant for scalar-mode payload mismatch.
    pub(crate) fn load_runtime_scalar_payload_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin load-runtime invariant for grouped-mode payload mismatch.
    pub(crate) fn load_runtime_grouped_payload_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin load-surface invariant for scalar-page payload mismatch.
    pub(crate) fn load_runtime_scalar_surface_payload_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin load-surface invariant for grouped-page payload mismatch.
    pub(crate) fn load_runtime_grouped_surface_payload_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin load-entrypoint invariant for non-load plans.
    pub(crate) fn load_executor_load_plan_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct an executor-origin delete-entrypoint unsupported grouped-mode error.
    pub(crate) fn delete_executor_grouped_unsupported() -> Self {
        Self::executor_unsupported()
    }

    /// Construct a query-origin delete-entrypoint invariant for non-delete plans.
    pub(crate) fn delete_executor_delete_plan_required() -> Self {
        Self::query_executor_invariant()
    }

    /// Construct a query-origin aggregate kernel invariant for fold-mode contract drift.
    pub(crate) fn aggregate_fold_mode_terminal_contract_required() -> Self {
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
    pub(crate) fn mutation_atomic_save_duplicate_key(_entity_path: &str, _key: impl Sized) -> Self {
        Self::executor_conflict()
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
    pub(crate) fn store_internal() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Store)
    }

    /// Construct the canonical unconfigured commit-memory id internal error.
    pub(crate) fn commit_memory_id_unconfigured() -> Self {
        Self::store_internal()
    }

    /// Construct the canonical commit-memory id mismatch internal error.
    pub(crate) fn commit_memory_id_mismatch(_cached_id: u8, _configured_id: u8) -> Self {
        Self::store_internal()
    }

    /// Construct the canonical commit-memory stable-key mismatch internal error.
    pub(crate) fn commit_memory_stable_key_mismatch(
        _cached_key: &str,
        _configured_key: &str,
    ) -> Self {
        Self::store_internal()
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

    /// Construct a commit control-memory growth failure.
    pub(crate) fn commit_control_memory_growth_failed() -> Self {
        Self::store_internal()
    }

    /// Construct a store-format memory registration failure.
    #[cfg(not(test))]
    pub(crate) fn database_format_memory_registration_failed(_err: impl Sized) -> Self {
        Self::store_internal()
    }

    /// Construct the canonical missing rollback-row invariant for delete execution.
    pub(crate) fn delete_rollback_row_required() -> Self {
        Self::store_internal()
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
        _expected_revision: u64,
        _current_revision: Option<u64>,
    ) -> Self {
        Self {
            class: ErrorClass::Conflict,
            origin: ErrorOrigin::Query,
            detail: Some(ErrorDetail::Query(QueryErrorDetail::StaleSchemaRevision)),
        }
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

    /// Construct the canonical persisted-row field encode internal error.
    pub(crate) fn persisted_row_field_encode_failed(field_name: &str, _detail: impl Sized) -> Self {
        Self::persisted_row_field_encode_internal(field_name)
    }

    /// Construct the compact persisted-row field encode internal error.
    pub(crate) fn persisted_row_field_encode_internal(_field_name: &str) -> Self {
        Self::persisted_row_encode_internal()
    }

    /// Construct the canonical bytes(field) value encode internal error.
    pub(crate) fn bytes_field_value_encode_failed(_detail: impl Sized) -> Self {
        Self::serialize_internal()
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
    pub(crate) fn commit_component_length_invalid() -> Self {
        Self::commit_corruption()
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

    /// Construct the canonical startup index-rebuild invalid-data-key corruption error.
    pub(crate) fn startup_index_rebuild_invalid_data_key() -> Self {
        Self::store_corruption()
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

    /// Construct the canonical persisted-row decode corruption error.
    pub(crate) fn persisted_row_decode_failed(_detail: impl Sized) -> Self {
        Self::persisted_row_decode_corruption()
    }

    /// Construct the compact persisted-row decode corruption error.
    pub(crate) fn persisted_row_decode_corruption() -> Self {
        Self::serialize_corruption()
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

    /// Construct the canonical data-key entity mismatch corruption error.
    pub(crate) fn data_key_entity_mismatch() -> Self {
        Self::store_corruption()
    }

    /// Construct the canonical reverse-index ordinal overflow internal error.
    pub(crate) fn reverse_index_ordinal_overflow(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _detail: impl Sized,
    ) -> Self {
        Self::index_internal()
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
        _expected_tag: impl Sized,
        _actual_tag: impl Sized,
    ) -> Self {
        Self::store_corruption()
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

    /// Construct the canonical reverse-index relation-target decode invariant failure.
    pub(crate) fn reverse_index_relation_target_decode_invariant_violated(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
    ) -> Self {
        Self::executor_internal()
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

    /// Construct the canonical missing persisted-field decode error.
    #[must_use]
    pub fn missing_persisted_slot(field_name: &'static str) -> Self {
        Self::persisted_row_field_decode_corruption(field_name)
    }

    /// Construct an identity-origin corruption error.
    pub(crate) fn identity_corruption() -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Identity)
    }

    /// Construct a store-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_unsupported() -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Store)
    }

    /// Construct the canonical schema DDL publication race error.
    #[cfg(any(test, feature = "sql"))]
    pub(crate) fn schema_ddl_publication_race_lost(_entity_path: &'static str) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(StoreError::SchemaDdlPublicationRaceLost)),
        }
    }

    /// Construct the canonical SQL DDL SET NOT NULL validation failure.
    #[cfg(feature = "sql")]
    pub(crate) fn schema_ddl_set_not_null_validation_failed(
        _entity_path: &'static str,
        _column_name: &str,
    ) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Store,
            detail: Some(ErrorDetail::Store(
                StoreError::SchemaDdlSetNotNullValidationFailed,
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

    /// Construct a query-origin unsupported aggregate target-field error.
    pub(crate) fn query_unknown_aggregate_target_field() -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
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
            detail: Some(ErrorDetail::Query(QueryErrorDetail::SqlWriteBoundary {
                boundary,
            })),
        }
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

    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(self.detail, Some(ErrorDetail::Store(StoreError::NotFound)))
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

    /// Construct an index uniqueness violation conflict error.
    pub(crate) fn index_violation(_path: &str, _index_fields: &[&str]) -> Self {
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
/// ErrorDetail
///
/// Structured, origin-specific error detail carried by [`InternalError`].
/// This enum is intentionally extensible.
///

pub enum ErrorDetail {
    Store(StoreError),
    Query(QueryErrorDetail),
    Recovery(RecoveryErrorDetail),
    // Future-proofing:
    // Index(IndexError),
    //
    // Executor(ExecutorErrorDetail),
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

    SchemaDdlSetNotNullValidationFailed,
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
        f.write_str(COMPACT_QUERY_DIAGNOSTIC_MESSAGE)
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
            Self::Store(error) => error.diagnostic_code(),
            Self::Query(error) => error.diagnostic_code(),
            Self::Recovery(error) => error.diagnostic_code(),
        }
    }

    /// Return compact structured diagnostic detail when the payload carries one.
    #[must_use]
    pub const fn diagnostic_detail(&self) -> Option<diagnostic_code::DiagnosticDetail> {
        match self {
            Self::Store(error) => error.diagnostic_detail(),
            Self::Query(error) => error.diagnostic_detail(),
            Self::Recovery(error) => error.diagnostic_detail(),
        }
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
}

impl StoreError {
    /// Return the compact diagnostic code for this store detail.
    #[must_use]
    pub const fn diagnostic_code(&self) -> diagnostic_code::DiagnosticCode {
        match self {
            Self::NotFound => diagnostic_code::DiagnosticCode::StoreNotFound,
            Self::Corrupt => diagnostic_code::DiagnosticCode::StoreCorruption,
            Self::InvariantViolation => diagnostic_code::DiagnosticCode::StoreInvariantViolation,
            Self::SchemaDdlPublicationRaceLost | Self::SchemaDdlSetNotNullValidationFailed => {
                diagnostic_code::DiagnosticCode::SchemaDdlAdmission
            }
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
            Self::SchemaDdlSetNotNullValidationFailed => {
                Some(diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                    reason: diagnostic_code::SchemaDdlAdmissionCode::SetNotNullValidationFailed,
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
            Self::ResultShapeMismatch { .. } => {
                diagnostic_code::DiagnosticCode::QueryResultShapeMismatch
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
            Self::ResultShapeMismatch { reason } => {
                Some(diagnostic_code::DiagnosticDetail::QueryResultShape { reason: *reason })
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

impl fmt::Debug for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u16)
    }
}
