//! Module: error
//!
//! Responsibility: module-local ownership and contracts for error.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(test)]
mod tests;

use crate::serialize::{SerializeError, SerializeErrorKind};
use std::fmt;
use thiserror::Error as ThisError;

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
    pub fn new(class: ErrorClass, origin: ErrorOrigin, message: impl Into<String>) -> Self {
        let message = message.into();

        let detail = match (class, origin) {
            (ErrorClass::Corruption, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::Corrupt {
                    message: message.clone(),
                }))
            }
            (ErrorClass::InvariantViolation, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::InvariantViolation {
                    message: message.clone(),
                }))
            }
            _ => None,
        };

        Self {
            class,
            origin,
            message,
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

    /// Rebuild this error with a new message while preserving class/origin taxonomy.
    #[cold]
    #[inline(never)]
    pub(crate) fn with_message(self, message: impl Into<String>) -> Self {
        Self::classified(self.class, self.origin, message)
    }

    /// Rebuild this error with a new origin while preserving class/message.
    ///
    /// Origin-scoped detail payloads are intentionally dropped when re-origining.
    #[cold]
    #[inline(never)]
    pub(crate) fn with_origin(self, origin: ErrorOrigin) -> Self {
        Self::classified(self.class, origin, self.message)
    }

    /// Construct an index-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            message.into(),
        )
    }

    /// Construct the canonical index field-count invariant for key building.
    pub(crate) fn index_key_field_count_exceeds_max(
        index_name: &str,
        field_count: usize,
        max_fields: usize,
    ) -> Self {
        Self::index_invariant(format!(
            "index '{index_name}' has {field_count} fields (max {max_fields})",
        ))
    }

    /// Construct the canonical index-key source-field-missing-on-model invariant.
    pub(crate) fn index_key_item_field_missing_on_entity_model(field: &str) -> Self {
        Self::index_invariant(format!(
            "index key item field missing on entity model: {field}",
        ))
    }

    /// Construct the canonical index-key source-field-missing-on-row invariant.
    pub(crate) fn index_key_item_field_missing_on_lookup_row(field: &str) -> Self {
        Self::index_invariant(format!(
            "index key item field missing on lookup row: {field}",
        ))
    }

    /// Construct the canonical index-expression source-type mismatch invariant.
    pub(crate) fn index_expression_source_type_mismatch(
        index_name: &str,
        expression: impl fmt::Display,
        expected: &str,
        source_label: &str,
    ) -> Self {
        Self::index_invariant(format!(
            "index '{index_name}' expression '{expression}' expected {expected} source value, got {source_label}",
        ))
    }

    /// Construct a planner-origin invariant violation with the canonical
    /// executor-boundary invariant prefix preserved in the message payload.
    #[cold]
    #[inline(never)]
    pub(crate) fn planner_executor_invariant(reason: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Planner,
            Self::executor_invariant_message(reason),
        )
    }

    /// Construct a query-origin invariant violation with the canonical
    /// executor-boundary invariant prefix preserved in the message payload.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_executor_invariant(reason: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            Self::executor_invariant_message(reason),
        )
    }

    /// Construct a cursor-origin invariant violation with the canonical
    /// executor-boundary invariant prefix preserved in the message payload.
    #[cold]
    #[inline(never)]
    pub(crate) fn cursor_executor_invariant(reason: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Cursor,
            Self::executor_invariant_message(reason),
        )
    }

    /// Construct an executor-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Executor,
            message.into(),
        )
    }

    /// Construct an executor-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Executor, message.into())
    }

    /// Construct an executor-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_unsupported(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Executor,
            message.into(),
        )
    }

    /// Construct an executor-origin save-preflight schema invariant.
    pub(crate) fn mutation_entity_schema_invalid(
        entity_path: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::executor_invariant(format!("entity schema invalid for {entity_path}: {detail}"))
    }

    /// Construct an executor-origin save-preflight primary-key missing invariant.
    pub(crate) fn mutation_entity_primary_key_missing(entity_path: &str, field_name: &str) -> Self {
        Self::executor_invariant(format!(
            "entity primary key field missing: {entity_path} field={field_name}",
        ))
    }

    /// Construct an executor-origin save-preflight primary-key invalid-value invariant.
    pub(crate) fn mutation_entity_primary_key_invalid_value(
        entity_path: &str,
        field_name: &str,
        value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(format!(
            "entity primary key field has invalid value: {entity_path} field={field_name} value={value:?}",
        ))
    }

    /// Construct an executor-origin save-preflight primary-key type mismatch invariant.
    pub(crate) fn mutation_entity_primary_key_type_mismatch(
        entity_path: &str,
        field_name: &str,
        value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(format!(
            "entity primary key field type mismatch: {entity_path} field={field_name} value={value:?}",
        ))
    }

    /// Construct an executor-origin save-preflight primary-key identity mismatch invariant.
    pub(crate) fn mutation_entity_primary_key_mismatch(
        entity_path: &str,
        field_name: &str,
        field_value: &crate::value::Value,
        identity_key: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(format!(
            "entity primary key mismatch: {entity_path} field={field_name} field_value={field_value:?} id_key={identity_key:?}",
        ))
    }

    /// Construct an executor-origin save-preflight field-missing invariant.
    pub(crate) fn mutation_entity_field_missing(
        entity_path: &str,
        field_name: &str,
        indexed: bool,
    ) -> Self {
        let indexed_note = if indexed { " (indexed)" } else { "" };

        Self::executor_invariant(format!(
            "entity field missing: {entity_path} field={field_name}{indexed_note}",
        ))
    }

    /// Construct an executor-origin save-preflight field-type mismatch invariant.
    pub(crate) fn mutation_entity_field_type_mismatch(
        entity_path: &str,
        field_name: &str,
        value: &crate::value::Value,
    ) -> Self {
        Self::executor_invariant(format!(
            "entity field type mismatch: {entity_path} field={field_name} value={value:?}",
        ))
    }

    /// Construct an executor-origin mutation result invariant.
    ///
    /// This constructor lands ahead of the public structural mutation surface,
    /// so the library target may not route through it until that caller exists.
    #[allow(dead_code)]
    pub(crate) fn mutation_structural_after_image_invalid(
        entity_path: &str,
        data_key: impl fmt::Display,
        detail: impl AsRef<str>,
    ) -> Self {
        Self::executor_invariant(format!(
            "mutation result is invalid: {entity_path} key={data_key} ({})",
            detail.as_ref(),
        ))
    }

    /// Construct an executor-origin mutation unknown-field invariant.
    pub(crate) fn mutation_structural_field_unknown(entity_path: &str, field_name: &str) -> Self {
        Self::executor_invariant(format!(
            "mutation field not found: {entity_path} field={field_name}",
        ))
    }

    /// Construct an executor-origin save-preflight decimal-scale unsupported error.
    pub(crate) fn mutation_decimal_scale_mismatch(
        entity_path: &str,
        field_name: &str,
        expected_scale: impl fmt::Display,
        actual_scale: impl fmt::Display,
    ) -> Self {
        Self::executor_unsupported(format!(
            "decimal field scale mismatch: {entity_path} field={field_name} expected_scale={expected_scale} actual_scale={actual_scale}",
        ))
    }

    /// Construct an executor-origin save-preflight set-encoding invariant.
    pub(crate) fn mutation_set_field_list_required(entity_path: &str, field_name: &str) -> Self {
        Self::executor_invariant(format!(
            "set field must encode as Value::List: {entity_path} field={field_name}",
        ))
    }

    /// Construct an executor-origin save-preflight set-canonicality invariant.
    pub(crate) fn mutation_set_field_not_canonical(entity_path: &str, field_name: &str) -> Self {
        Self::executor_invariant(format!(
            "set field must be strictly ordered and deduplicated: {entity_path} field={field_name}",
        ))
    }

    /// Construct an executor-origin save-preflight map-encoding invariant.
    pub(crate) fn mutation_map_field_map_required(entity_path: &str, field_name: &str) -> Self {
        Self::executor_invariant(format!(
            "map field must encode as Value::Map: {entity_path} field={field_name}",
        ))
    }

    /// Construct an executor-origin save-preflight map-entry invariant.
    pub(crate) fn mutation_map_field_entries_invalid(
        entity_path: &str,
        field_name: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::executor_invariant(format!(
            "map field entries violate map invariants: {entity_path} field={field_name} ({detail})",
        ))
    }

    /// Construct an executor-origin save-preflight map-canonicality invariant.
    pub(crate) fn mutation_map_field_entries_not_canonical(
        entity_path: &str,
        field_name: &str,
    ) -> Self {
        Self::executor_invariant(format!(
            "map field entries are not in canonical deterministic order: {entity_path} field={field_name}",
        ))
    }

    /// Construct a query-origin scalar page invariant for missing predicate slots.
    pub(crate) fn scalar_page_predicate_slots_required() -> Self {
        Self::query_executor_invariant("post-access filtering requires precompiled predicate slots")
    }

    /// Construct a query-origin scalar page invariant for ordering before filtering.
    pub(crate) fn scalar_page_ordering_after_filtering_required() -> Self {
        Self::query_executor_invariant("ordering must run after filtering")
    }

    /// Construct a query-origin scalar page invariant for missing order at the cursor boundary.
    pub(crate) fn scalar_page_cursor_boundary_order_required() -> Self {
        Self::query_executor_invariant("cursor boundary requires ordering")
    }

    /// Construct a query-origin scalar page invariant for cursor-before-ordering drift.
    pub(crate) fn scalar_page_cursor_boundary_after_ordering_required() -> Self {
        Self::query_executor_invariant("cursor boundary must run after ordering")
    }

    /// Construct a query-origin scalar page invariant for pagination-before-ordering drift.
    pub(crate) fn scalar_page_pagination_after_ordering_required() -> Self {
        Self::query_executor_invariant("pagination must run after ordering")
    }

    /// Construct a query-origin scalar page invariant for delete-limit-before-ordering drift.
    pub(crate) fn scalar_page_delete_limit_after_ordering_required() -> Self {
        Self::query_executor_invariant("delete limit must run after ordering")
    }

    /// Construct a query-origin load-runtime invariant for scalar-mode payload mismatch.
    pub(crate) fn load_runtime_scalar_payload_required() -> Self {
        Self::query_executor_invariant("scalar load mode must carry scalar runtime payload")
    }

    /// Construct a query-origin load-runtime invariant for grouped-mode payload mismatch.
    pub(crate) fn load_runtime_grouped_payload_required() -> Self {
        Self::query_executor_invariant("grouped load mode must carry grouped runtime payload")
    }

    /// Construct a query-origin load-surface invariant for scalar-page payload mismatch.
    pub(crate) fn load_runtime_scalar_surface_payload_required() -> Self {
        Self::query_executor_invariant("scalar page load mode must carry scalar runtime payload")
    }

    /// Construct a query-origin load-surface invariant for grouped-page payload mismatch.
    pub(crate) fn load_runtime_grouped_surface_payload_required() -> Self {
        Self::query_executor_invariant("grouped page load mode must carry grouped runtime payload")
    }

    /// Construct a query-origin load-entrypoint invariant for non-load plans.
    pub(crate) fn load_executor_load_plan_required() -> Self {
        Self::query_executor_invariant("load executor requires load plans")
    }

    /// Construct an executor-origin delete-entrypoint unsupported grouped-mode error.
    pub(crate) fn delete_executor_grouped_unsupported() -> Self {
        Self::executor_unsupported("grouped query execution is not yet enabled in this release")
    }

    /// Construct a query-origin delete-entrypoint invariant for non-delete plans.
    pub(crate) fn delete_executor_delete_plan_required() -> Self {
        Self::query_executor_invariant("delete executor requires delete plans")
    }

    /// Construct a query-origin aggregate kernel invariant for fold-mode contract drift.
    pub(crate) fn aggregate_fold_mode_terminal_contract_required() -> Self {
        Self::query_executor_invariant(
            "aggregate fold mode must match route fold-mode contract for aggregate terminal",
        )
    }

    /// Construct a query-origin fast-stream invariant for missing exact key-count observability.
    pub(crate) fn fast_stream_exact_key_count_required() -> Self {
        Self::query_executor_invariant("fast-path stream must expose an exact key-count hint")
    }

    /// Construct a query-origin fast-stream invariant for route kind/request mismatch.
    pub(crate) fn fast_stream_route_kind_request_match_required() -> Self {
        Self::query_executor_invariant("fast-stream route kind/request mismatch")
    }

    /// Construct a query-origin scan invariant for missing index-prefix executable specs.
    pub(crate) fn secondary_index_prefix_spec_required() -> Self {
        Self::query_executor_invariant(
            "index-prefix executable spec must be materialized for index-prefix plans",
        )
    }

    /// Construct a query-origin scan invariant for missing index-range executable specs.
    pub(crate) fn index_range_limit_spec_required() -> Self {
        Self::query_executor_invariant(
            "index-range executable spec must be materialized for index-range plans",
        )
    }

    /// Construct an executor-origin mutation unsupported error for duplicate atomic save keys.
    pub(crate) fn mutation_atomic_save_duplicate_key(
        entity_path: &str,
        key: impl fmt::Display,
    ) -> Self {
        Self::executor_unsupported(format!(
            "atomic save batch rejected duplicate key: entity={entity_path} key={key}",
        ))
    }

    /// Construct an executor-origin mutation invariant for index-store generation drift.
    pub(crate) fn mutation_index_store_generation_changed(
        expected_generation: u64,
        observed_generation: u64,
    ) -> Self {
        Self::executor_invariant(format!(
            "index store generation changed between preflight and apply: expected {expected_generation}, found {observed_generation}",
        ))
    }

    /// Build the canonical executor-invariant message prefix.
    #[must_use]
    #[cold]
    #[inline(never)]
    pub(crate) fn executor_invariant_message(reason: impl Into<String>) -> String {
        format!("executor invariant violated: {}", reason.into())
    }

    /// Construct a planner-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn planner_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Planner,
            message.into(),
        )
    }

    /// Build the canonical invalid-logical-plan message prefix.
    #[must_use]
    pub(crate) fn invalid_logical_plan_message(reason: impl Into<String>) -> String {
        format!("invalid logical plan: {}", reason.into())
    }

    /// Construct a planner-origin invariant with the canonical invalid-plan prefix.
    pub(crate) fn query_invalid_logical_plan(reason: impl Into<String>) -> Self {
        Self::planner_invariant(Self::invalid_logical_plan_message(reason))
    }

    /// Construct a query-origin invariant violation.
    #[cold]
    #[inline(never)]
    pub(crate) fn query_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            message.into(),
        )
    }

    /// Construct a store-origin invariant violation.
    pub(crate) fn store_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Store,
            message.into(),
        )
    }

    /// Construct the canonical duplicate runtime-hook entity-tag invariant.
    pub(crate) fn duplicate_runtime_hooks_for_entity_tag(
        entity_tag: crate::types::EntityTag,
    ) -> Self {
        Self::store_invariant(format!(
            "duplicate runtime hooks for entity tag '{}'",
            entity_tag.value()
        ))
    }

    /// Construct the canonical duplicate runtime-hook entity-path invariant.
    pub(crate) fn duplicate_runtime_hooks_for_entity_path(entity_path: &str) -> Self {
        Self::store_invariant(format!(
            "duplicate runtime hooks for entity path '{entity_path}'"
        ))
    }

    /// Construct a store-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Store, message.into())
    }

    /// Construct the canonical unconfigured commit-memory id internal error.
    pub(crate) fn commit_memory_id_unconfigured() -> Self {
        Self::store_internal(
            "commit memory id is not configured; initialize recovery before commit store access",
        )
    }

    /// Construct the canonical commit-memory id mismatch internal error.
    pub(crate) fn commit_memory_id_mismatch(cached_id: u8, configured_id: u8) -> Self {
        Self::store_internal(format!(
            "commit memory id mismatch: cached={cached_id}, configured={configured_id}",
        ))
    }

    /// Construct the canonical memory-registry initialization failure for commit memory.
    pub(crate) fn commit_memory_registry_init_failed(err: impl fmt::Display) -> Self {
        Self::store_internal(format!("memory registry init failed: {err}"))
    }

    /// Construct the canonical migration cursor persistence-width internal error.
    pub(crate) fn migration_next_step_index_u64_required(id: &str, version: u64) -> Self {
        Self::store_internal(format!(
            "migration '{id}@{version}' next step index does not fit persisted u64 cursor",
        ))
    }

    /// Construct the canonical recovery-integrity totals corruption error.
    pub(crate) fn recovery_integrity_validation_failed(
        missing_index_entries: u64,
        divergent_index_entries: u64,
        orphan_index_references: u64,
    ) -> Self {
        Self::store_corruption(format!(
            "recovery integrity validation failed: missing_index_entries={missing_index_entries} divergent_index_entries={divergent_index_entries} orphan_index_references={orphan_index_references}",
        ))
    }

    /// Construct an index-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Index, message.into())
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
    pub(crate) fn query_unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Query, message.into())
    }

    /// Construct a serialize-origin internal error.
    #[cold]
    #[inline(never)]
    pub(crate) fn serialize_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Serialize, message.into())
    }

    /// Construct the canonical persisted-row encode internal error.
    pub(crate) fn persisted_row_encode_failed(detail: impl fmt::Display) -> Self {
        Self::serialize_internal(format!("row encode failed: {detail}"))
    }

    /// Construct the canonical persisted-row field encode internal error.
    pub(crate) fn persisted_row_field_encode_failed(
        field_name: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::serialize_internal(format!(
            "row encode failed for field '{field_name}': {detail}",
        ))
    }

    /// Construct the canonical bytes(field) value encode internal error.
    pub(crate) fn bytes_field_value_encode_failed(detail: impl fmt::Display) -> Self {
        Self::serialize_internal(format!("bytes(field) value encode failed: {detail}"))
    }

    /// Construct the canonical migration-state serialization failure.
    pub(crate) fn migration_state_serialize_failed(err: impl fmt::Display) -> Self {
        Self::serialize_internal(format!("failed to serialize migration state: {err}"))
    }

    /// Construct a store-origin corruption error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_corruption(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Store, message.into())
    }

    /// Construct the canonical multiple-commit-memory-ids corruption error.
    pub(crate) fn multiple_commit_memory_ids_registered(ids: impl fmt::Debug) -> Self {
        Self::store_corruption(format!(
            "multiple commit marker memory ids registered: {ids:?}"
        ))
    }

    /// Construct the canonical persisted migration-step index conversion corruption error.
    pub(crate) fn migration_persisted_step_index_invalid_usize(
        id: &str,
        version: u64,
        step_index: u64,
    ) -> Self {
        Self::store_corruption(format!(
            "migration '{id}@{version}' persisted step index does not fit runtime usize: {step_index}",
        ))
    }

    /// Construct the canonical persisted migration-step index bounds corruption error.
    pub(crate) fn migration_persisted_step_index_out_of_bounds(
        id: &str,
        version: u64,
        step_index: usize,
        total_steps: usize,
    ) -> Self {
        Self::store_corruption(format!(
            "migration '{id}@{version}' persisted step index out of bounds: {step_index} > {total_steps}",
        ))
    }

    /// Construct a store-origin commit-marker corruption error.
    pub(crate) fn commit_corruption(detail: impl fmt::Display) -> Self {
        Self::store_corruption(format!("commit marker corrupted: {detail}"))
    }

    /// Construct a store-origin commit-marker component corruption error.
    pub(crate) fn commit_component_corruption(component: &str, detail: impl fmt::Display) -> Self {
        Self::store_corruption(format!("commit marker {component} corrupted: {detail}"))
    }

    /// Construct the canonical commit-marker id generation internal error.
    pub(crate) fn commit_id_generation_failed(detail: impl fmt::Display) -> Self {
        Self::store_internal(format!("commit id generation failed: {detail}"))
    }

    /// Construct the canonical commit-marker payload u32-length-limit error.
    pub(crate) fn commit_marker_payload_exceeds_u32_length_limit(label: &str, len: usize) -> Self {
        Self::store_unsupported(format!("{label} exceeds u32 length limit: {len} bytes"))
    }

    /// Construct the canonical commit-marker component invalid-length corruption error.
    pub(crate) fn commit_component_length_invalid(
        component: &str,
        len: usize,
        expected: impl fmt::Display,
    ) -> Self {
        Self::commit_component_corruption(
            component,
            format!("invalid length {len}, expected {expected}"),
        )
    }

    /// Construct the canonical commit-marker max-size corruption error.
    pub(crate) fn commit_marker_exceeds_max_size(size: usize, max_size: u32) -> Self {
        Self::commit_corruption(format!(
            "commit marker exceeds max size: {size} bytes (limit {max_size})",
        ))
    }

    /// Construct the canonical pre-persist commit-marker max-size unsupported error.
    #[cfg(test)]
    pub(crate) fn commit_marker_exceeds_max_size_before_persist(
        size: usize,
        max_size: u32,
    ) -> Self {
        Self::store_unsupported(format!(
            "commit marker exceeds max size: {size} bytes (limit {max_size})",
        ))
    }

    /// Construct the canonical commit-control slot max-size unsupported error.
    pub(crate) fn commit_control_slot_exceeds_max_size(size: usize, max_size: u32) -> Self {
        Self::store_unsupported(format!(
            "commit control slot exceeds max size: {size} bytes (limit {max_size})",
        ))
    }

    /// Construct the canonical commit-control marker-bytes length-limit error.
    pub(crate) fn commit_control_slot_marker_bytes_exceed_u32_length_limit(size: usize) -> Self {
        Self::store_unsupported(format!(
            "commit marker bytes exceed u32 length limit: {size} bytes",
        ))
    }

    /// Construct the canonical commit-control migration-bytes length-limit error.
    pub(crate) fn commit_control_slot_migration_bytes_exceed_u32_length_limit(size: usize) -> Self {
        Self::store_unsupported(format!(
            "commit migration bytes exceed u32 length limit: {size} bytes",
        ))
    }

    /// Construct the canonical startup index-rebuild invalid-data-key corruption error.
    pub(crate) fn startup_index_rebuild_invalid_data_key(
        store_path: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::store_corruption(format!(
            "startup index rebuild failed: invalid data key in store '{store_path}' ({detail})",
        ))
    }

    /// Construct an index-origin corruption error.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_corruption(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Index, message.into())
    }

    /// Construct the canonical unique-validation corruption wrapper.
    pub(crate) fn index_unique_validation_corruption(
        entity_path: &str,
        fields: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::index_plan_index_corruption(format!(
            "index corrupted: {entity_path} ({fields}) -> {detail}",
        ))
    }

    /// Construct the canonical structural index-entry corruption wrapper.
    pub(crate) fn structural_index_entry_corruption(
        entity_path: &str,
        fields: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::index_plan_index_corruption(format!(
            "index corrupted: {entity_path} ({fields}) -> {detail}",
        ))
    }

    /// Construct the canonical missing new entity-key invariant during unique validation.
    pub(crate) fn index_unique_validation_entity_key_required() -> Self {
        Self::index_invariant("missing entity key during unique validation")
    }

    /// Construct the canonical unique-validation structural row-decode corruption error.
    pub(crate) fn index_unique_validation_row_deserialize_failed(
        data_key: impl fmt::Display,
        source: impl fmt::Display,
    ) -> Self {
        Self::index_plan_serialize_corruption(format!(
            "failed to structurally deserialize row: {data_key} ({source})"
        ))
    }

    /// Construct the canonical unique-validation primary-key slot decode corruption error.
    pub(crate) fn index_unique_validation_primary_key_decode_failed(
        data_key: impl fmt::Display,
        source: impl fmt::Display,
    ) -> Self {
        Self::index_plan_serialize_corruption(format!(
            "failed to decode structural primary-key slot: {data_key} ({source})"
        ))
    }

    /// Construct the canonical unique-validation stored key rebuild corruption error.
    pub(crate) fn index_unique_validation_key_rebuild_failed(
        data_key: impl fmt::Display,
        entity_path: &str,
        source: impl fmt::Display,
    ) -> Self {
        Self::index_plan_serialize_corruption(format!(
            "failed to structurally decode unique key row {data_key} for {entity_path}: {source}",
        ))
    }

    /// Construct the canonical unique-validation missing-row corruption error.
    pub(crate) fn index_unique_validation_row_required(data_key: impl fmt::Display) -> Self {
        Self::index_plan_store_corruption(format!("missing row: {data_key}"))
    }

    /// Construct the canonical structural index-predicate parse invariant.
    pub(crate) fn index_predicate_parse_failed(
        entity_path: &str,
        index_name: &str,
        predicate_sql: &str,
        err: impl fmt::Display,
    ) -> Self {
        Self::index_invariant(format!(
            "index predicate parse failed: {entity_path} ({index_name}) WHERE {predicate_sql} -> {err}",
        ))
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
        context: &'static str,
        err: impl fmt::Display,
    ) -> Self {
        Self::index_corruption(format!("index key corrupted during {context}: {err}"))
    }

    /// Construct the canonical index-scan missing projection-component invariant.
    pub(crate) fn index_projection_component_required(
        index_name: &str,
        component_index: usize,
    ) -> Self {
        Self::index_invariant(format!(
            "index projection referenced missing component: index='{index_name}' component_index={component_index}",
        ))
    }

    /// Construct the canonical unexpected unique index-entry cardinality corruption error.
    pub(crate) fn unique_index_entry_single_key_required() -> Self {
        Self::index_corruption("unique index entry contains an unexpected number of keys")
    }

    /// Construct the canonical scan-time index-entry decode corruption error.
    pub(crate) fn index_entry_decode_failed(err: impl fmt::Display) -> Self {
        Self::index_corruption(err.to_string())
    }

    /// Construct a serialize-origin corruption error.
    pub(crate) fn serialize_corruption(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Serialize,
            message.into(),
        )
    }

    /// Construct the canonical persisted-row decode corruption error.
    pub(crate) fn persisted_row_decode_failed(detail: impl fmt::Display) -> Self {
        Self::serialize_corruption(format!("row decode: {detail}"))
    }

    /// Construct the canonical persisted-row field decode corruption error.
    pub(crate) fn persisted_row_field_decode_failed(
        field_name: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::serialize_corruption(format!(
            "row decode failed for field '{field_name}': {detail}",
        ))
    }

    /// Construct the canonical persisted-row field-kind decode corruption error.
    pub(crate) fn persisted_row_field_kind_decode_failed(
        field_name: &str,
        field_kind: impl fmt::Debug,
        detail: impl fmt::Display,
    ) -> Self {
        Self::persisted_row_field_decode_failed(
            field_name,
            format!("kind={field_kind:?}: {detail}"),
        )
    }

    /// Construct the canonical persisted-row scalar-payload length corruption error.
    pub(crate) fn persisted_row_field_payload_exact_len_required(
        field_name: &str,
        payload_kind: &str,
        expected_len: usize,
    ) -> Self {
        let unit = if expected_len == 1 { "byte" } else { "bytes" };

        Self::persisted_row_field_decode_failed(
            field_name,
            format!("{payload_kind} payload must be exactly {expected_len} {unit}"),
        )
    }

    /// Construct the canonical persisted-row scalar-payload empty-body corruption error.
    pub(crate) fn persisted_row_field_payload_must_be_empty(
        field_name: &str,
        payload_kind: &str,
    ) -> Self {
        Self::persisted_row_field_decode_failed(
            field_name,
            format!("{payload_kind} payload must be empty"),
        )
    }

    /// Construct the canonical persisted-row scalar-payload invalid-byte corruption error.
    pub(crate) fn persisted_row_field_payload_invalid_byte(
        field_name: &str,
        payload_kind: &str,
        value: u8,
    ) -> Self {
        Self::persisted_row_field_decode_failed(
            field_name,
            format!("invalid {payload_kind} payload byte {value}"),
        )
    }

    /// Construct the canonical persisted-row scalar-payload non-finite corruption error.
    pub(crate) fn persisted_row_field_payload_non_finite(
        field_name: &str,
        payload_kind: &str,
    ) -> Self {
        Self::persisted_row_field_decode_failed(
            field_name,
            format!("{payload_kind} payload is non-finite"),
        )
    }

    /// Construct the canonical persisted-row scalar-payload out-of-range corruption error.
    pub(crate) fn persisted_row_field_payload_out_of_range(
        field_name: &str,
        payload_kind: &str,
    ) -> Self {
        Self::persisted_row_field_decode_failed(
            field_name,
            format!("{payload_kind} payload out of range for target type"),
        )
    }

    /// Construct the canonical persisted-row invalid text payload corruption error.
    pub(crate) fn persisted_row_field_text_payload_invalid_utf8(
        field_name: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::persisted_row_field_decode_failed(
            field_name,
            format!("invalid UTF-8 text payload ({detail})"),
        )
    }

    /// Construct the canonical persisted-row structural slot-lookup invariant.
    pub(crate) fn persisted_row_slot_lookup_out_of_bounds(model_path: &str, slot: usize) -> Self {
        Self::index_invariant(format!(
            "slot lookup outside model bounds during structural row access: model='{model_path}' slot={slot}",
        ))
    }

    /// Construct the canonical persisted-row structural slot-cache invariant.
    pub(crate) fn persisted_row_slot_cache_lookup_out_of_bounds(
        model_path: &str,
        slot: usize,
    ) -> Self {
        Self::index_invariant(format!(
            "slot cache lookup outside model bounds during structural row access: model='{model_path}' slot={slot}",
        ))
    }

    /// Construct the canonical persisted-row primary-key-slot-missing invariant.
    pub(crate) fn persisted_row_primary_key_field_missing(model_path: &str) -> Self {
        Self::index_invariant(format!(
            "entity primary key field missing during structural row validation: {model_path}",
        ))
    }

    /// Construct the canonical persisted-row primary-key decode corruption error.
    pub(crate) fn persisted_row_primary_key_not_storage_encodable(
        data_key: impl fmt::Display,
        detail: impl fmt::Display,
    ) -> Self {
        Self::persisted_row_decode_failed(format!(
            "primary-key value is not storage-key encodable: {data_key} ({detail})",
        ))
    }

    /// Construct the canonical persisted-row missing primary-key slot corruption error.
    pub(crate) fn persisted_row_primary_key_slot_missing(data_key: impl fmt::Display) -> Self {
        Self::persisted_row_decode_failed(format!(
            "missing primary-key slot while validating {data_key}",
        ))
    }

    /// Construct the canonical persisted-row key mismatch corruption error.
    pub(crate) fn persisted_row_key_mismatch(
        expected_key: impl fmt::Display,
        found_key: impl fmt::Display,
    ) -> Self {
        Self::store_corruption(format!(
            "row key mismatch: expected {expected_key}, found {found_key}",
        ))
    }

    /// Construct the canonical persisted-row missing declared-field corruption error.
    pub(crate) fn persisted_row_declared_field_missing(field_name: &str) -> Self {
        Self::persisted_row_decode_failed(format!("missing declared field `{field_name}`"))
    }

    /// Construct the canonical data-key entity mismatch corruption error.
    pub(crate) fn data_key_entity_mismatch(
        expected: impl fmt::Display,
        found: impl fmt::Display,
    ) -> Self {
        Self::store_corruption(format!(
            "data key entity mismatch: expected {expected}, found {found}",
        ))
    }

    /// Construct the canonical data-key primary-key decode corruption error.
    pub(crate) fn data_key_primary_key_decode_failed(value: impl fmt::Debug) -> Self {
        Self::store_corruption(format!("data key primary key decode failed: {value:?}",))
    }

    /// Construct the canonical reverse-index ordinal overflow internal error.
    pub(crate) fn reverse_index_ordinal_overflow(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::index_internal(format!(
            "reverse index ordinal overflow: source={source_path} field={field_name} target={target_path} ({detail})",
        ))
    }

    /// Construct the canonical reverse-index entry corruption error.
    pub(crate) fn reverse_index_entry_corrupted(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        index_key: impl fmt::Debug,
        detail: impl fmt::Display,
    ) -> Self {
        Self::index_corruption(format!(
            "reverse index entry corrupted: source={source_path} field={field_name} target={target_path} key={index_key:?} ({detail})",
        ))
    }

    /// Construct the canonical reverse-index entry encode unsupported error.
    pub(crate) fn reverse_index_entry_encode_failed(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::index_unsupported(format!(
            "reverse index entry encoding failed: source={source_path} field={field_name} target={target_path} ({detail})",
        ))
    }

    /// Construct the canonical relation-target store missing internal error.
    pub(crate) fn relation_target_store_missing(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        store_path: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::executor_internal(format!(
            "relation target store missing: source={source_path} field={field_name} target={target_path} store={store_path} ({detail})",
        ))
    }

    /// Construct the canonical relation-target key decode corruption error.
    pub(crate) fn relation_target_key_decode_failed(
        context_label: &str,
        source_path: &str,
        field_name: &str,
        target_path: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::identity_corruption(format!(
            "{context_label}: source={source_path} field={field_name} target={target_path} ({detail})",
        ))
    }

    /// Construct the canonical relation-target entity mismatch corruption error.
    pub(crate) fn relation_target_entity_mismatch(
        context_label: &str,
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        expected_tag: impl fmt::Display,
        actual_tag: impl fmt::Display,
    ) -> Self {
        Self::store_corruption(format!(
            "{context_label}: source={source_path} field={field_name} target={target_path} expected={target_entity_name} (tag={expected_tag}) actual_tag={actual_tag}",
        ))
    }

    /// Construct the canonical relation-source row decode corruption error.
    pub(crate) fn relation_source_row_decode_failed(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        detail: impl fmt::Display,
    ) -> Self {
        Self::serialize_corruption(format!(
            "relation source row decode: source={source_path} field={field_name} target={target_path} ({detail})",
        ))
    }

    /// Construct the canonical relation-source unsupported scalar relation-key corruption error.
    pub(crate) fn relation_source_row_unsupported_scalar_relation_key(
        source_path: &str,
        field_name: &str,
        target_path: &str,
    ) -> Self {
        Self::serialize_corruption(format!(
            "relation source row decode: unsupported scalar relation key: source={source_path} field={field_name} target={target_path}",
        ))
    }

    /// Construct the canonical invalid strong-relation field-kind corruption error.
    pub(crate) fn relation_source_row_invalid_field_kind(field_kind: impl fmt::Debug) -> Self {
        Self::serialize_corruption(format!(
            "invalid strong relation field kind during structural decode: {field_kind:?}"
        ))
    }

    /// Construct the canonical unsupported strong-relation key-kind corruption error.
    pub(crate) fn relation_source_row_unsupported_key_kind(field_kind: impl fmt::Debug) -> Self {
        Self::serialize_corruption(format!(
            "unsupported strong relation key kind during structural decode: {field_kind:?}"
        ))
    }

    /// Construct the canonical reverse-index relation-target decode invariant failure.
    pub(crate) fn reverse_index_relation_target_decode_invariant_violated(
        source_path: &str,
        field_name: &str,
        target_path: &str,
    ) -> Self {
        Self::executor_internal(format!(
            "relation target decode invariant violated while preparing reverse index: source={source_path} field={field_name} target={target_path}",
        ))
    }

    /// Construct the canonical covering-component empty-payload corruption error.
    pub(crate) fn bytes_covering_component_payload_empty() -> Self {
        Self::index_corruption("index component payload is empty during covering projection decode")
    }

    /// Construct the canonical covering-component truncated bool corruption error.
    pub(crate) fn bytes_covering_bool_payload_truncated() -> Self {
        Self::index_corruption("bool covering component payload is truncated")
    }

    /// Construct the canonical covering-component invalid-length corruption error.
    pub(crate) fn bytes_covering_component_payload_invalid_length(payload_kind: &str) -> Self {
        Self::index_corruption(format!(
            "{payload_kind} covering component payload has invalid length"
        ))
    }

    /// Construct the canonical covering-component invalid-bool corruption error.
    pub(crate) fn bytes_covering_bool_payload_invalid_value() -> Self {
        Self::index_corruption("bool covering component payload has invalid value")
    }

    /// Construct the canonical covering-component invalid text terminator corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_terminator() -> Self {
        Self::index_corruption("text covering component payload has invalid terminator")
    }

    /// Construct the canonical covering-component trailing-text corruption error.
    pub(crate) fn bytes_covering_text_payload_trailing_bytes() -> Self {
        Self::index_corruption("text covering component payload contains trailing bytes")
    }

    /// Construct the canonical covering-component invalid-UTF-8 text corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_utf8() -> Self {
        Self::index_corruption("text covering component payload is not valid UTF-8")
    }

    /// Construct the canonical covering-component invalid text escape corruption error.
    pub(crate) fn bytes_covering_text_payload_invalid_escape_byte() -> Self {
        Self::index_corruption("text covering component payload has invalid escape byte")
    }

    /// Construct the canonical covering-component missing text terminator corruption error.
    pub(crate) fn bytes_covering_text_payload_missing_terminator() -> Self {
        Self::index_corruption("text covering component payload is missing terminator")
    }

    /// Construct the canonical missing persisted-field decode error.
    #[must_use]
    pub fn missing_persisted_slot(field_name: &'static str) -> Self {
        Self::serialize_corruption(format!("row decode: missing required field '{field_name}'",))
    }

    /// Construct an identity-origin corruption error.
    pub(crate) fn identity_corruption(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Identity,
            message.into(),
        )
    }

    /// Construct a store-origin unsupported error.
    #[cold]
    #[inline(never)]
    pub(crate) fn store_unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Store, message.into())
    }

    /// Construct the canonical empty migration label unsupported error.
    pub(crate) fn migration_label_empty(label: &str) -> Self {
        Self::store_unsupported(format!("{label} cannot be empty"))
    }

    /// Construct the canonical empty migration-step row-op set unsupported error.
    pub(crate) fn migration_step_row_ops_required(name: &str) -> Self {
        Self::store_unsupported(format!(
            "migration step '{name}' must include at least one row op",
        ))
    }

    /// Construct the canonical invalid migration-plan version unsupported error.
    pub(crate) fn migration_plan_version_required(id: &str) -> Self {
        Self::store_unsupported(format!("migration plan '{id}' version must be > 0",))
    }

    /// Construct the canonical empty migration-plan steps unsupported error.
    pub(crate) fn migration_plan_steps_required(id: &str) -> Self {
        Self::store_unsupported(format!(
            "migration plan '{id}' must include at least one step",
        ))
    }

    /// Construct the canonical migration cursor out-of-bounds unsupported error.
    pub(crate) fn migration_cursor_out_of_bounds(
        id: &str,
        version: u64,
        next_step: usize,
        total_steps: usize,
    ) -> Self {
        Self::store_unsupported(format!(
            "migration '{id}@{version}' cursor out of bounds: next_step={next_step} total_steps={total_steps}",
        ))
    }

    /// Construct the canonical max-steps-required migration execution error.
    pub(crate) fn migration_execution_requires_max_steps(id: &str) -> Self {
        Self::store_unsupported(format!("migration '{id}' execution requires max_steps > 0",))
    }

    /// Construct the canonical in-progress migration-plan conflict error.
    pub(crate) fn migration_in_progress_conflict(
        requested_id: &str,
        requested_version: u64,
        active_id: &str,
        active_version: u64,
    ) -> Self {
        Self::store_unsupported(format!(
            "migration '{requested_id}@{requested_version}' cannot execute while migration '{active_id}@{active_version}' is in progress",
        ))
    }

    /// Construct the canonical unsupported persisted entity-tag store error.
    pub(crate) fn unsupported_entity_tag_in_data_store(
        entity_tag: crate::types::EntityTag,
    ) -> Self {
        Self::store_unsupported(format!(
            "unsupported entity tag in data store: '{}'",
            entity_tag.value()
        ))
    }

    /// Construct the canonical configured-vs-registered commit-memory id mismatch error.
    pub(crate) fn configured_commit_memory_id_mismatch(
        configured_id: u8,
        registered_id: u8,
    ) -> Self {
        Self::store_unsupported(format!(
            "configured commit memory id {configured_id} does not match existing commit marker id {registered_id}",
        ))
    }

    /// Construct the canonical occupied commit-memory id unsupported error.
    pub(crate) fn commit_memory_id_already_registered(memory_id: u8, label: &str) -> Self {
        Self::store_unsupported(format!(
            "configured commit memory id {memory_id} is already registered as '{label}'",
        ))
    }

    /// Construct the canonical out-of-range commit-memory id unsupported error.
    pub(crate) fn commit_memory_id_outside_reserved_ranges(memory_id: u8) -> Self {
        Self::store_unsupported(format!(
            "configured commit memory id {memory_id} is outside reserved ranges",
        ))
    }

    /// Construct the canonical commit-memory id registration failure.
    pub(crate) fn commit_memory_id_registration_failed(err: impl fmt::Display) -> Self {
        Self::store_internal(format!("commit memory id registration failed: {err}"))
    }

    /// Construct an index-origin unsupported error.
    pub(crate) fn index_unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Index, message.into())
    }

    /// Construct the canonical index-key component size-limit unsupported error.
    pub(crate) fn index_component_exceeds_max_size(
        key_item: impl fmt::Display,
        len: usize,
        max_component_size: usize,
    ) -> Self {
        Self::index_unsupported(format!(
            "index component exceeds max size: key item '{key_item}' -> {len} bytes (limit {max_component_size})",
        ))
    }

    /// Construct the canonical index-entry max-keys unsupported error during commit encoding.
    pub(crate) fn index_entry_exceeds_max_keys(
        entity_path: &str,
        fields: &str,
        keys: usize,
    ) -> Self {
        Self::index_unsupported(format!(
            "index entry exceeds max keys: {entity_path} ({fields}) -> {keys} keys",
        ))
    }

    /// Construct the canonical duplicate-key invariant during commit entry encoding.
    #[cfg(test)]
    pub(crate) fn index_entry_duplicate_keys_unexpected(entity_path: &str, fields: &str) -> Self {
        Self::index_invariant(format!(
            "index entry unexpectedly contains duplicate keys: {entity_path} ({fields})",
        ))
    }

    /// Construct the canonical index-entry key-encoding unsupported error during commit encoding.
    pub(crate) fn index_entry_key_encoding_failed(
        entity_path: &str,
        fields: &str,
        err: impl fmt::Display,
    ) -> Self {
        Self::index_unsupported(format!(
            "index entry key encoding failed: {entity_path} ({fields}) -> {err}",
        ))
    }

    /// Construct a serialize-origin unsupported error.
    pub(crate) fn serialize_unsupported(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Serialize,
            message.into(),
        )
    }

    /// Construct a cursor-origin unsupported error.
    pub(crate) fn cursor_unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Cursor, message.into())
    }

    /// Construct a serialize-origin incompatible persisted-format error.
    pub(crate) fn serialize_incompatible_persisted_format(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::IncompatiblePersistedFormat,
            ErrorOrigin::Serialize,
            message.into(),
        )
    }

    /// Construct the canonical persisted-payload decode failure mapping for one
    /// DB-owned serialized payload boundary.
    pub(crate) fn serialize_payload_decode_failed(
        source: SerializeError,
        payload_label: &'static str,
    ) -> Self {
        match source {
            // DB codec only decodes engine-owned persisted payloads.
            // Size-limit breaches indicate persisted bytes violate DB storage policy.
            SerializeError::DeserializeSizeLimitExceeded { len, max_bytes } => {
                Self::serialize_corruption(format!(
                    "{payload_label} decode failed: payload size {len} exceeds limit {max_bytes}"
                ))
            }
            SerializeError::Deserialize(_) => Self::serialize_corruption(format!(
                "{payload_label} decode failed: {}",
                SerializeErrorKind::Deserialize
            )),
            SerializeError::Serialize(_) => Self::serialize_corruption(format!(
                "{payload_label} decode failed: {}",
                SerializeErrorKind::Serialize
            )),
        }
    }

    /// Construct a query-origin unsupported error preserving one SQL parser
    /// unsupported-feature label in structured error detail.
    #[cfg(feature = "sql")]
    pub(crate) fn query_unsupported_sql_feature(feature: &'static str) -> Self {
        let message = format!(
            "SQL query is not executable in this release: unsupported SQL feature: {feature}"
        );

        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message,
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::UnsupportedSqlFeature { feature },
            )),
        }
    }

    pub fn store_not_found(key: impl Into<String>) -> Self {
        let key = key.into();

        Self {
            class: ErrorClass::NotFound,
            origin: ErrorOrigin::Store,
            message: format!("data key not found: {key}"),
            detail: Some(ErrorDetail::Store(StoreError::NotFound { key })),
        }
    }

    /// Construct a standardized unsupported-entity-path error.
    pub fn unsupported_entity_path(path: impl Into<String>) -> Self {
        let path = path.into();

        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Store,
            format!("unsupported entity path: '{path}'"),
        )
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
    pub(crate) fn index_plan_corruption(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        let message = message.into();
        Self::new(
            ErrorClass::Corruption,
            origin,
            format!("corruption detected ({origin}): {message}"),
        )
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
    pub(crate) fn index_plan_store_corruption(message: impl Into<String>) -> Self {
        Self::index_plan_corruption(ErrorOrigin::Store, message)
    }

    /// Construct an index-plan corruption error for serialize-origin failures.
    #[cold]
    #[inline(never)]
    pub(crate) fn index_plan_serialize_corruption(message: impl Into<String>) -> Self {
        Self::index_plan_corruption(ErrorOrigin::Serialize, message)
    }

    /// Construct an index-plan invariant violation error with a canonical prefix.
    #[cfg(test)]
    pub(crate) fn index_plan_invariant(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        let message = message.into();
        Self::new(
            ErrorClass::InvariantViolation,
            origin,
            format!("invariant violation detected ({origin}): {message}"),
        )
    }

    /// Construct an index-plan invariant violation error for store-origin failures.
    #[cfg(test)]
    pub(crate) fn index_plan_store_invariant(message: impl Into<String>) -> Self {
        Self::index_plan_invariant(ErrorOrigin::Store, message)
    }

    /// Construct an index uniqueness violation conflict error.
    pub(crate) fn index_violation(path: &str, index_fields: &[&str]) -> Self {
        Self::new(
            ErrorClass::Conflict,
            ErrorOrigin::Index,
            format!(
                "index constraint violation: {path} ({})",
                index_fields.join(", ")
            ),
        )
    }
}

///
/// ErrorDetail
///
/// Structured, origin-specific error detail carried by [`InternalError`].
/// This enum is intentionally extensible.
///

#[derive(Debug, ThisError)]
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

#[derive(Debug, ThisError)]
pub enum StoreError {
    #[error("key not found: {key}")]
    NotFound { key: String },

    #[error("store corruption: {message}")]
    Corrupt { message: String },

    #[error("store invariant violation: {message}")]
    InvariantViolation { message: String },
}

///
/// QueryErrorDetail
///
/// Query-origin structured error detail payload.
///

#[derive(Debug, ThisError)]
pub enum QueryErrorDetail {
    #[error("unsupported SQL feature: {feature}")]
    UnsupportedSqlFeature { feature: &'static str },
}

///
/// ErrorClass
/// Internal error taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    Corruption,
    IncompatiblePersistedFormat,
    NotFound,
    Internal,
    Conflict,
    Unsupported,
    InvariantViolation,
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

///
/// ErrorOrigin
/// Internal origin taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
