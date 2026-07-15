//! Module: metrics::state::ops
//! Responsibility: global operation metrics DTO and derived-ratio helpers.
//! Does not own: mutable metrics state storage or report window construction.
//! Boundary: keeps public operation counter payload shape separate from state.

use candid::CandidType;
use serde::Deserialize;

///
/// MetricRatio
///
/// MetricRatio carries a derived metric as an exact raw numerator and
/// denominator pair. Callers can choose their own decimal rendering policy
/// without losing precision inside the canister metrics layer.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricRatio {
    numerator: u64,
    denominator: u64,
}

impl MetricRatio {
    /// Returns the ratio numerator.
    #[must_use]
    pub const fn numerator(&self) -> u64 {
        self.numerator
    }

    /// Returns the ratio denominator.
    #[must_use]
    pub const fn denominator(&self) -> u64 {
        self.denominator
    }

    /// Returns the raw numerator and denominator pair.
    #[must_use]
    pub const fn into_numerator_and_denominator(self) -> (u64, u64) {
        (self.numerator, self.denominator)
    }
}

// Convert raw counter pairs into optional ratio values without encoding a
// sentinel for "no activity". Consumers can distinguish absent denominators
// from legitimate zero-valued work.
pub(in crate::metrics) const fn ratio(numerator: u64, denominator: u64) -> Option<MetricRatio> {
    if denominator == 0 {
        return None;
    }

    Some(MetricRatio {
        numerator,
        denominator,
    })
}

#[cfg_attr(doc, doc = "EventOps\n\nOperation counters.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct EventOps {
    // Executor entrypoints
    pub(crate) load_calls: u64,
    pub(crate) save_calls: u64,
    pub(crate) delete_calls: u64,
    pub(crate) save_insert_calls: u64,
    pub(crate) save_update_calls: u64,
    pub(crate) save_replace_calls: u64,
    pub(crate) exec_success: u64,
    pub(crate) exec_error_corruption: u64,
    pub(crate) exec_error_incompatible_persisted_format: u64,
    pub(crate) exec_error_not_found: u64,
    pub(crate) exec_error_internal: u64,
    pub(crate) exec_error_conflict: u64,
    pub(crate) exec_error_unsupported: u64,
    pub(crate) exec_error_invariant_violation: u64,
    pub(crate) exec_aborted: u64,
    pub(crate) cache_shared_query_plan_hits: u64,
    pub(crate) cache_shared_query_plan_misses: u64,
    pub(crate) cache_shared_query_plan_inserts: u64,
    pub(crate) cache_shared_query_plan_entries: u64,
    pub(crate) cache_shared_query_plan_miss_cold: u64,
    pub(crate) cache_shared_query_plan_miss_distinct_key: u64,
    pub(crate) cache_shared_query_plan_miss_schema_fingerprint: u64,
    pub(crate) cache_shared_query_plan_miss_visibility: u64,
    pub(crate) cache_sql_compiled_command_hits: u64,
    pub(crate) cache_sql_compiled_command_misses: u64,
    pub(crate) cache_sql_compiled_command_inserts: u64,
    pub(crate) cache_sql_compiled_command_entries: u64,
    pub(crate) cache_sql_compiled_command_miss_cold: u64,
    pub(crate) cache_sql_compiled_command_miss_distinct_key: u64,
    pub(crate) cache_sql_compiled_command_miss_schema_fingerprint: u64,
    pub(crate) cache_sql_compiled_command_miss_surface: u64,
    pub(crate) schema_reconcile_checks: u64,
    pub(crate) schema_reconcile_exact_match: u64,
    pub(crate) schema_reconcile_first_create: u64,
    pub(crate) schema_reconcile_latest_snapshot_corrupt: u64,
    pub(crate) schema_reconcile_rejected_field_slot: u64,
    pub(crate) schema_reconcile_rejected_other: u64,
    pub(crate) schema_reconcile_rejected_row_layout: u64,
    pub(crate) schema_reconcile_rejected_schema_version: u64,
    pub(crate) schema_reconcile_store_write_error: u64,
    pub(crate) schema_transition_checks: u64,
    pub(crate) schema_transition_add_expression_index: u64,
    pub(crate) schema_transition_add_field_path_index: u64,
    pub(crate) schema_transition_append_only_nullable_fields: u64,
    pub(crate) schema_transition_exact_match: u64,
    pub(crate) schema_transition_metadata_only_index_rename: u64,
    pub(crate) schema_transition_rejected_entity_identity: u64,
    pub(crate) schema_transition_rejected_field_contract: u64,
    pub(crate) schema_transition_rejected_field_slot: u64,
    pub(crate) schema_transition_rejected_row_layout: u64,
    pub(crate) schema_transition_rejected_schema_version: u64,
    pub(crate) schema_transition_rejected_snapshot: u64,
    pub(crate) schema_store_snapshots: u64,
    pub(crate) schema_store_encoded_bytes: u64,
    pub(crate) schema_store_latest_snapshot_bytes: u64,
    pub(crate) accepted_schema_fields: u64,
    pub(crate) accepted_schema_nested_leaf_facts: u64,
    pub(crate) sql_compile_rejects: u64,
    pub(crate) sql_compile_reject_cache_key: u64,
    pub(crate) sql_compile_reject_parse: u64,
    pub(crate) sql_compile_reject_semantic: u64,

    // Planner kinds
    pub(crate) plan_by_key: u64,
    pub(crate) plan_by_keys: u64,
    pub(crate) plan_key_range: u64,
    pub(crate) plan_index_branch_set: u64,
    pub(crate) plan_index_prefix: u64,
    pub(crate) plan_index_multi_lookup: u64,
    pub(crate) plan_index_range: u64,
    pub(crate) plan_explicit_full_scan: u64,
    pub(crate) plan_union: u64,
    pub(crate) plan_intersection: u64,
    pub(crate) plan_grouped_hash_materialized: u64,
    pub(crate) plan_grouped_ordered_materialized: u64,
    pub(crate) plan_choice_conflicting_primary_key_children_access_preferred: u64,
    pub(crate) plan_choice_constant_false_predicate: u64,
    pub(crate) plan_choice_empty_child_access_preferred: u64,
    pub(crate) plan_choice_full_scan_access: u64,
    pub(crate) plan_choice_intent_key_access_override: u64,
    pub(crate) plan_choice_limit_zero_window: u64,
    pub(crate) plan_choice_non_index_access: u64,
    pub(crate) plan_choice_planner_composite_non_index: u64,
    pub(crate) plan_choice_planner_full_scan_fallback: u64,
    pub(crate) plan_choice_planner_key_set_access: u64,
    pub(crate) plan_choice_planner_primary_key_lookup: u64,
    pub(crate) plan_choice_planner_primary_key_range: u64,
    pub(crate) plan_choice_required_order_primary_key_range_preferred: u64,
    pub(crate) plan_choice_singleton_primary_key_child_access_preferred: u64,
    pub(crate) prepared_shape_already_finalized: u64,

    // Rows touched
    pub(crate) rows_loaded: u64,
    pub(crate) rows_saved: u64,
    pub(crate) rows_inserted: u64,
    pub(crate) rows_updated: u64,
    pub(crate) rows_replaced: u64,
    pub(crate) rows_scanned: u64,
    pub(crate) rows_filtered: u64,
    pub(crate) rows_aggregated: u64,
    pub(crate) rows_emitted: u64,
    pub(crate) load_candidate_rows_scanned: u64,
    pub(crate) load_candidate_rows_filtered: u64,
    pub(crate) load_result_rows_emitted: u64,
    pub(crate) rows_deleted: u64,
    pub(crate) sql_insert_calls: u64,
    pub(crate) sql_insert_select_calls: u64,
    pub(crate) sql_update_calls: u64,
    pub(crate) sql_delete_calls: u64,
    pub(crate) sql_write_staged_rows: u64,
    pub(crate) sql_write_matched_rows: u64,
    pub(crate) sql_write_mutated_rows: u64,
    pub(crate) sql_write_returning_rows: u64,
    pub(crate) sql_write_error_insert: u64,
    pub(crate) sql_write_error_insert_select: u64,
    pub(crate) sql_write_error_update: u64,
    pub(crate) sql_write_error_delete: u64,
    pub(crate) sql_write_error_corruption: u64,
    pub(crate) sql_write_error_incompatible_persisted_format: u64,
    pub(crate) sql_write_error_not_found: u64,
    pub(crate) sql_write_error_internal: u64,
    pub(crate) sql_write_error_conflict: u64,
    pub(crate) sql_write_error_unsupported: u64,
    pub(crate) sql_write_error_invariant_violation: u64,

    // Index maintenance
    pub(crate) index_inserts: u64,
    pub(crate) index_removes: u64,
    pub(crate) reverse_index_inserts: u64,
    pub(crate) reverse_index_removes: u64,
    pub(crate) relation_reverse_lookups: u64,
    pub(crate) relation_delete_blocks: u64,
    pub(crate) write_rows_touched: u64,
    pub(crate) write_index_entries_changed: u64,
    pub(crate) write_reverse_index_entries_changed: u64,
    pub(crate) write_relation_checks: u64,
    pub(crate) unique_violations: u64,
    pub(crate) non_atomic_partial_commits: u64,
    pub(crate) non_atomic_partial_rows_committed: u64,
}

impl EventOps {
    #[must_use]
    pub const fn load_calls(&self) -> u64 {
        self.load_calls
    }

    #[must_use]
    pub const fn save_calls(&self) -> u64 {
        self.save_calls
    }

    #[must_use]
    pub const fn delete_calls(&self) -> u64 {
        self.delete_calls
    }

    #[must_use]
    pub const fn save_insert_calls(&self) -> u64 {
        self.save_insert_calls
    }

    #[must_use]
    pub const fn save_update_calls(&self) -> u64 {
        self.save_update_calls
    }

    #[must_use]
    pub const fn save_replace_calls(&self) -> u64 {
        self.save_replace_calls
    }

    #[must_use]
    pub const fn exec_success(&self) -> u64 {
        self.exec_success
    }

    #[must_use]
    pub const fn exec_error_corruption(&self) -> u64 {
        self.exec_error_corruption
    }

    #[must_use]
    pub const fn exec_error_incompatible_persisted_format(&self) -> u64 {
        self.exec_error_incompatible_persisted_format
    }

    #[must_use]
    pub const fn exec_error_not_found(&self) -> u64 {
        self.exec_error_not_found
    }

    #[must_use]
    pub const fn exec_error_internal(&self) -> u64 {
        self.exec_error_internal
    }

    #[must_use]
    pub const fn exec_error_conflict(&self) -> u64 {
        self.exec_error_conflict
    }

    #[must_use]
    pub const fn exec_error_unsupported(&self) -> u64 {
        self.exec_error_unsupported
    }

    #[must_use]
    pub const fn exec_error_invariant_violation(&self) -> u64 {
        self.exec_error_invariant_violation
    }

    #[must_use]
    pub const fn exec_aborted(&self) -> u64 {
        self.exec_aborted
    }

    #[must_use]
    pub const fn cache_shared_query_plan_hits(&self) -> u64 {
        self.cache_shared_query_plan_hits
    }

    #[must_use]
    pub const fn cache_shared_query_plan_misses(&self) -> u64 {
        self.cache_shared_query_plan_misses
    }

    #[must_use]
    pub const fn cache_shared_query_plan_inserts(&self) -> u64 {
        self.cache_shared_query_plan_inserts
    }

    #[must_use]
    pub const fn cache_shared_query_plan_entries(&self) -> u64 {
        self.cache_shared_query_plan_entries
    }

    #[must_use]
    pub const fn cache_shared_query_plan_miss_cold(&self) -> u64 {
        self.cache_shared_query_plan_miss_cold
    }

    #[must_use]
    pub const fn cache_shared_query_plan_miss_distinct_key(&self) -> u64 {
        self.cache_shared_query_plan_miss_distinct_key
    }

    #[must_use]
    pub const fn cache_shared_query_plan_miss_schema_fingerprint(&self) -> u64 {
        self.cache_shared_query_plan_miss_schema_fingerprint
    }

    #[must_use]
    pub const fn cache_shared_query_plan_miss_visibility(&self) -> u64 {
        self.cache_shared_query_plan_miss_visibility
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_hits(&self) -> u64 {
        self.cache_sql_compiled_command_hits
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_misses(&self) -> u64 {
        self.cache_sql_compiled_command_misses
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_inserts(&self) -> u64 {
        self.cache_sql_compiled_command_inserts
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_entries(&self) -> u64 {
        self.cache_sql_compiled_command_entries
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_miss_cold(&self) -> u64 {
        self.cache_sql_compiled_command_miss_cold
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_miss_distinct_key(&self) -> u64 {
        self.cache_sql_compiled_command_miss_distinct_key
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_miss_schema_fingerprint(&self) -> u64 {
        self.cache_sql_compiled_command_miss_schema_fingerprint
    }

    #[must_use]
    pub const fn cache_sql_compiled_command_miss_surface(&self) -> u64 {
        self.cache_sql_compiled_command_miss_surface
    }

    #[must_use]
    pub const fn schema_reconcile_checks(&self) -> u64 {
        self.schema_reconcile_checks
    }

    #[must_use]
    pub const fn schema_reconcile_exact_match(&self) -> u64 {
        self.schema_reconcile_exact_match
    }

    #[must_use]
    pub const fn schema_reconcile_first_create(&self) -> u64 {
        self.schema_reconcile_first_create
    }

    #[must_use]
    pub const fn schema_reconcile_latest_snapshot_corrupt(&self) -> u64 {
        self.schema_reconcile_latest_snapshot_corrupt
    }

    #[must_use]
    pub const fn schema_reconcile_rejected_field_slot(&self) -> u64 {
        self.schema_reconcile_rejected_field_slot
    }

    #[must_use]
    pub const fn schema_reconcile_rejected_other(&self) -> u64 {
        self.schema_reconcile_rejected_other
    }

    #[must_use]
    pub const fn schema_reconcile_rejected_row_layout(&self) -> u64 {
        self.schema_reconcile_rejected_row_layout
    }

    #[must_use]
    pub const fn schema_reconcile_rejected_schema_version(&self) -> u64 {
        self.schema_reconcile_rejected_schema_version
    }

    #[must_use]
    pub const fn schema_reconcile_store_write_error(&self) -> u64 {
        self.schema_reconcile_store_write_error
    }

    #[must_use]
    pub const fn schema_transition_checks(&self) -> u64 {
        self.schema_transition_checks
    }

    #[must_use]
    pub const fn schema_transition_add_expression_index(&self) -> u64 {
        self.schema_transition_add_expression_index
    }

    #[must_use]
    pub const fn schema_transition_add_field_path_index(&self) -> u64 {
        self.schema_transition_add_field_path_index
    }

    #[must_use]
    pub const fn schema_transition_append_only_nullable_fields(&self) -> u64 {
        self.schema_transition_append_only_nullable_fields
    }

    #[must_use]
    pub const fn schema_transition_exact_match(&self) -> u64 {
        self.schema_transition_exact_match
    }

    #[must_use]
    pub const fn schema_transition_metadata_only_index_rename(&self) -> u64 {
        self.schema_transition_metadata_only_index_rename
    }

    #[must_use]
    pub const fn schema_transition_rejected_entity_identity(&self) -> u64 {
        self.schema_transition_rejected_entity_identity
    }

    #[must_use]
    pub const fn schema_transition_rejected_field_contract(&self) -> u64 {
        self.schema_transition_rejected_field_contract
    }

    #[must_use]
    pub const fn schema_transition_rejected_field_slot(&self) -> u64 {
        self.schema_transition_rejected_field_slot
    }

    #[must_use]
    pub const fn schema_transition_rejected_row_layout(&self) -> u64 {
        self.schema_transition_rejected_row_layout
    }

    #[must_use]
    pub const fn schema_transition_rejected_schema_version(&self) -> u64 {
        self.schema_transition_rejected_schema_version
    }

    #[must_use]
    pub const fn schema_transition_rejected_snapshot(&self) -> u64 {
        self.schema_transition_rejected_snapshot
    }

    #[must_use]
    pub const fn schema_store_snapshots(&self) -> u64 {
        self.schema_store_snapshots
    }

    #[must_use]
    pub const fn schema_store_encoded_bytes(&self) -> u64 {
        self.schema_store_encoded_bytes
    }

    #[must_use]
    pub const fn schema_store_latest_snapshot_bytes(&self) -> u64 {
        self.schema_store_latest_snapshot_bytes
    }

    #[must_use]
    pub const fn accepted_schema_fields(&self) -> u64 {
        self.accepted_schema_fields
    }

    #[must_use]
    pub const fn accepted_schema_nested_leaf_facts(&self) -> u64 {
        self.accepted_schema_nested_leaf_facts
    }

    #[must_use]
    pub const fn sql_compile_rejects(&self) -> u64 {
        self.sql_compile_rejects
    }

    #[must_use]
    pub const fn sql_compile_reject_cache_key(&self) -> u64 {
        self.sql_compile_reject_cache_key
    }

    #[must_use]
    pub const fn sql_compile_reject_parse(&self) -> u64 {
        self.sql_compile_reject_parse
    }

    #[must_use]
    pub const fn sql_compile_reject_semantic(&self) -> u64 {
        self.sql_compile_reject_semantic
    }

    #[must_use]
    pub const fn plan_by_key(&self) -> u64 {
        self.plan_by_key
    }

    #[must_use]
    pub const fn plan_by_keys(&self) -> u64 {
        self.plan_by_keys
    }

    #[must_use]
    pub const fn plan_key_range(&self) -> u64 {
        self.plan_key_range
    }

    #[must_use]
    pub const fn plan_index_branch_set(&self) -> u64 {
        self.plan_index_branch_set
    }

    #[must_use]
    pub const fn plan_index_prefix(&self) -> u64 {
        self.plan_index_prefix
    }

    #[must_use]
    pub const fn plan_index_multi_lookup(&self) -> u64 {
        self.plan_index_multi_lookup
    }

    #[must_use]
    pub const fn plan_index_range(&self) -> u64 {
        self.plan_index_range
    }

    #[must_use]
    pub const fn plan_explicit_full_scan(&self) -> u64 {
        self.plan_explicit_full_scan
    }

    #[must_use]
    pub const fn plan_union(&self) -> u64 {
        self.plan_union
    }

    #[must_use]
    pub const fn plan_intersection(&self) -> u64 {
        self.plan_intersection
    }

    #[must_use]
    pub const fn plan_grouped_hash_materialized(&self) -> u64 {
        self.plan_grouped_hash_materialized
    }

    #[must_use]
    pub const fn plan_grouped_ordered_materialized(&self) -> u64 {
        self.plan_grouped_ordered_materialized
    }

    #[must_use]
    pub const fn plan_choice_conflicting_primary_key_children_access_preferred(&self) -> u64 {
        self.plan_choice_conflicting_primary_key_children_access_preferred
    }

    #[must_use]
    pub const fn plan_choice_constant_false_predicate(&self) -> u64 {
        self.plan_choice_constant_false_predicate
    }

    #[must_use]
    pub const fn plan_choice_empty_child_access_preferred(&self) -> u64 {
        self.plan_choice_empty_child_access_preferred
    }

    #[must_use]
    pub const fn plan_choice_full_scan_access(&self) -> u64 {
        self.plan_choice_full_scan_access
    }

    #[must_use]
    pub const fn plan_choice_intent_key_access_override(&self) -> u64 {
        self.plan_choice_intent_key_access_override
    }

    #[must_use]
    pub const fn plan_choice_limit_zero_window(&self) -> u64 {
        self.plan_choice_limit_zero_window
    }

    #[must_use]
    pub const fn plan_choice_non_index_access(&self) -> u64 {
        self.plan_choice_non_index_access
    }

    #[must_use]
    pub const fn plan_choice_planner_composite_non_index(&self) -> u64 {
        self.plan_choice_planner_composite_non_index
    }

    #[must_use]
    pub const fn plan_choice_planner_full_scan_fallback(&self) -> u64 {
        self.plan_choice_planner_full_scan_fallback
    }

    #[must_use]
    pub const fn plan_choice_planner_key_set_access(&self) -> u64 {
        self.plan_choice_planner_key_set_access
    }

    #[must_use]
    pub const fn plan_choice_planner_primary_key_lookup(&self) -> u64 {
        self.plan_choice_planner_primary_key_lookup
    }

    #[must_use]
    pub const fn plan_choice_planner_primary_key_range(&self) -> u64 {
        self.plan_choice_planner_primary_key_range
    }

    #[must_use]
    pub const fn plan_choice_required_order_primary_key_range_preferred(&self) -> u64 {
        self.plan_choice_required_order_primary_key_range_preferred
    }

    #[must_use]
    pub const fn plan_choice_singleton_primary_key_child_access_preferred(&self) -> u64 {
        self.plan_choice_singleton_primary_key_child_access_preferred
    }

    #[must_use]
    pub const fn prepared_shape_already_finalized(&self) -> u64 {
        self.prepared_shape_already_finalized
    }

    #[must_use]
    pub const fn rows_loaded(&self) -> u64 {
        self.rows_loaded
    }

    #[must_use]
    pub const fn rows_saved(&self) -> u64 {
        self.rows_saved
    }

    #[must_use]
    pub const fn rows_inserted(&self) -> u64 {
        self.rows_inserted
    }

    #[must_use]
    pub const fn rows_updated(&self) -> u64 {
        self.rows_updated
    }

    #[must_use]
    pub const fn rows_replaced(&self) -> u64 {
        self.rows_replaced
    }

    #[must_use]
    pub const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    #[must_use]
    pub const fn rows_filtered(&self) -> u64 {
        self.rows_filtered
    }

    #[must_use]
    pub const fn rows_aggregated(&self) -> u64 {
        self.rows_aggregated
    }

    #[must_use]
    pub const fn rows_emitted(&self) -> u64 {
        self.rows_emitted
    }

    #[must_use]
    pub const fn load_candidate_rows_scanned(&self) -> u64 {
        self.load_candidate_rows_scanned
    }

    #[must_use]
    pub const fn load_candidate_rows_filtered(&self) -> u64 {
        self.load_candidate_rows_filtered
    }

    #[must_use]
    pub const fn load_result_rows_emitted(&self) -> u64 {
        self.load_result_rows_emitted
    }

    #[must_use]
    pub const fn rows_deleted(&self) -> u64 {
        self.rows_deleted
    }

    #[must_use]
    pub const fn sql_insert_calls(&self) -> u64 {
        self.sql_insert_calls
    }

    #[must_use]
    pub const fn sql_insert_select_calls(&self) -> u64 {
        self.sql_insert_select_calls
    }

    #[must_use]
    pub const fn sql_update_calls(&self) -> u64 {
        self.sql_update_calls
    }

    #[must_use]
    pub const fn sql_delete_calls(&self) -> u64 {
        self.sql_delete_calls
    }

    #[must_use]
    pub const fn sql_write_staged_rows(&self) -> u64 {
        self.sql_write_staged_rows
    }

    #[must_use]
    pub const fn sql_write_matched_rows(&self) -> u64 {
        self.sql_write_matched_rows
    }

    #[must_use]
    pub const fn sql_write_mutated_rows(&self) -> u64 {
        self.sql_write_mutated_rows
    }

    #[must_use]
    pub const fn sql_write_returning_rows(&self) -> u64 {
        self.sql_write_returning_rows
    }

    #[must_use]
    pub const fn sql_write_error_insert(&self) -> u64 {
        self.sql_write_error_insert
    }

    #[must_use]
    pub const fn sql_write_error_insert_select(&self) -> u64 {
        self.sql_write_error_insert_select
    }

    #[must_use]
    pub const fn sql_write_error_update(&self) -> u64 {
        self.sql_write_error_update
    }

    #[must_use]
    pub const fn sql_write_error_delete(&self) -> u64 {
        self.sql_write_error_delete
    }

    #[must_use]
    pub const fn sql_write_error_corruption(&self) -> u64 {
        self.sql_write_error_corruption
    }

    #[must_use]
    pub const fn sql_write_error_incompatible_persisted_format(&self) -> u64 {
        self.sql_write_error_incompatible_persisted_format
    }

    #[must_use]
    pub const fn sql_write_error_not_found(&self) -> u64 {
        self.sql_write_error_not_found
    }

    #[must_use]
    pub const fn sql_write_error_internal(&self) -> u64 {
        self.sql_write_error_internal
    }

    #[must_use]
    pub const fn sql_write_error_conflict(&self) -> u64 {
        self.sql_write_error_conflict
    }

    #[must_use]
    pub const fn sql_write_error_unsupported(&self) -> u64 {
        self.sql_write_error_unsupported
    }

    #[must_use]
    pub const fn sql_write_error_invariant_violation(&self) -> u64 {
        self.sql_write_error_invariant_violation
    }

    #[must_use]
    pub const fn index_inserts(&self) -> u64 {
        self.index_inserts
    }

    #[must_use]
    pub const fn index_removes(&self) -> u64 {
        self.index_removes
    }

    #[must_use]
    pub const fn reverse_index_inserts(&self) -> u64 {
        self.reverse_index_inserts
    }

    #[must_use]
    pub const fn reverse_index_removes(&self) -> u64 {
        self.reverse_index_removes
    }

    #[must_use]
    pub const fn relation_reverse_lookups(&self) -> u64 {
        self.relation_reverse_lookups
    }

    #[must_use]
    pub const fn relation_delete_blocks(&self) -> u64 {
        self.relation_delete_blocks
    }

    #[must_use]
    pub const fn write_rows_touched(&self) -> u64 {
        self.write_rows_touched
    }

    #[must_use]
    pub const fn write_index_entries_changed(&self) -> u64 {
        self.write_index_entries_changed
    }

    #[must_use]
    pub const fn write_reverse_index_entries_changed(&self) -> u64 {
        self.write_reverse_index_entries_changed
    }

    #[must_use]
    pub const fn write_relation_checks(&self) -> u64 {
        self.write_relation_checks
    }

    #[must_use]
    pub const fn unique_violations(&self) -> u64 {
        self.unique_violations
    }

    #[must_use]
    pub const fn non_atomic_partial_commits(&self) -> u64 {
        self.non_atomic_partial_commits
    }

    #[must_use]
    pub const fn non_atomic_partial_rows_committed(&self) -> u64 {
        self.non_atomic_partial_rows_committed
    }

    /// Returns result rows emitted per load candidate row scanned.
    #[must_use]
    pub const fn load_selectivity_ratio(&self) -> Option<MetricRatio> {
        ratio(
            self.load_result_rows_emitted,
            self.load_candidate_rows_scanned,
        )
    }

    /// Returns candidate rows filtered per load candidate row scanned.
    #[must_use]
    pub const fn load_filter_ratio(&self) -> Option<MetricRatio> {
        ratio(
            self.load_candidate_rows_filtered,
            self.load_candidate_rows_scanned,
        )
    }

    /// Returns SQL-mutated rows per SQL-matched row.
    #[must_use]
    pub const fn sql_write_mutation_ratio(&self) -> Option<MetricRatio> {
        ratio(self.sql_write_mutated_rows, self.sql_write_matched_rows)
    }

    /// Returns SQL `RETURNING` rows per SQL-mutated row.
    #[must_use]
    pub const fn sql_write_returning_ratio(&self) -> Option<MetricRatio> {
        ratio(self.sql_write_returning_rows, self.sql_write_mutated_rows)
    }

    /// Returns primary index entries changed per write row touched.
    #[must_use]
    pub const fn write_index_entries_per_row(&self) -> Option<MetricRatio> {
        ratio(self.write_index_entries_changed, self.write_rows_touched)
    }

    /// Returns reverse-index entries changed per write row touched.
    #[must_use]
    pub const fn write_reverse_index_entries_per_row(&self) -> Option<MetricRatio> {
        ratio(
            self.write_reverse_index_entries_changed,
            self.write_rows_touched,
        )
    }

    /// Returns relation checks performed per write row touched.
    #[must_use]
    pub const fn write_relation_checks_per_row(&self) -> Option<MetricRatio> {
        ratio(self.write_relation_checks, self.write_rows_touched)
    }
}
