//! Module: metrics::state
//! Responsibility: mutable runtime metrics state and outward report DTOs.
//! Does not own: instrumentation call sites or sink routing.
//! Boundary: in-memory metrics state behind the crate-level sink/report surface.
//!
//! Runtime metrics are update-only by contract.
//! Query-side instrumentation is intentionally not surfaced by `report`, so
//! query metrics are non-existent by design under IC query semantics.

use crate::runtime::now_millis;
use candid::CandidType;
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap};

mod compact;
mod ops;
mod report;
mod summary;

pub(super) use compact::compact_report_window_start;
pub use compact::{
    CompactEntityMetrics, CompactEventCounters, CompactMetric, CompactMetricsReport,
    compact_metric_code,
};
pub(in crate::metrics) use ops::ratio;
pub use ops::{EventOps, MetricRatio};
pub(super) use report::report_window_start;
pub use report::{EventCounters, EventReport};
pub use summary::EntitySummary;
pub(in crate::metrics) use summary::entity_summary_from_counters;

#[derive(Clone, Debug)]
pub(crate) struct EventState {
    pub(crate) ops: EventOps,
    pub(crate) perf: EventPerf,
    pub(crate) entities: BTreeMap<String, EntityCounters>,
    pub(crate) window_start_ms: u64,
}

impl Default for EventState {
    fn default() -> Self {
        Self {
            ops: EventOps::default(),
            perf: EventPerf::default(),
            entities: BTreeMap::new(),
            window_start_ms: now_millis(),
        }
    }
}
#[derive(Clone, Debug, Default)]
pub(crate) struct EntityCounters {
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
    pub(crate) cache_shared_query_plan_miss_cold: u64,
    pub(crate) cache_shared_query_plan_miss_distinct_key: u64,
    pub(crate) cache_shared_query_plan_miss_schema_fingerprint: u64,
    pub(crate) cache_shared_query_plan_miss_visibility: u64,
    pub(crate) cache_sql_compiled_command_hits: u64,
    pub(crate) cache_sql_compiled_command_misses: u64,
    pub(crate) cache_sql_compiled_command_inserts: u64,
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
    pub(crate) plan_grouped_ordered_streaming: u64,
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

#[cfg_attr(doc, doc = "EventPerf\n\nInstruction totals and maxima.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct EventPerf {
    // Instruction totals per executor (ic_cdk::api::performance_counter(1))
    pub(crate) load_inst_total: u128,
    pub(crate) save_inst_total: u128,
    pub(crate) delete_inst_total: u128,

    // Maximum observed instruction deltas
    pub(crate) load_inst_max: u64,
    pub(crate) save_inst_max: u64,
    pub(crate) delete_inst_max: u64,
}

impl EventPerf {
    #[must_use]
    pub const fn new(
        load_inst_total: u128,
        save_inst_total: u128,
        delete_inst_total: u128,
        load_inst_max: u64,
        save_inst_max: u64,
        delete_inst_max: u64,
    ) -> Self {
        Self {
            load_inst_total,
            save_inst_total,
            delete_inst_total,
            load_inst_max,
            save_inst_max,
            delete_inst_max,
        }
    }

    #[must_use]
    pub const fn load_inst_total(&self) -> u128 {
        self.load_inst_total
    }

    #[must_use]
    pub const fn save_inst_total(&self) -> u128 {
        self.save_inst_total
    }

    #[must_use]
    pub const fn delete_inst_total(&self) -> u128 {
        self.delete_inst_total
    }

    #[must_use]
    pub const fn load_inst_max(&self) -> u64 {
        self.load_inst_max
    }

    #[must_use]
    pub const fn save_inst_max(&self) -> u64 {
        self.save_inst_max
    }

    #[must_use]
    pub const fn delete_inst_max(&self) -> u64 {
        self.delete_inst_max
    }
}

thread_local! {
    static EVENT_STATE: RefCell<EventState> = RefCell::new(EventState::default());
}

// Borrow metrics immutably.
pub(crate) fn with_state<R>(f: impl FnOnce(&EventState) -> R) -> R {
    EVENT_STATE.with(|m| f(&m.borrow()))
}

// Borrow metrics mutably.
pub(crate) fn with_state_mut<R>(f: impl FnOnce(&mut EventState) -> R) -> R {
    EVENT_STATE.with(|m| f(&mut m.borrow_mut()))
}

// Reset all counters (useful in tests).
pub(super) fn reset() {
    with_state_mut(|m| *m = EventState::default());
}

// Reset all event state: counters, perf, and serialize counters.
pub(crate) fn reset_all() {
    reset();
}

// Accumulate instruction counts and track a max.
pub(super) fn add_instructions(total: &mut u128, max: &mut u64, delta_inst: u64) {
    *total = total.saturating_add(u128::from(delta_inst));
    if delta_inst > *max {
        *max = delta_inst;
    }
}
