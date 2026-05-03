//! Module: metrics::state
//! Responsibility: mutable runtime metrics state and outward report DTOs.
//! Does not own: instrumentation call sites or sink routing.
//! Boundary: in-memory metrics state behind the crate-level sink/report surface.
//!
//! Runtime metrics are update-only by contract.
//! Query-side instrumentation is intentionally not surfaced by `report`, so
//! query metrics are non-existent by design under IC query semantics.

use candid::CandidType;
use canic_cdk::utils::time::now_millis;
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap};

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
    pub(crate) cache_sql_compiled_command_hits: u64,
    pub(crate) cache_sql_compiled_command_misses: u64,
    pub(crate) cache_sql_compiled_command_inserts: u64,
    pub(crate) cache_sql_compiled_command_entries: u64,

    // Planner kinds
    pub(crate) plan_index: u64,
    pub(crate) plan_keys: u64,
    pub(crate) plan_range: u64,
    pub(crate) plan_full_scan: u64,
    pub(crate) plan_by_key: u64,
    pub(crate) plan_by_keys: u64,
    pub(crate) plan_key_range: u64,
    pub(crate) plan_index_prefix: u64,
    pub(crate) plan_index_multi_lookup: u64,
    pub(crate) plan_index_range: u64,
    pub(crate) plan_explicit_full_scan: u64,
    pub(crate) plan_union: u64,
    pub(crate) plan_intersection: u64,
    pub(crate) plan_grouped_hash_materialized: u64,
    pub(crate) plan_grouped_ordered_materialized: u64,

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
    pub(crate) sql_write_matched_rows: u64,
    pub(crate) sql_write_mutated_rows: u64,
    pub(crate) sql_write_returning_rows: u64,

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
    pub const fn plan_index(&self) -> u64 {
        self.plan_index
    }

    #[must_use]
    pub const fn plan_keys(&self) -> u64 {
        self.plan_keys
    }

    #[must_use]
    pub const fn plan_range(&self) -> u64 {
        self.plan_range
    }

    #[must_use]
    pub const fn plan_full_scan(&self) -> u64 {
        self.plan_full_scan
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
    pub(crate) cache_sql_compiled_command_hits: u64,
    pub(crate) cache_sql_compiled_command_misses: u64,
    pub(crate) cache_sql_compiled_command_inserts: u64,
    pub(crate) plan_index: u64,
    pub(crate) plan_keys: u64,
    pub(crate) plan_range: u64,
    pub(crate) plan_full_scan: u64,
    pub(crate) plan_by_key: u64,
    pub(crate) plan_by_keys: u64,
    pub(crate) plan_key_range: u64,
    pub(crate) plan_index_prefix: u64,
    pub(crate) plan_index_multi_lookup: u64,
    pub(crate) plan_index_range: u64,
    pub(crate) plan_explicit_full_scan: u64,
    pub(crate) plan_union: u64,
    pub(crate) plan_intersection: u64,
    pub(crate) plan_grouped_hash_materialized: u64,
    pub(crate) plan_grouped_ordered_materialized: u64,
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
    pub(crate) sql_write_matched_rows: u64,
    pub(crate) sql_write_mutated_rows: u64,
    pub(crate) sql_write_returning_rows: u64,
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

#[cfg_attr(doc, doc = "EventReport\n\nMetrics query payload.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct EventReport {
    counters: Option<EventCounters>,
    entity_counters: Vec<EntitySummary>,
    window_filter_matched: bool,
    requested_window_start_ms: Option<u64>,
    active_window_start_ms: u64,
}

impl EventReport {
    #[must_use]
    pub(crate) const fn new(
        counters: Option<EventCounters>,
        entity_counters: Vec<EntitySummary>,
        window_filter_matched: bool,
        requested_window_start_ms: Option<u64>,
        active_window_start_ms: u64,
    ) -> Self {
        Self {
            counters,
            entity_counters,
            window_filter_matched,
            requested_window_start_ms,
            active_window_start_ms,
        }
    }

    #[must_use]
    pub const fn counters(&self) -> Option<&EventCounters> {
        self.counters.as_ref()
    }

    #[must_use]
    pub fn entity_counters(&self) -> &[EntitySummary] {
        &self.entity_counters
    }

    #[must_use]
    pub const fn window_filter_matched(&self) -> bool {
        self.window_filter_matched
    }

    #[must_use]
    pub const fn requested_window_start_ms(&self) -> Option<u64> {
        self.requested_window_start_ms
    }

    #[must_use]
    pub const fn active_window_start_ms(&self) -> u64 {
        self.active_window_start_ms
    }

    #[must_use]
    pub fn into_counters(self) -> Option<EventCounters> {
        self.counters
    }

    #[must_use]
    pub fn into_entity_counters(self) -> Vec<EntitySummary> {
        self.entity_counters
    }
}

//
// EventCounters
//
// Top-level metrics counters returned by `icydb_metrics()`.
// This keeps aggregate ops/perf totals while leaving per-entity detail to the
// separate `entity_counters` payload.
//

#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct EventCounters {
    pub(crate) ops: EventOps,
    pub(crate) perf: EventPerf,
    pub(crate) window_start_ms: u64,
    pub(crate) window_end_ms: u64,
    pub(crate) window_duration_ms: u64,
}

impl EventCounters {
    #[must_use]
    pub(crate) const fn new(
        ops: EventOps,
        perf: EventPerf,
        window_start_ms: u64,
        window_end_ms: u64,
    ) -> Self {
        Self {
            ops,
            perf,
            window_start_ms,
            window_end_ms,
            window_duration_ms: window_end_ms.saturating_sub(window_start_ms),
        }
    }

    #[must_use]
    pub const fn ops(&self) -> &EventOps {
        &self.ops
    }

    #[must_use]
    pub const fn perf(&self) -> &EventPerf {
        &self.perf
    }

    #[must_use]
    pub const fn window_start_ms(&self) -> u64 {
        self.window_start_ms
    }

    #[must_use]
    pub const fn window_end_ms(&self) -> u64 {
        self.window_end_ms
    }

    #[must_use]
    pub const fn window_duration_ms(&self) -> u64 {
        self.window_duration_ms
    }
}

#[cfg_attr(doc, doc = "EntitySummary\n\nPer-entity metrics summary.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct EntitySummary {
    path: String,
    load_calls: u64,
    save_calls: u64,
    delete_calls: u64,
    save_insert_calls: u64,
    save_update_calls: u64,
    save_replace_calls: u64,
    exec_success: u64,
    exec_error_corruption: u64,
    exec_error_incompatible_persisted_format: u64,
    exec_error_not_found: u64,
    exec_error_internal: u64,
    exec_error_conflict: u64,
    exec_error_unsupported: u64,
    exec_error_invariant_violation: u64,
    exec_aborted: u64,
    cache_shared_query_plan_hits: u64,
    cache_shared_query_plan_misses: u64,
    cache_shared_query_plan_inserts: u64,
    cache_sql_compiled_command_hits: u64,
    cache_sql_compiled_command_misses: u64,
    cache_sql_compiled_command_inserts: u64,
    plan_index: u64,
    plan_keys: u64,
    plan_range: u64,
    plan_full_scan: u64,
    plan_by_key: u64,
    plan_by_keys: u64,
    plan_key_range: u64,
    plan_index_prefix: u64,
    plan_index_multi_lookup: u64,
    plan_index_range: u64,
    plan_explicit_full_scan: u64,
    plan_union: u64,
    plan_intersection: u64,
    plan_grouped_hash_materialized: u64,
    plan_grouped_ordered_materialized: u64,
    rows_loaded: u64,
    rows_saved: u64,
    rows_inserted: u64,
    rows_updated: u64,
    rows_replaced: u64,
    rows_scanned: u64,
    rows_filtered: u64,
    rows_aggregated: u64,
    rows_emitted: u64,
    load_candidate_rows_scanned: u64,
    load_candidate_rows_filtered: u64,
    load_result_rows_emitted: u64,
    rows_deleted: u64,
    sql_insert_calls: u64,
    sql_insert_select_calls: u64,
    sql_update_calls: u64,
    sql_delete_calls: u64,
    sql_write_matched_rows: u64,
    sql_write_mutated_rows: u64,
    sql_write_returning_rows: u64,
    index_inserts: u64,
    index_removes: u64,
    reverse_index_inserts: u64,
    reverse_index_removes: u64,
    relation_reverse_lookups: u64,
    relation_delete_blocks: u64,
    write_rows_touched: u64,
    write_index_entries_changed: u64,
    write_reverse_index_entries_changed: u64,
    write_relation_checks: u64,
    unique_violations: u64,
    non_atomic_partial_commits: u64,
    non_atomic_partial_rows_committed: u64,
}

impl EntitySummary {
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

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
    pub const fn plan_index(&self) -> u64 {
        self.plan_index
    }

    #[must_use]
    pub const fn plan_keys(&self) -> u64 {
        self.plan_keys
    }

    #[must_use]
    pub const fn plan_range(&self) -> u64 {
        self.plan_range
    }

    #[must_use]
    pub const fn plan_full_scan(&self) -> u64 {
        self.plan_full_scan
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

    // Rank entity summaries by all visible activity so write-heavy or
    // maintenance-heavy entities are not hidden below read-heavy entities.
    const fn activity_score(&self) -> u64 {
        self.load_calls
            .saturating_add(self.save_calls)
            .saturating_add(self.delete_calls)
            .saturating_add(self.save_insert_calls)
            .saturating_add(self.save_update_calls)
            .saturating_add(self.save_replace_calls)
            .saturating_add(self.exec_success)
            .saturating_add(self.exec_error_corruption)
            .saturating_add(self.exec_error_incompatible_persisted_format)
            .saturating_add(self.exec_error_not_found)
            .saturating_add(self.exec_error_internal)
            .saturating_add(self.exec_error_conflict)
            .saturating_add(self.exec_error_unsupported)
            .saturating_add(self.exec_error_invariant_violation)
            .saturating_add(self.exec_aborted)
            .saturating_add(self.cache_shared_query_plan_hits)
            .saturating_add(self.cache_shared_query_plan_misses)
            .saturating_add(self.cache_shared_query_plan_inserts)
            .saturating_add(self.cache_sql_compiled_command_hits)
            .saturating_add(self.cache_sql_compiled_command_misses)
            .saturating_add(self.cache_sql_compiled_command_inserts)
            .saturating_add(self.plan_index)
            .saturating_add(self.plan_keys)
            .saturating_add(self.plan_range)
            .saturating_add(self.plan_full_scan)
            .saturating_add(self.plan_by_key)
            .saturating_add(self.plan_by_keys)
            .saturating_add(self.plan_key_range)
            .saturating_add(self.plan_index_prefix)
            .saturating_add(self.plan_index_multi_lookup)
            .saturating_add(self.plan_index_range)
            .saturating_add(self.plan_explicit_full_scan)
            .saturating_add(self.plan_union)
            .saturating_add(self.plan_intersection)
            .saturating_add(self.plan_grouped_hash_materialized)
            .saturating_add(self.plan_grouped_ordered_materialized)
            .saturating_add(self.rows_loaded)
            .saturating_add(self.rows_saved)
            .saturating_add(self.rows_inserted)
            .saturating_add(self.rows_updated)
            .saturating_add(self.rows_replaced)
            .saturating_add(self.rows_scanned)
            .saturating_add(self.rows_filtered)
            .saturating_add(self.rows_aggregated)
            .saturating_add(self.rows_emitted)
            .saturating_add(self.load_candidate_rows_scanned)
            .saturating_add(self.load_candidate_rows_filtered)
            .saturating_add(self.load_result_rows_emitted)
            .saturating_add(self.rows_deleted)
            .saturating_add(self.sql_insert_calls)
            .saturating_add(self.sql_insert_select_calls)
            .saturating_add(self.sql_update_calls)
            .saturating_add(self.sql_delete_calls)
            .saturating_add(self.sql_write_matched_rows)
            .saturating_add(self.sql_write_mutated_rows)
            .saturating_add(self.sql_write_returning_rows)
            .saturating_add(self.index_inserts)
            .saturating_add(self.index_removes)
            .saturating_add(self.reverse_index_inserts)
            .saturating_add(self.reverse_index_removes)
            .saturating_add(self.relation_reverse_lookups)
            .saturating_add(self.relation_delete_blocks)
            .saturating_add(self.write_rows_touched)
            .saturating_add(self.write_index_entries_changed)
            .saturating_add(self.write_reverse_index_entries_changed)
            .saturating_add(self.write_relation_checks)
            .saturating_add(self.unique_violations)
            .saturating_add(self.non_atomic_partial_commits)
            .saturating_add(self.non_atomic_partial_rows_committed)
    }
}

// Project mutable per-entity counters into the stable report DTO.
//
// Keeping this projection out of `report_window_start` leaves the window
// filtering logic readable while still making every report field explicit.
fn entity_summary_from_counters(path: &str, ops: &EntityCounters) -> EntitySummary {
    EntitySummary {
        path: path.to_string(),
        load_calls: ops.load_calls,
        save_calls: ops.save_calls,
        delete_calls: ops.delete_calls,
        save_insert_calls: ops.save_insert_calls,
        save_update_calls: ops.save_update_calls,
        save_replace_calls: ops.save_replace_calls,
        exec_success: ops.exec_success,
        exec_error_corruption: ops.exec_error_corruption,
        exec_error_incompatible_persisted_format: ops.exec_error_incompatible_persisted_format,
        exec_error_not_found: ops.exec_error_not_found,
        exec_error_internal: ops.exec_error_internal,
        exec_error_conflict: ops.exec_error_conflict,
        exec_error_unsupported: ops.exec_error_unsupported,
        exec_error_invariant_violation: ops.exec_error_invariant_violation,
        exec_aborted: ops.exec_aborted,
        cache_shared_query_plan_hits: ops.cache_shared_query_plan_hits,
        cache_shared_query_plan_misses: ops.cache_shared_query_plan_misses,
        cache_shared_query_plan_inserts: ops.cache_shared_query_plan_inserts,
        cache_sql_compiled_command_hits: ops.cache_sql_compiled_command_hits,
        cache_sql_compiled_command_misses: ops.cache_sql_compiled_command_misses,
        cache_sql_compiled_command_inserts: ops.cache_sql_compiled_command_inserts,
        plan_index: ops.plan_index,
        plan_keys: ops.plan_keys,
        plan_range: ops.plan_range,
        plan_full_scan: ops.plan_full_scan,
        plan_by_key: ops.plan_by_key,
        plan_by_keys: ops.plan_by_keys,
        plan_key_range: ops.plan_key_range,
        plan_index_prefix: ops.plan_index_prefix,
        plan_index_multi_lookup: ops.plan_index_multi_lookup,
        plan_index_range: ops.plan_index_range,
        plan_explicit_full_scan: ops.plan_explicit_full_scan,
        plan_union: ops.plan_union,
        plan_intersection: ops.plan_intersection,
        plan_grouped_hash_materialized: ops.plan_grouped_hash_materialized,
        plan_grouped_ordered_materialized: ops.plan_grouped_ordered_materialized,
        rows_loaded: ops.rows_loaded,
        rows_saved: ops.rows_saved,
        rows_inserted: ops.rows_inserted,
        rows_updated: ops.rows_updated,
        rows_replaced: ops.rows_replaced,
        rows_scanned: ops.rows_scanned,
        rows_filtered: ops.rows_filtered,
        rows_aggregated: ops.rows_aggregated,
        rows_emitted: ops.rows_emitted,
        load_candidate_rows_scanned: ops.load_candidate_rows_scanned,
        load_candidate_rows_filtered: ops.load_candidate_rows_filtered,
        load_result_rows_emitted: ops.load_result_rows_emitted,
        rows_deleted: ops.rows_deleted,
        sql_insert_calls: ops.sql_insert_calls,
        sql_insert_select_calls: ops.sql_insert_select_calls,
        sql_update_calls: ops.sql_update_calls,
        sql_delete_calls: ops.sql_delete_calls,
        sql_write_matched_rows: ops.sql_write_matched_rows,
        sql_write_mutated_rows: ops.sql_write_mutated_rows,
        sql_write_returning_rows: ops.sql_write_returning_rows,
        index_inserts: ops.index_inserts,
        index_removes: ops.index_removes,
        reverse_index_inserts: ops.reverse_index_inserts,
        reverse_index_removes: ops.reverse_index_removes,
        relation_reverse_lookups: ops.relation_reverse_lookups,
        relation_delete_blocks: ops.relation_delete_blocks,
        write_rows_touched: ops.write_rows_touched,
        write_index_entries_changed: ops.write_index_entries_changed,
        write_reverse_index_entries_changed: ops.write_reverse_index_entries_changed,
        write_relation_checks: ops.write_relation_checks,
        unique_violations: ops.unique_violations,
        non_atomic_partial_commits: ops.non_atomic_partial_commits,
        non_atomic_partial_rows_committed: ops.non_atomic_partial_rows_committed,
    }
}

// Build a metrics report gated by `window_start_ms`.
//
// This is a window-start filter:
// - If `window_start_ms` is `None`, return the current window.
// - If `window_start_ms <= state.window_start_ms`, return the current window.
// - If `window_start_ms > state.window_start_ms`, return an empty report.
//
// IcyDB stores aggregate counters only, so it cannot produce a precise
// sub-window report after `state.window_start_ms`.
#[must_use]
pub(super) fn report_window_start(window_start_ms: Option<u64>) -> EventReport {
    let snap = with_state(Clone::clone);
    if let Some(requested_window_start_ms) = window_start_ms
        && requested_window_start_ms > snap.window_start_ms
    {
        return EventReport::new(
            None,
            Vec::new(),
            false,
            window_start_ms,
            snap.window_start_ms,
        );
    }

    let mut entity_counters: Vec<EntitySummary> = Vec::new();
    for (path, ops) in &snap.entities {
        entity_counters.push(entity_summary_from_counters(path, ops));
    }

    entity_counters.sort_by(|a, b| {
        b.activity_score()
            .cmp(&a.activity_score())
            .then_with(|| b.rows_loaded.cmp(&a.rows_loaded))
            .then_with(|| b.rows_saved.cmp(&a.rows_saved))
            .then_with(|| b.rows_scanned.cmp(&a.rows_scanned))
            .then_with(|| b.rows_deleted.cmp(&a.rows_deleted))
            .then_with(|| a.path.cmp(&b.path))
    });

    EventReport::new(
        Some(EventCounters::new(
            snap.ops.clone(),
            snap.perf.clone(),
            snap.window_start_ms,
            now_millis(),
        )),
        entity_counters,
        true,
        window_start_ms,
        snap.window_start_ms,
    )
}
