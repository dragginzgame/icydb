//! Runtime metrics are update-only by contract.
//! Query-side instrumentation is intentionally not surfaced by `report`, so
//! query metrics are non-existent by design under IC query semantics.

use candid::CandidType;
use canic_cdk::utils::time::now_millis;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, cmp::Ordering, collections::BTreeMap};

/// EventState
/// Mutable runtime counters and rolling perf state for the current window.
/// Stored in thread-local memory for update-only instrumentation.

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct EventState {
    pub ops: EventOps,
    pub perf: EventPerf,
    pub entities: BTreeMap<String, EntityCounters>,
    pub window_start_ms: u64,
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

/// EventOps
/// Aggregated operation counters for executors, plans, rows, and index maintenance.
/// Values are monotonic within a metrics window.
/// Call counters are execution attempts; errors still increment them.
/// Row counters reflect rows touched after execution, not requested rows.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventOps {
    // Executor entrypoints
    pub load_calls: u64,
    pub save_calls: u64,
    pub delete_calls: u64,

    // Planner kinds
    pub plan_index: u64,
    pub plan_keys: u64,
    pub plan_range: u64,
    pub plan_full_scan: u64,

    // Rows touched
    pub rows_loaded: u64,
    pub rows_scanned: u64,
    pub rows_deleted: u64,

    // Index maintenance
    pub index_inserts: u64,
    pub index_removes: u64,
    pub reverse_index_inserts: u64,
    pub reverse_index_removes: u64,
    pub relation_reverse_lookups: u64,
    pub relation_delete_blocks: u64,
    pub unique_violations: u64,
}

/// EntityCounters
/// Per-entity counters mirroring `EventOps` categories.
/// Used to compute report-level per-entity summaries.

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntityCounters {
    pub load_calls: u64,
    pub save_calls: u64,
    pub delete_calls: u64,
    pub rows_loaded: u64,
    pub rows_scanned: u64,
    pub rows_deleted: u64,
    pub index_inserts: u64,
    pub index_removes: u64,
    pub reverse_index_inserts: u64,
    pub reverse_index_removes: u64,
    pub relation_reverse_lookups: u64,
    pub relation_delete_blocks: u64,
    pub unique_violations: u64,
}

/// EventPerf
/// Aggregate and max instruction deltas per executor kind.
/// Captures execution pressure, not wall-clock latency.
/// Instruction deltas are pressure indicators (validation + planning + execution),
/// not latency measurements.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventPerf {
    // Instruction totals per executor (ic_cdk::api::performance_counter(1))
    pub load_inst_total: u128,
    pub save_inst_total: u128,
    pub delete_inst_total: u128,

    // Maximum observed instruction deltas
    pub load_inst_max: u64,
    pub save_inst_max: u64,
    pub delete_inst_max: u64,
}

thread_local! {
    static EVENT_STATE: RefCell<EventState> = RefCell::new(EventState::default());
}

/// Borrow metrics immutably.
pub(crate) fn with_state<R>(f: impl FnOnce(&EventState) -> R) -> R {
    EVENT_STATE.with(|m| f(&m.borrow()))
}

/// Borrow metrics mutably.
pub(crate) fn with_state_mut<R>(f: impl FnOnce(&mut EventState) -> R) -> R {
    EVENT_STATE.with(|m| f(&mut m.borrow_mut()))
}

/// Reset all counters (useful in tests).
pub(super) fn reset() {
    with_state_mut(|m| *m = EventState::default());
}

/// Reset all event state: counters, perf, and serialize counters.
pub(crate) fn reset_all() {
    reset();
}

/// Accumulate instruction counts and track a max.
pub(super) fn add_instructions(total: &mut u128, max: &mut u64, delta_inst: u64) {
    *total = total.saturating_add(u128::from(delta_inst));
    if delta_inst > *max {
        *max = delta_inst;
    }
}

/// EventReport
/// Event/counter report for runtime metrics query endpoints.
/// Storage snapshot types live in snapshot/storage modules.

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventReport {
    /// Ephemeral runtime counters since `window_start_ms`.
    pub counters: Option<EventState>,
    /// Per-entity ephemeral counters and averages.
    pub entity_counters: Vec<EntitySummary>,
}

/// EntitySummary
/// Derived per-entity metrics for report consumers.
/// Includes absolute counters and simple averages.

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntitySummary {
    pub path: String,
    pub load_calls: u64,
    pub delete_calls: u64,
    pub rows_loaded: u64,
    pub rows_scanned: u64,
    pub rows_deleted: u64,
    pub avg_rows_per_load: f64,
    pub avg_rows_scanned_per_load: f64,
    pub avg_rows_per_delete: f64,
    pub index_inserts: u64,
    pub index_removes: u64,
    pub reverse_index_inserts: u64,
    pub reverse_index_removes: u64,
    pub relation_reverse_lookups: u64,
    pub relation_delete_blocks: u64,
    pub unique_violations: u64,
}

/// Build a metrics report gated by `window_start_ms`.
///
/// This is a window-start filter:
/// - If `window_start_ms` is `None`, return the current window.
/// - If `window_start_ms <= state.window_start_ms`, return the current window.
/// - If `window_start_ms > state.window_start_ms`, return an empty report.
///
/// IcyDB stores aggregate counters only, so it cannot produce a precise
/// sub-window report after `state.window_start_ms`.
#[must_use]
#[expect(clippy::cast_precision_loss)]
pub(super) fn report_window_start(window_start_ms: Option<u64>) -> EventReport {
    let snap = with_state(Clone::clone);
    if let Some(requested_window_start_ms) = window_start_ms
        && requested_window_start_ms > snap.window_start_ms
    {
        return EventReport::default();
    }

    let mut entity_counters: Vec<EntitySummary> = Vec::new();
    for (path, ops) in &snap.entities {
        let avg_load = if ops.load_calls > 0 {
            ops.rows_loaded as f64 / ops.load_calls as f64
        } else {
            0.0
        };
        let avg_scanned = if ops.load_calls > 0 {
            ops.rows_scanned as f64 / ops.load_calls as f64
        } else {
            0.0
        };
        let avg_delete = if ops.delete_calls > 0 {
            ops.rows_deleted as f64 / ops.delete_calls as f64
        } else {
            0.0
        };

        entity_counters.push(EntitySummary {
            path: path.clone(),
            load_calls: ops.load_calls,
            delete_calls: ops.delete_calls,
            rows_loaded: ops.rows_loaded,
            rows_scanned: ops.rows_scanned,
            rows_deleted: ops.rows_deleted,
            avg_rows_per_load: avg_load,
            avg_rows_scanned_per_load: avg_scanned,
            avg_rows_per_delete: avg_delete,
            index_inserts: ops.index_inserts,
            index_removes: ops.index_removes,
            reverse_index_inserts: ops.reverse_index_inserts,
            reverse_index_removes: ops.reverse_index_removes,
            relation_reverse_lookups: ops.relation_reverse_lookups,
            relation_delete_blocks: ops.relation_delete_blocks,
            unique_violations: ops.unique_violations,
        });
    }

    entity_counters.sort_by(|a, b| {
        match b
            .avg_rows_per_load
            .partial_cmp(&a.avg_rows_per_load)
            .unwrap_or(Ordering::Equal)
        {
            Ordering::Equal => match b.rows_loaded.cmp(&a.rows_loaded) {
                Ordering::Equal => a.path.cmp(&b.path),
                other => other,
            },
            other => other,
        }
    });

    EventReport {
        counters: Some(snap),
        entity_counters,
    }
}

///
/// TESTS
///

#[cfg(test)]
#[expect(clippy::float_cmp)]
mod tests {
    use crate::obs::metrics::{
        EntityCounters, report_window_start, reset_all, with_state, with_state_mut,
    };

    #[test]
    fn reset_all_clears_state() {
        with_state_mut(|m| {
            m.ops.load_calls = 3;
            m.ops.index_inserts = 2;
            m.perf.save_inst_max = 9;
            m.entities.insert(
                "alpha".to_string(),
                EntityCounters {
                    load_calls: 1,
                    ..Default::default()
                },
            );
        });

        reset_all();

        with_state(|m| {
            assert_eq!(m.ops.load_calls, 0);
            assert_eq!(m.ops.index_inserts, 0);
            assert_eq!(m.perf.save_inst_max, 0);
            assert!(m.entities.is_empty());
        });
    }

    #[test]
    fn report_sorts_entities_by_average_rows() {
        reset_all();
        with_state_mut(|m| {
            m.entities.insert(
                "alpha".to_string(),
                EntityCounters {
                    load_calls: 2,
                    rows_loaded: 6,
                    ..Default::default()
                },
            );
            m.entities.insert(
                "beta".to_string(),
                EntityCounters {
                    load_calls: 1,
                    rows_loaded: 5,
                    ..Default::default()
                },
            );
            m.entities.insert(
                "gamma".to_string(),
                EntityCounters {
                    load_calls: 2,
                    rows_loaded: 6,
                    ..Default::default()
                },
            );
        });

        let report = report_window_start(None);
        let paths: Vec<_> = report
            .entity_counters
            .iter()
            .map(|e| e.path.as_str())
            .collect();

        // Order by avg rows per load desc, then rows_loaded desc, then path asc.
        assert_eq!(paths, ["beta", "alpha", "gamma"]);
        assert_eq!(report.entity_counters[0].avg_rows_per_load, 5.0);
        assert_eq!(report.entity_counters[1].avg_rows_per_load, 3.0);
        assert_eq!(report.entity_counters[2].avg_rows_per_load, 3.0);
    }
}
