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
    pub(crate) ops: EventOps,
    pub(crate) perf: EventPerf,
    pub(crate) entities: BTreeMap<String, EntityCounters>,
    pub(crate) window_start_ms: u64,
}

impl EventState {
    #[must_use]
    pub const fn new(
        ops: EventOps,
        perf: EventPerf,
        entities: BTreeMap<String, EntityCounters>,
        window_start_ms: u64,
    ) -> Self {
        Self {
            ops,
            perf,
            entities,
            window_start_ms,
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
    pub const fn entities(&self) -> &BTreeMap<String, EntityCounters> {
        &self.entities
    }

    #[must_use]
    pub const fn window_start_ms(&self) -> u64 {
        self.window_start_ms
    }
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
    pub(crate) load_calls: u64,
    pub(crate) save_calls: u64,
    pub(crate) delete_calls: u64,

    // Planner kinds
    pub(crate) plan_index: u64,
    pub(crate) plan_keys: u64,
    pub(crate) plan_range: u64,
    pub(crate) plan_full_scan: u64,
    pub(crate) plan_grouped_hash_materialized: u64,
    pub(crate) plan_grouped_ordered_materialized: u64,

    // Rows touched
    pub(crate) rows_loaded: u64,
    pub(crate) rows_scanned: u64,
    pub(crate) rows_deleted: u64,

    // Index maintenance
    pub(crate) index_inserts: u64,
    pub(crate) index_removes: u64,
    pub(crate) reverse_index_inserts: u64,
    pub(crate) reverse_index_removes: u64,
    pub(crate) relation_reverse_lookups: u64,
    pub(crate) relation_delete_blocks: u64,
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
    pub const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    #[must_use]
    pub const fn rows_deleted(&self) -> u64 {
        self.rows_deleted
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

/// EntityCounters
/// Per-entity counters mirroring `EventOps` categories.
/// Used to compute report-level per-entity summaries.

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntityCounters {
    pub(crate) load_calls: u64,
    pub(crate) save_calls: u64,
    pub(crate) delete_calls: u64,
    pub(crate) rows_loaded: u64,
    pub(crate) rows_scanned: u64,
    pub(crate) rows_deleted: u64,
    pub(crate) index_inserts: u64,
    pub(crate) index_removes: u64,
    pub(crate) reverse_index_inserts: u64,
    pub(crate) reverse_index_removes: u64,
    pub(crate) relation_reverse_lookups: u64,
    pub(crate) relation_delete_blocks: u64,
    pub(crate) unique_violations: u64,
    pub(crate) non_atomic_partial_commits: u64,
    pub(crate) non_atomic_partial_rows_committed: u64,
}

impl EntityCounters {
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
    pub const fn rows_loaded(&self) -> u64 {
        self.rows_loaded
    }

    #[must_use]
    pub const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    #[must_use]
    pub const fn rows_deleted(&self) -> u64 {
        self.rows_deleted
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

/// EventPerf
/// Aggregate and max instruction deltas per executor kind.
/// Captures execution pressure, not wall-clock latency.
/// Instruction deltas are pressure indicators (validation + planning + execution),
/// not latency measurements.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
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
    counters: Option<EventState>,
    /// Per-entity ephemeral counters and averages.
    entity_counters: Vec<EntitySummary>,
}

impl EventReport {
    #[must_use]
    pub(crate) const fn new(
        counters: Option<EventState>,
        entity_counters: Vec<EntitySummary>,
    ) -> Self {
        Self {
            counters,
            entity_counters,
        }
    }

    #[must_use]
    pub const fn counters(&self) -> Option<&EventState> {
        self.counters.as_ref()
    }

    #[must_use]
    pub fn entity_counters(&self) -> &[EntitySummary] {
        &self.entity_counters
    }

    #[must_use]
    pub fn into_counters(self) -> Option<EventState> {
        self.counters
    }

    #[must_use]
    pub fn into_entity_counters(self) -> Vec<EntitySummary> {
        self.entity_counters
    }
}

/// EntitySummary
/// Derived per-entity metrics for report consumers.
/// Includes absolute counters and simple averages.

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntitySummary {
    path: String,
    load_calls: u64,
    delete_calls: u64,
    rows_loaded: u64,
    rows_scanned: u64,
    rows_deleted: u64,
    avg_rows_per_load: f64,
    avg_rows_scanned_per_load: f64,
    avg_rows_per_delete: f64,
    index_inserts: u64,
    index_removes: u64,
    reverse_index_inserts: u64,
    reverse_index_removes: u64,
    relation_reverse_lookups: u64,
    relation_delete_blocks: u64,
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
    pub const fn delete_calls(&self) -> u64 {
        self.delete_calls
    }

    #[must_use]
    pub const fn rows_loaded(&self) -> u64 {
        self.rows_loaded
    }

    #[must_use]
    pub const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    #[must_use]
    pub const fn rows_deleted(&self) -> u64 {
        self.rows_deleted
    }

    #[must_use]
    pub const fn avg_rows_per_load(&self) -> f64 {
        self.avg_rows_per_load
    }

    #[must_use]
    pub const fn avg_rows_scanned_per_load(&self) -> f64 {
        self.avg_rows_scanned_per_load
    }

    #[must_use]
    pub const fn avg_rows_per_delete(&self) -> f64 {
        self.avg_rows_per_delete
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
            non_atomic_partial_commits: ops.non_atomic_partial_commits,
            non_atomic_partial_rows_committed: ops.non_atomic_partial_rows_committed,
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

    EventReport::new(Some(snap), entity_counters)
}

///
/// TESTS
///

#[cfg(test)]
#[expect(clippy::float_cmp)]
mod tests {
    use crate::obs::metrics::{
        EntityCounters, EntitySummary, EventOps, EventPerf, EventReport, EventState,
        report_window_start, reset_all, with_state, with_state_mut,
    };
    use serde::Serialize;
    use serde_cbor::Value as CborValue;
    use std::collections::BTreeMap;

    fn to_cbor_value<T: Serialize>(value: &T) -> CborValue {
        let bytes =
            serde_cbor::to_vec(value).expect("test fixtures must serialize into CBOR payloads");
        serde_cbor::from_slice::<CborValue>(&bytes)
            .expect("test fixtures must deserialize into CBOR value trees")
    }

    fn expect_cbor_map(value: &CborValue) -> &BTreeMap<CborValue, CborValue> {
        match value {
            CborValue::Map(map) => map,
            other => panic!("expected CBOR map, got {other:?}"),
        }
    }

    fn map_field<'a>(map: &'a BTreeMap<CborValue, CborValue>, key: &str) -> Option<&'a CborValue> {
        map.get(&CborValue::Text(key.to_string()))
    }

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

    #[test]
    fn event_report_serialization_shape_is_stable() {
        let report = EventReport {
            counters: Some(EventState {
                ops: EventOps {
                    load_calls: 1,
                    rows_loaded: 2,
                    rows_scanned: 3,
                    non_atomic_partial_rows_committed: 4,
                    ..Default::default()
                },
                perf: EventPerf {
                    load_inst_total: 11,
                    load_inst_max: 12,
                    ..Default::default()
                },
                entities: BTreeMap::from([(
                    "alpha".to_string(),
                    EntityCounters {
                        load_calls: 5,
                        rows_loaded: 8,
                        ..Default::default()
                    },
                )]),
                window_start_ms: 99,
            }),
            entity_counters: vec![EntitySummary {
                path: "alpha".to_string(),
                load_calls: 5,
                rows_loaded: 8,
                avg_rows_per_load: 1.6,
                ..Default::default()
            }],
        };

        let encoded = to_cbor_value(&report);
        let root = expect_cbor_map(&encoded);
        assert!(
            map_field(root, "counters").is_some(),
            "EventReport must keep `counters` as serialized field key",
        );
        assert!(
            map_field(root, "entity_counters").is_some(),
            "EventReport must keep `entity_counters` as serialized field key",
        );

        let counters = map_field(root, "counters").expect("counters payload should exist");
        let counters_map = expect_cbor_map(counters);
        assert!(
            map_field(counters_map, "ops").is_some(),
            "EventState must keep `ops` as serialized field key",
        );
        assert!(
            map_field(counters_map, "perf").is_some(),
            "EventState must keep `perf` as serialized field key",
        );
        assert!(
            map_field(counters_map, "entities").is_some(),
            "EventState must keep `entities` as serialized field key",
        );
        assert!(
            map_field(counters_map, "window_start_ms").is_some(),
            "EventState must keep `window_start_ms` as serialized field key",
        );
    }

    #[test]
    fn entity_summary_serialization_shape_is_stable() {
        let encoded = to_cbor_value(&EntitySummary {
            path: "alpha".to_string(),
            load_calls: 5,
            delete_calls: 6,
            rows_loaded: 8,
            rows_scanned: 9,
            rows_deleted: 10,
            avg_rows_per_load: 1.6,
            avg_rows_scanned_per_load: 1.8,
            avg_rows_per_delete: 2.0,
            index_inserts: 11,
            index_removes: 12,
            reverse_index_inserts: 13,
            reverse_index_removes: 14,
            relation_reverse_lookups: 15,
            relation_delete_blocks: 16,
            unique_violations: 17,
            non_atomic_partial_commits: 18,
            non_atomic_partial_rows_committed: 19,
        });
        let root = expect_cbor_map(&encoded);
        assert!(
            map_field(root, "path").is_some(),
            "EntitySummary must keep `path` as serialized field key",
        );
        assert!(
            map_field(root, "avg_rows_per_load").is_some(),
            "EntitySummary must keep `avg_rows_per_load` as serialized field key",
        );
        assert!(
            map_field(root, "relation_delete_blocks").is_some(),
            "EntitySummary must keep `relation_delete_blocks` as serialized field key",
        );
        assert!(
            map_field(root, "non_atomic_partial_rows_committed").is_some(),
            "EntitySummary must keep `non_atomic_partial_rows_committed` as serialized field key",
        );
    }
}
