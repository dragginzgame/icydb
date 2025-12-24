use crate::traits::EntityKind;
use candid::CandidType;
use canic_cdk::{api::performance_counter, utils::time::now_millis};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, cmp::Ordering, collections::BTreeMap, marker::PhantomData};

///
/// Metrics
/// Ephemeral, in-memory counters and simple perf totals for operations.
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct EventState {
    pub ops: EventOps,
    pub perf: EventPerf,
    pub entities: BTreeMap<String, EntityCounters>,
    pub since_ms: u64,
}

impl Default for EventState {
    fn default() -> Self {
        Self {
            ops: EventOps::default(),
            perf: EventPerf::default(),
            entities: BTreeMap::new(),
            since_ms: now_millis(),
        }
    }
}

///
/// EventOps
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventOps {
    // Executor entrypoints
    pub load_calls: u64,
    pub exists_calls: u64,
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
    pub unique_violations: u64,
}

///
/// EntityOps
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntityCounters {
    pub load_calls: u64,
    pub exists_calls: u64,
    pub save_calls: u64,
    pub delete_calls: u64,
    pub rows_loaded: u64,
    pub rows_scanned: u64,
    pub rows_deleted: u64,
    pub index_inserts: u64,
    pub index_removes: u64,
    pub unique_violations: u64,
}

///
/// Perf
///

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
pub fn reset() {
    with_state_mut(|m| *m = EventState::default());
}

/// Reset all event state: counters, perf, and serialize counters.
pub fn reset_all() {
    reset();
}

/// Accumulate instruction counts and track a max.
#[allow(clippy::missing_const_for_fn)]
pub fn add_instructions(total: &mut u128, max: &mut u64, delta_inst: u64) {
    *total = total.saturating_add(u128::from(delta_inst));
    if delta_inst > *max {
        *max = delta_inst;
    }
}

///
/// ExecKind
///

#[derive(Clone, Copy, Debug)]
pub enum ExecKind {
    Load,
    Save,
    Delete,
}

/// Begin an executor timing span and increment call counters.
/// Returns the start instruction counter value.
#[must_use]
pub(crate) fn exec_start(kind: ExecKind) -> u64 {
    with_state_mut(|m| match kind {
        ExecKind::Load => m.ops.load_calls = m.ops.load_calls.saturating_add(1),
        ExecKind::Save => m.ops.save_calls = m.ops.save_calls.saturating_add(1),
        ExecKind::Delete => m.ops.delete_calls = m.ops.delete_calls.saturating_add(1),
    });

    // Instruction counter (counter_type = 1) is per-message and monotonic.
    performance_counter(1)
}

/// Finish an executor timing span and aggregate instruction deltas and row counters.
pub(crate) fn exec_finish(kind: ExecKind, start_inst: u64, rows_touched: u64) {
    let now = performance_counter(1);
    let delta = now.saturating_sub(start_inst);

    with_state_mut(|m| match kind {
        ExecKind::Load => {
            m.ops.rows_loaded = m.ops.rows_loaded.saturating_add(rows_touched);
            add_instructions(
                &mut m.perf.load_inst_total,
                &mut m.perf.load_inst_max,
                delta,
            );
        }
        ExecKind::Save => {
            add_instructions(
                &mut m.perf.save_inst_total,
                &mut m.perf.save_inst_max,
                delta,
            );
        }
        ExecKind::Delete => {
            m.ops.rows_deleted = m.ops.rows_deleted.saturating_add(rows_touched);
            add_instructions(
                &mut m.perf.delete_inst_total,
                &mut m.perf.delete_inst_max,
                delta,
            );
        }
    });
}

/// Per-entity variants using EntityKind::PATH
#[must_use]
pub(crate) fn exec_start_for<E>(kind: ExecKind) -> u64
where
    E: EntityKind,
{
    let start = exec_start(kind);
    with_state_mut(|m| {
        let entry = m.entities.entry(E::PATH.to_string()).or_default();
        match kind {
            ExecKind::Load => entry.load_calls = entry.load_calls.saturating_add(1),
            ExecKind::Save => entry.save_calls = entry.save_calls.saturating_add(1),
            ExecKind::Delete => entry.delete_calls = entry.delete_calls.saturating_add(1),
        }
    });
    start
}

/// Finish a per-entity span and accumulate rows touched.
pub(crate) fn exec_finish_for<E>(kind: ExecKind, start_inst: u64, rows_touched: u64)
where
    E: EntityKind,
{
    exec_finish(kind, start_inst, rows_touched);
    with_state_mut(|m| {
        let entry = m.entities.entry(E::PATH.to_string()).or_default();
        match kind {
            ExecKind::Load => entry.rows_loaded = entry.rows_loaded.saturating_add(rows_touched),
            ExecKind::Delete => {
                entry.rows_deleted = entry.rows_deleted.saturating_add(rows_touched);
            }
            ExecKind::Save => {}
        }
    });
}

///
/// Span
/// RAII guard to simplify metrics instrumentation
///

pub(crate) struct Span<E: EntityKind> {
    kind: ExecKind,
    start: u64,
    rows: u64,
    finished: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Span<E> {
    #[must_use]
    /// Start a metrics span for a specific entity and executor kind.
    pub(crate) fn new(kind: ExecKind) -> Self {
        Self {
            kind,
            start: exec_start_for::<E>(kind),
            rows: 0,
            finished: false,
            _marker: PhantomData,
        }
    }

    pub(crate) const fn set_rows(&mut self, rows: u64) {
        self.rows = rows;
    }

    #[expect(dead_code)]
    /// Increment the recorded row count.
    pub(crate) const fn add_rows(&mut self, rows: u64) {
        self.rows = self.rows.saturating_add(rows);
    }

    #[expect(dead_code)]
    /// Finish the span early (also happens on Drop).
    pub(crate) fn finish(mut self) {
        if !self.finished {
            exec_finish_for::<E>(self.kind, self.start, self.rows);
            self.finished = true;
        }
    }
}

impl<E: EntityKind> Drop for Span<E> {
    fn drop(&mut self) {
        if !self.finished {
            exec_finish_for::<E>(self.kind, self.start, self.rows);
            self.finished = true;
        }
    }
}

///
/// EventReport
/// Event/counter report; storage snapshot types live in snapshot/storage modules.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventReport {
    /// Ephemeral runtime counters since `since_ms`.
    pub counters: Option<EventState>,
    /// Per-entity ephemeral counters and averages.
    pub entity_counters: Vec<EntitySummary>,
}

///
/// EntitySummary
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntitySummary {
    pub path: String,
    pub load_calls: u64,
    pub exists_calls: u64,
    pub delete_calls: u64,
    pub rows_loaded: u64,
    pub rows_scanned: u64,
    pub rows_deleted: u64,
    pub avg_rows_per_load: f64,
    pub avg_rows_scanned_per_load: f64,
    pub avg_rows_per_delete: f64,
    pub index_inserts: u64,
    pub index_removes: u64,
    pub unique_violations: u64,
}

/// Increment unique-violation counters globally and for a specific entity type.
pub(crate) fn record_unique_violation_for<E>(m: &mut EventState)
where
    E: crate::traits::EntityKind,
{
    m.ops.unique_violations = m.ops.unique_violations.saturating_add(1);
    let entry = m.entities.entry(E::PATH.to_string()).or_default();
    entry.unique_violations = entry.unique_violations.saturating_add(1);
}

/// Increment existence-check counters globally and for a specific entity type.
pub(crate) fn record_exists_call_for<E>()
where
    E: crate::traits::EntityKind,
{
    with_state_mut(|m| {
        m.ops.exists_calls = m.ops.exists_calls.saturating_add(1);
        let entry = m.entities.entry(E::PATH.to_string()).or_default();
        entry.exists_calls = entry.exists_calls.saturating_add(1);
    });
}

/// Increment row-scan counters globally and for a specific entity type.
pub(crate) fn record_rows_scanned_for<E>(rows_scanned: u64)
where
    E: crate::traits::EntityKind,
{
    with_state_mut(|m| {
        m.ops.rows_scanned = m.ops.rows_scanned.saturating_add(rows_scanned);
        let entry = m.entities.entry(E::PATH.to_string()).or_default();
        entry.rows_scanned = entry.rows_scanned.saturating_add(rows_scanned);
    });
}

///
/// EventSelect
/// Select which parts of the metrics report to include.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct EventSelect {
    pub data: bool,
    pub index: bool,
    pub counters: bool,
    pub entities: bool,
}

impl EventSelect {
    #[must_use]
    pub const fn all() -> Self {
        Self {
            data: true,
            index: true,
            counters: true,
            entities: true,
        }
    }
}

impl Default for EventSelect {
    fn default() -> Self {
        Self::all()
    }
}

/// Build a metrics report by inspecting in-memory counters only.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn report() -> EventReport {
    let snap = with_state(Clone::clone);

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
            exists_calls: ops.exists_calls,
            delete_calls: ops.delete_calls,
            rows_loaded: ops.rows_loaded,
            rows_scanned: ops.rows_scanned,
            rows_deleted: ops.rows_deleted,
            avg_rows_per_load: avg_load,
            avg_rows_scanned_per_load: avg_scanned,
            avg_rows_per_delete: avg_delete,
            index_inserts: ops.index_inserts,
            index_removes: ops.index_removes,
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
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

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

        let report = report();
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
