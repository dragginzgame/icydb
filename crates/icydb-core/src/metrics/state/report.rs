//! Module: metrics::state::report
//! Responsibility: rich metrics report DTOs and windowed report construction.
//! Does not own: mutable metrics state updates or compact metrics reports.
//! Boundary: keeps rich endpoint payload construction separate from raw state.

use candid::CandidType;
use serde::Deserialize;

use crate::runtime::now_millis;

use super::{EntitySummary, EventOps, EventPerf, entity_summary_from_counters, with_state};

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
    const fn new(
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
// Top-level metrics counters returned by the generated metrics endpoint.
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
    const fn new(ops: EventOps, perf: EventPerf, window_start_ms: u64, window_end_ms: u64) -> Self {
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
pub(in crate::metrics) fn report_window_start(window_start_ms: Option<u64>) -> EventReport {
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
            .then_with(|| b.rows_loaded().cmp(&a.rows_loaded()))
            .then_with(|| b.rows_saved().cmp(&a.rows_saved()))
            .then_with(|| b.rows_scanned().cmp(&a.rows_scanned()))
            .then_with(|| b.rows_deleted().cmp(&a.rows_deleted()))
            .then_with(|| a.path().cmp(b.path()))
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
