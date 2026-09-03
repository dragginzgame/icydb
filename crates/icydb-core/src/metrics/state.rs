//! Module: metrics::state
//! Responsibility: bounded on-canister entity execution counters and reporting.
//! Does not own: endpoint attribution, query identity, or persisted metrics.
//! Boundary: one heap-only accumulator keyed by accepted entity path.

use crate::runtime::now_millis;
use candid::CandidType;
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap};

#[derive(Clone, Debug, Default)]
struct EntityCounter {
    hits: u64,
    instructions_total: u64,
    instructions_max: u64,
}

#[derive(Clone, Debug)]
struct MetricsState {
    entities: BTreeMap<String, EntityCounter>,
    window_start_ms: u64,
}

impl Default for MetricsState {
    fn default() -> Self {
        Self {
            entities: BTreeMap::new(),
            window_start_ms: now_millis(),
        }
    }
}

thread_local! {
    static STATE: RefCell<MetricsState> = RefCell::new(MetricsState::default());
}

/// Cost attributed to one accepted entity during the active metrics window.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct EntityMetrics {
    path: String,
    hits: u64,
    instructions_total: u64,
    instructions_max: u64,
}

impl EntityMetrics {
    /// Accepted entity path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Number of observed entity execution spans.
    #[must_use]
    pub const fn hits(&self) -> u64 {
        self.hits
    }

    /// Saturating sum of local instructions attributed to the entity.
    #[must_use]
    pub const fn instructions_total(&self) -> u64 {
        self.instructions_total
    }

    /// Largest local instruction delta attributed to one entity execution.
    #[must_use]
    pub const fn instructions_max(&self) -> u64 {
        self.instructions_max
    }
}

/// Heap-only metrics window sorted by descending attributed instruction cost.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct MetricsReport {
    window_start_ms: u64,
    window_end_ms: u64,
    entities: Vec<EntityMetrics>,
}

impl MetricsReport {
    /// Millisecond timestamp at which this metrics window began.
    #[must_use]
    pub const fn window_start_ms(&self) -> u64 {
        self.window_start_ms
    }

    /// Millisecond timestamp at which this report was read.
    #[must_use]
    pub const fn window_end_ms(&self) -> u64 {
        self.window_end_ms
    }

    /// Per-entity observations, ordered by total cost, hits, then path.
    #[must_use]
    pub const fn entities(&self) -> &[EntityMetrics] {
        self.entities.as_slice()
    }
}

pub(super) fn record_entity_execution(entity_path: &str, instructions: u64) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let counter = state.entities.entry(entity_path.to_string()).or_default();
        counter.hits = counter.hits.saturating_add(1);
        counter.instructions_total = counter.instructions_total.saturating_add(instructions);
        counter.instructions_max = counter.instructions_max.max(instructions);
    });
}

/// Snapshot the current on-canister metrics window.
#[must_use]
pub fn metrics_report() -> MetricsReport {
    STATE.with(|state| {
        let state = state.borrow();
        let mut entities = state
            .entities
            .iter()
            .map(|(path, counter)| EntityMetrics {
                path: path.clone(),
                hits: counter.hits,
                instructions_total: counter.instructions_total,
                instructions_max: counter.instructions_max,
            })
            .collect::<Vec<_>>();
        entities.sort_by(|left, right| {
            right
                .instructions_total
                .cmp(&left.instructions_total)
                .then_with(|| right.hits.cmp(&left.hits))
                .then_with(|| left.path.cmp(&right.path))
        });

        MetricsReport {
            window_start_ms: state.window_start_ms,
            window_end_ms: now_millis(),
            entities,
        }
    })
}

/// Reset the heap-only metrics window.
pub fn metrics_reset_all() {
    STATE.with(|state| *state.borrow_mut() = MetricsState::default());
}

#[cfg(test)]
mod tests {
    use super::{metrics_report, metrics_reset_all, record_entity_execution};

    #[test]
    fn report_orders_entities_by_total_cost_then_hits_then_path() {
        metrics_reset_all();
        record_entity_execution("store::beta", 10);
        record_entity_execution("store::alpha", 5);
        record_entity_execution("store::alpha", 5);
        record_entity_execution("store::gamma", 10);

        let report = metrics_report();
        let paths = report
            .entities()
            .iter()
            .map(super::EntityMetrics::path)
            .collect::<Vec<_>>();

        assert_eq!(paths, vec!["store::alpha", "store::beta", "store::gamma"]);
        assert_eq!(report.entities()[0].hits(), 2);
        assert_eq!(report.entities()[0].instructions_total(), 10);
        assert_eq!(report.entities()[0].instructions_max(), 5);
    }
}
