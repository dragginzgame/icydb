//! Module: db::executor::diagnostics::counters
//! Responsibility: node-correlated row-flow counter contracts for executor diagnostics.
//! Does not own: metrics sink emission or plan-selection behavior.
//! Boundary: additive diagnostics counters for explain/runtime correlation.

#![cfg_attr(not(test), allow(dead_code))]

use crate::db::diagnostics::ExecutionTrace;

///
/// ExecutionNodeCounters
///
/// Additive row-flow counters correlated to one execution diagnostics node.
/// Counters are observability-only and must not affect runtime semantics.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ExecutionNodeCounters {
    scanned: u64,
    filtered: u64,
    aggregated: u64,
    emitted: u64,
}

impl ExecutionNodeCounters {
    /// Construct one explicit row-flow counter snapshot.
    #[must_use]
    pub(crate) const fn new(
        rows_scanned: u64,
        rows_filtered: u64,
        rows_aggregated: u64,
        rows_emitted: u64,
    ) -> Self {
        Self {
            scanned: rows_scanned,
            filtered: rows_filtered,
            aggregated: rows_aggregated,
            emitted: rows_emitted,
        }
    }

    /// Project one baseline diagnostics counter snapshot from execution trace output.
    ///
    /// The trace currently exposes scanned/materialized/returned dimensions.
    /// `rows_filtered` and `rows_aggregated` remain zero unless explicitly provided by
    /// call-site row-flow accounting.
    #[must_use]
    pub(crate) const fn from_execution_trace(trace: &ExecutionTrace) -> Self {
        Self {
            scanned: trace.keys_scanned(),
            filtered: 0,
            aggregated: 0,
            emitted: trace.rows_returned(),
        }
    }

    /// Saturating-add one counter snapshot into this snapshot.
    pub(crate) const fn saturating_add_assign(&mut self, rhs: Self) {
        self.scanned = self.scanned.saturating_add(rhs.scanned);
        self.filtered = self.filtered.saturating_add(rhs.filtered);
        self.aggregated = self.aggregated.saturating_add(rhs.aggregated);
        self.emitted = self.emitted.saturating_add(rhs.emitted);
    }

    /// Return rows scanned.
    #[must_use]
    pub(crate) const fn rows_scanned(self) -> u64 {
        self.scanned
    }

    /// Return rows filtered.
    #[must_use]
    pub(crate) const fn rows_filtered(self) -> u64 {
        self.filtered
    }

    /// Return rows aggregated.
    #[must_use]
    pub(crate) const fn rows_aggregated(self) -> u64 {
        self.aggregated
    }

    /// Return rows emitted.
    #[must_use]
    pub(crate) const fn rows_emitted(self) -> u64 {
        self.emitted
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::ExecutionNodeCounters;

    use crate::db::{access::AccessPlan, diagnostics::ExecutionTrace, direction::Direction};

    #[test]
    fn execution_node_counters_from_trace_projects_rows_scanned_and_rows_emitted() {
        let access = AccessPlan::by_key(99u64);
        let mut trace = ExecutionTrace::new(&access, Direction::Asc, false);
        trace.set_path_outcome(None, 12, 5, 4, 7, true, false, 0, 0);

        let counters = ExecutionNodeCounters::from_execution_trace(&trace);
        assert_eq!(counters.rows_scanned(), 12);
        assert_eq!(counters.rows_filtered(), 0);
        assert_eq!(counters.rows_aggregated(), 0);
        assert_eq!(counters.rows_emitted(), 4);
    }

    #[test]
    fn execution_node_counters_saturating_add_assign_is_stable() {
        let mut counters = ExecutionNodeCounters::new(1, 2, 3, 4);
        counters.saturating_add_assign(ExecutionNodeCounters::new(5, 6, 7, 8));

        assert_eq!(
            counters,
            ExecutionNodeCounters::new(6, 8, 10, 12),
            "row-flow counters must combine additively",
        );
    }
}
