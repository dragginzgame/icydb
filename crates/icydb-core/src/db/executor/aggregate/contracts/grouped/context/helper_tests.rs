//! Test-only constructors and adapters for the parent module.

use super::*;

impl ExecutionBudget {
    /// Return the peak number of simultaneously live canonical groups.
    #[must_use]
    pub(in crate::db::executor) const fn peak_live_groups(&self) -> u64 {
        self.peak_live_groups
    }

    /// Return the peak number of simultaneously live aggregate state slots.
    #[must_use]
    pub(in crate::db::executor) const fn peak_live_aggregate_states(&self) -> u64 {
        self.peak_live_aggregate_states
    }

    /// Return the peak number of simultaneously live grouped DISTINCT values.
    #[must_use]
    pub(in crate::db::executor) const fn peak_live_distinct_values(&self) -> u64 {
        self.peak_live_distinct_values
    }
}

impl ExecutionConfig {
    /// Build one unbounded grouped configuration for tests.
    #[must_use]
    pub(in crate::db::executor) const fn unbounded() -> Self {
        Self::with_hard_limits(u64::MAX, u64::MAX)
    }
}

impl ExecutionContext {
    /// Freeze executor-owned grouped work and live-state facts after a successful fold.
    #[must_use]
    pub(in crate::db::executor) const fn successful_runtime_stats(
        &self,
        early_scan_stop: bool,
    ) -> GroupedRuntimeStats {
        GroupedRuntimeStats {
            groups_observed: self.budget.groups(),
            groups_finalized: self.budget.groups(),
            peak_live_groups: self.budget.peak_live_groups(),
            peak_live_aggregate_states: self.budget.peak_live_aggregate_states(),
            peak_live_distinct_values: self.budget.peak_live_distinct_values(),
            peak_estimated_state_bytes: self.budget.peak_estimated_bytes(),
            early_scan_stop,
        }
    }
}

///
/// GroupedRuntimeStats
///
/// GroupedRuntimeStats is the immutable grouped work/live-state snapshot
/// emitted only after one grouped fold completes successfully.
/// It carries executor-owned facts into diagnostics without reconstructing
/// runtime behavior from the selected route or returned page.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedRuntimeStats {
    pub(super) groups_observed: u64,
    pub(super) groups_finalized: u64,
    pub(super) peak_live_groups: u64,
    pub(super) peak_live_aggregate_states: u64,
    pub(super) peak_live_distinct_values: u64,
    pub(super) peak_estimated_state_bytes: u64,
    pub(super) early_scan_stop: bool,
}

impl GroupedRuntimeStats {
    /// Return the number of canonical groups observed by successful fold execution.
    #[must_use]
    pub(in crate::db::executor) const fn groups_observed(&self) -> u64 {
        self.groups_observed
    }

    /// Return the number of canonical groups finalized by successful fold execution.
    #[must_use]
    pub(in crate::db::executor) const fn groups_finalized(&self) -> u64 {
        self.groups_finalized
    }

    /// Return the peak number of simultaneously live canonical groups.
    #[must_use]
    pub(in crate::db::executor) const fn peak_live_groups(&self) -> u64 {
        self.peak_live_groups
    }

    /// Return the peak number of simultaneously live aggregate state slots.
    #[must_use]
    pub(in crate::db::executor) const fn peak_live_aggregate_states(&self) -> u64 {
        self.peak_live_aggregate_states
    }

    /// Return the peak number of simultaneously live grouped DISTINCT values.
    #[must_use]
    pub(in crate::db::executor) const fn peak_live_distinct_values(&self) -> u64 {
        self.peak_live_distinct_values
    }

    /// Return the peak conservative grouped state estimate, including owned group-key backing.
    #[must_use]
    pub(in crate::db::executor) const fn peak_estimated_state_bytes(&self) -> u64 {
        self.peak_estimated_state_bytes
    }

    /// Return whether bounded ordered page selection stopped the source scan early.
    #[must_use]
    pub(in crate::db::executor) const fn early_scan_stop(&self) -> bool {
        self.early_scan_stop
    }
}
