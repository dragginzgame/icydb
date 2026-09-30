//! Module: executor::aggregate::contracts::state::distinct
//! Responsibility: prepared grouped DISTINCT execution facts.
//! Does not own: grouped hash table policy or aggregate reducer payloads.
//! Boundary: carries DISTINCT enablement and value-deduplication policy.

///
/// GroupedDistinctExecutionMode
///
/// GroupedDistinctExecutionMode carries the planner-prepared grouped DISTINCT
/// facts into reducer state.
/// It prevents reducer execution from reinterpreting aggregate kind while still
/// keeping key-based and value-based DISTINCT admission explicit.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedDistinctExecutionMode {
    enabled: bool,
    uses_value_dedup: bool,
}

impl GroupedDistinctExecutionMode {
    /// Build one prepared grouped DISTINCT execution mode.
    #[must_use]
    pub(in crate::db::executor) const fn new(enabled: bool, uses_value_dedup: bool) -> Self {
        Self {
            enabled,
            uses_value_dedup,
        }
    }

    /// Return whether grouped DISTINCT admission is enabled.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) const fn enabled(self) -> bool {
        self.enabled
    }

    /// Return whether grouped DISTINCT admission deduplicates by input value.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) const fn uses_value_dedup(
        self,
    ) -> bool {
        self.uses_value_dedup
    }
}
