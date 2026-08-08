//! Module: query::plan::model_builder
//! Responsibility: pure logical plan-model constructors/builders.
//! Does not own: access-plan coupling or semantic interpretation.
//! Boundary: logical plan builders that remain independent of access planning.

use crate::db::query::plan::{DeleteSpec, FieldSlot, GroupedExecutionConfig, LoadSpec};

const PLANNER_DEFAULT_MAX_GROUPS: u64 = 10_000;
const PLANNER_DEFAULT_MAX_GROUP_BYTES: u64 = 16 * 1024 * 1024;

impl LoadSpec {
    /// Create an empty load spec.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            limit: None,
            offset: 0,
        }
    }
}

impl DeleteSpec {
    /// Create an empty delete spec.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            limit: None,
            offset: 0,
        }
    }
}

impl FieldSlot {
    /// Build one field slot directly for tests that need custom slot shapes.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn from_test_slot(index: usize, field: impl Into<String>) -> Self {
        Self::unresolved(index, field)
    }
}

impl GroupedExecutionConfig {
    /// Build the planner-owned conservative grouped execution ceiling.
    ///
    /// SQL grouping has no syntax for supplying executor hard limits. Its
    /// lowering boundary uses this finite authority so complete hash builds
    /// can execute without turning an omitted row `LIMIT` into unbounded
    /// retained state.
    #[must_use]
    pub(in crate::db) const fn planner_default_bounded() -> Self {
        Self::with_hard_limits(PLANNER_DEFAULT_MAX_GROUPS, PLANNER_DEFAULT_MAX_GROUP_BYTES)
    }

    /// Build one grouped execution config with explicit hard limits.
    #[must_use]
    pub(in crate::db) const fn with_hard_limits(max_groups: u64, max_group_bytes: u64) -> Self {
        Self {
            max_groups,
            max_group_bytes,
        }
    }

    /// Build one unbounded grouped execution config.
    #[must_use]
    pub(in crate::db) const fn unbounded() -> Self {
        Self::with_hard_limits(u64::MAX, u64::MAX)
    }

    /// Return grouped hard limit for maximum groups.
    #[must_use]
    pub(in crate::db) const fn max_groups(&self) -> u64 {
        self.max_groups
    }

    /// Return grouped hard limit for estimated grouped bytes.
    #[must_use]
    pub(in crate::db) const fn max_group_bytes(&self) -> u64 {
        self.max_group_bytes
    }

    /// Return whether both grouped hard limits are finite and non-zero.
    #[must_use]
    pub(in crate::db) const fn is_finite_bounded(&self) -> bool {
        self.max_groups > 0
            && self.max_groups < u64::MAX
            && self.max_group_bytes > 0
            && self.max_group_bytes < u64::MAX
    }
}

impl Default for GroupedExecutionConfig {
    fn default() -> Self {
        Self::unbounded()
    }
}
