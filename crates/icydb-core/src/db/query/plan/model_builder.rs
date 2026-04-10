//! Module: query::plan::model_builder
//! Responsibility: pure logical plan-model constructors/builders.
//! Does not own: access-plan coupling or semantic interpretation.
//! Boundary: model-only helpers that remain independent of access planning.

use crate::db::query::plan::{DeleteSpec, FieldSlot, GroupedExecutionConfig, LoadSpec};

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
        Self { limit: None }
    }
}

impl FieldSlot {
    /// Build one field slot directly for tests that need invalid slot shapes.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_parts_for_test(index: usize, field: impl Into<String>) -> Self {
        Self {
            index,
            field: field.into(),
            kind: None,
        }
    }
}

impl GroupedExecutionConfig {
    /// Build one grouped execution config with explicit hard limits.
    #[must_use]
    pub(crate) const fn with_hard_limits(max_groups: u64, max_group_bytes: u64) -> Self {
        Self {
            max_groups,
            max_group_bytes,
        }
    }

    /// Build one unbounded grouped execution config.
    #[must_use]
    pub(crate) const fn unbounded() -> Self {
        Self::with_hard_limits(u64::MAX, u64::MAX)
    }

    /// Return grouped hard limit for maximum groups.
    #[must_use]
    pub(crate) const fn max_groups(&self) -> u64 {
        self.max_groups
    }

    /// Return grouped hard limit for estimated grouped bytes.
    #[must_use]
    pub(crate) const fn max_group_bytes(&self) -> u64 {
        self.max_group_bytes
    }
}

impl Default for GroupedExecutionConfig {
    fn default() -> Self {
        Self::unbounded()
    }
}
