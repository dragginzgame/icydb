//! Test-only constructors and adapters for the parent module.

use super::*;

impl DynamicQuery {
    /// Enable projection DISTINCT for maintained internal execution callers.
    ///
    /// The public dynamic-query grammar deliberately does not expose this
    /// builder; SQL and internal executor contracts remain the DISTINCT
    /// frontends until a separately reviewed public API is designed.
    #[must_use]
    pub(in crate::db) const fn distinct_for_internal_execution(mut self) -> Self {
        self.distinct = true;
        self
    }

    pub(in crate::db) const fn projection_is_distinct(&self) -> bool {
        self.distinct
    }
}
