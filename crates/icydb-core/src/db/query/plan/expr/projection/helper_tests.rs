//! Test-only constructors and adapters for the parent module.

use super::*;

impl ProjectionSpec {
    /// Build one projection semantic contract for tests outside planner modules.
    #[must_use]
    pub(in crate::db) const fn from_fields_for_test(fields: Vec<ProjectionField>) -> Self {
        Self::new(fields)
    }
}
