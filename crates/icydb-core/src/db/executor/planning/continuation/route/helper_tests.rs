//! Test-only constructors and adapters for the parent module.

use super::*;

impl RouteContinuationPlan {
    /// Construct one canonical initial continuation plan for mutation-style routes.
    #[must_use]
    pub(super) const fn initial_with_policy(continuation_policy: ContinuationPolicy) -> Self {
        Self::new(
            ContinuationMode::Initial,
            continuation_policy,
            AccessWindow::new(0, None, None),
            AccessWindow::new(0, None, None),
        )
    }

    /// Construct one canonical initial continuation plan for test route fixtures.
    #[must_use]
    pub(in crate::db::executor) const fn initial_for_test() -> Self {
        Self::initial_with_policy(ContinuationPolicy::new(true, true, true))
    }
}
