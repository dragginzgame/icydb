//! Test-only constructors and adapters for the parent module.

use super::*;

impl RequestExecutionRoot {
    #[must_use]
    pub(in crate::db) fn new_for_tests(budget: HardExecutionBudget) -> Self {
        Self::from_budget(budget)
    }

    #[must_use]
    pub(in crate::db) fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.scope.observed(resource)
    }
}

impl RequestExecutionScope {
    pub(super) fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.counters.observed[resource_index(resource)].get()
    }
}
