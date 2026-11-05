//! Test-only constructors and adapters for the parent module.

use super::*;

impl HardExecutionFailureHeadroom {
    /// Return the instruction reserve excluded from ordinary charged work.
    #[must_use]
    pub(in crate::db) const fn instruction_units(self) -> u64 {
        self.instruction_units
    }

    /// Return the response-byte reserve excluded from ordinary result work.
    #[must_use]
    pub(in crate::db) const fn response_bytes(self) -> u64 {
        self.response_bytes
    }
}

impl HardExecutionBudget {
    /// Return the reserve retained for typed failure construction.
    #[must_use]
    pub(in crate::db) const fn failure_headroom(&self) -> HardExecutionFailureHeadroom {
        self.failure_headroom
    }

    /// Construct a uniform test-only profile without exposing limit minting publicly.
    #[must_use]
    pub(in crate::db) const fn uniform_for_tests(
        limit: u64,
        failure_headroom: HardExecutionFailureHeadroom,
    ) -> Self {
        Self::new([limit; RESOURCE_COUNT], failure_headroom)
    }

    /// Replace one resource ceiling in a test-only injected profile.
    #[must_use]
    pub(in crate::db) const fn with_limit_for_tests(
        mut self,
        resource: DiagnosticExecutionBudgetResource,
        limit: u64,
    ) -> Self {
        self.limits[resource_index(resource)] = limit;
        self
    }
}

impl HardExecutionBudgetTracker {
    #[must_use]
    pub(in crate::db) fn new_for_tests(
        budget: HardExecutionBudget,
        context: HardExecutionContext,
    ) -> Self {
        debug_assert!(budget.failure_headroom.is_reserved());
        Self {
            budget: HardExecutionBudgetAuthority::TestOwned(Box::new(budget)),
            context,
            request_scope: None,
            observed: [0; RESOURCE_COUNT],
            last_instruction_counter: None,
            charges_since_instruction_watermark: 0,
        }
    }

    /// Return the retained cumulative usage, including rejected work attempts.
    #[must_use]
    pub(in crate::db) const fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.observed[resource_index(resource)]
    }

    /// Return the profile's failure-construction reserve.
    #[must_use]
    pub(in crate::db) const fn failure_headroom(&self) -> HardExecutionFailureHeadroom {
        self.budget.budget().failure_headroom()
    }
}

impl MaintenanceConstructionBudget {
    pub(in crate::db) fn with_limit_for_tests(
        resource: DiagnosticExecutionBudgetResource,
        limit: u64,
    ) -> Self {
        Self {
            exhausted: Cell::new(None),
            tracker: RefCell::new(HardExecutionBudgetTracker::new_for_tests(
                MUTATION_HARD_BUDGET.with_limit_for_tests(resource, limit),
                HardExecutionContext::new(
                    DiagnosticExecutionBudgetScope::Execution,
                    DiagnosticExecutionLane::Mutation,
                    0,
                ),
            )),
        }
    }

    /// Inspect cumulative admission for exact-boundary maintenance controls.
    pub(in crate::db) fn observed_for_tests(
        &self,
        resource: DiagnosticExecutionBudgetResource,
    ) -> u64 {
        self.tracker.borrow().observed(resource)
    }
}

pub(in crate::db) const fn read_hard_budget_limit_for_tests(
    resource: DiagnosticExecutionBudgetResource,
) -> u64 {
    READ_HARD_BUDGET.limit(resource)
}

pub(in crate::db) fn with_query_execution_budget_for_tests<T>(
    budget: HardExecutionBudget,
    context: HardExecutionContext,
    run: impl FnOnce() -> Result<T, QueryError>,
) -> Result<T, QueryError> {
    with_execution_budget(
        HardExecutionBudgetTracker::new_for_tests(budget, context),
        run,
        QueryError::execute,
        ExecutionBudgetFinish::Automatic,
    )
}

pub(in crate::db) fn with_execution_budget_for_tests<T, E>(
    budget: HardExecutionBudget,
    context: HardExecutionContext,
    run: impl FnOnce() -> Result<T, E>,
    map_internal: fn(InternalError) -> E,
) -> Result<T, E> {
    with_execution_budget(
        HardExecutionBudgetTracker::new_for_tests(budget, context),
        run,
        map_internal,
        ExecutionBudgetFinish::Automatic,
    )
}
