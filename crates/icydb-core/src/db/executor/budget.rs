//! Module: db::executor::budget
//! Responsibility: finite hard limits and monotonic accounting for database work.
//! Does not own: request-root propagation, physical-operator instrumentation, or paging progress.
//! Boundary: charges one named resource before or during bounded work and returns typed exhaustion.

use crate::error::InternalError;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

const RESOURCE_COUNT: usize = DiagnosticExecutionBudgetResource::ALL.len();

/// Reserved capacity needed to construct and encode a typed budget failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct HardExecutionFailureHeadroom {
    instruction_units: u64,
    response_bytes: u64,
}

impl HardExecutionFailureHeadroom {
    /// Construct one explicit failure-reserve contract.
    #[must_use]
    pub(in crate::db) const fn new(instruction_units: u64, response_bytes: u64) -> Self {
        Self {
            instruction_units,
            response_bytes,
        }
    }

    /// Return the instruction reserve excluded from ordinary charged work.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn instruction_units(self) -> u64 {
        self.instruction_units
    }

    /// Return the response-byte reserve excluded from ordinary result work.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn response_bytes(self) -> u64 {
        self.response_bytes
    }

    const fn is_reserved(self) -> bool {
        self.instruction_units != 0 && self.response_bytes != 0
    }
}

/// Finite per-resource ceilings for one hard execution scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct HardExecutionBudget {
    limits: [u64; RESOURCE_COUNT],
    failure_headroom: HardExecutionFailureHeadroom,
}

impl HardExecutionBudget {
    /// Construct one closed finite budget profile.
    #[must_use]
    pub(in crate::db) const fn new(
        limits: [u64; RESOURCE_COUNT],
        failure_headroom: HardExecutionFailureHeadroom,
    ) -> Self {
        Self {
            limits,
            failure_headroom,
        }
    }

    /// Return the absolute ceiling for one maintained resource.
    #[must_use]
    pub(in crate::db) const fn limit(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.limits[resource_index(resource)]
    }

    /// Return the reserve retained for typed failure construction.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn failure_headroom(&self) -> HardExecutionFailureHeadroom {
        self.failure_headroom
    }

    /// Construct a uniform test-only profile without exposing limit minting publicly.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn uniform_for_tests(
        limit: u64,
        failure_headroom: HardExecutionFailureHeadroom,
    ) -> Self {
        Self::new([limit; RESOURCE_COUNT], failure_headroom)
    }
}

/// Immutable attribution attached to one hard-budget counter set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct HardExecutionContext {
    scope: DiagnosticExecutionBudgetScope,
    lane: DiagnosticExecutionLane,
    normalized_shape_fingerprint_prefix: u64,
}

impl HardExecutionContext {
    /// Construct attribution for one counter owner and literal-free query shape.
    #[must_use]
    pub(in crate::db) const fn new(
        scope: DiagnosticExecutionBudgetScope,
        lane: DiagnosticExecutionLane,
        normalized_shape_fingerprint_prefix: u64,
    ) -> Self {
        Self {
            scope,
            lane,
            normalized_shape_fingerprint_prefix,
        }
    }
}

/// Typed hard-budget exhaustion with no query literals or row payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutionBudgetExceeded {
    resource: DiagnosticExecutionBudgetResource,
    limit: u64,
    observed: u64,
    context: HardExecutionContext,
}

impl ExecutionBudgetExceeded {
    /// Return the resource whose ceiling rejected work.
    #[must_use]
    pub(in crate::db) const fn resource(self) -> DiagnosticExecutionBudgetResource {
        self.resource
    }

    /// Return the configured hard ceiling.
    #[must_use]
    pub(in crate::db) const fn limit(self) -> u64 {
        self.limit
    }

    /// Return the attempted cumulative usage, saturated on arithmetic overflow.
    #[must_use]
    pub(in crate::db) const fn observed(self) -> u64 {
        self.observed
    }

    /// Return the counter owner that rejected work.
    #[must_use]
    pub(in crate::db) const fn scope(self) -> DiagnosticExecutionBudgetScope {
        self.context.scope
    }

    /// Return the execution lane whose work was charged.
    #[must_use]
    pub(in crate::db) const fn lane(self) -> DiagnosticExecutionLane {
        self.context.lane
    }

    /// Return the bounded literal-free query-shape fingerprint prefix.
    #[must_use]
    pub(in crate::db) const fn normalized_shape_fingerprint_prefix(self) -> u64 {
        self.context.normalized_shape_fingerprint_prefix
    }
}

impl From<ExecutionBudgetExceeded> for InternalError {
    fn from(exhausted: ExecutionBudgetExceeded) -> Self {
        Self::execution_budget_exceeded(
            exhausted.resource(),
            exhausted.limit(),
            exhausted.observed(),
            exhausted.scope(),
            exhausted.lane(),
            exhausted.normalized_shape_fingerprint_prefix(),
        )
    }
}

/// Monotonic usage counters for one hard execution budget.
pub(in crate::db) struct HardExecutionBudgetTracker<'budget> {
    budget: &'budget HardExecutionBudget,
    context: HardExecutionContext,
    observed: [u64; RESOURCE_COUNT],
}

impl<'budget> HardExecutionBudgetTracker<'budget> {
    /// Start one counter set at zero usage.
    #[must_use]
    pub(in crate::db) const fn new(
        budget: &'budget HardExecutionBudget,
        context: HardExecutionContext,
    ) -> Self {
        debug_assert!(budget.failure_headroom.is_reserved());
        Self {
            budget,
            context,
            observed: [0; RESOURCE_COUNT],
        }
    }

    /// Charge work whose bounded amount is known before it starts.
    pub(in crate::db) const fn precharge(
        &mut self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        self.charge(resource, amount)
    }

    /// Charge one bounded increment at a maintained loop boundary.
    pub(in crate::db) const fn charge_periodic(
        &mut self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        self.charge(resource, amount)
    }

    /// Return the retained cumulative usage, including rejected work attempts.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.observed[resource_index(resource)]
    }

    /// Return the profile's failure-construction reserve.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn failure_headroom(&self) -> HardExecutionFailureHeadroom {
        self.budget.failure_headroom()
    }

    const fn charge(
        &mut self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        let index = resource_index(resource);
        let current = self.observed[index];
        let (observed, overflowed) = current.overflowing_add(amount);
        let observed = if overflowed { u64::MAX } else { observed };
        self.observed[index] = observed;
        let limit = self.budget.limit(resource);
        if overflowed || observed > limit {
            return Err(ExecutionBudgetExceeded {
                resource,
                limit,
                observed,
                context: self.context,
            });
        }
        Ok(())
    }
}

const fn resource_index(resource: DiagnosticExecutionBudgetResource) -> usize {
    match resource {
        DiagnosticExecutionBudgetResource::QueryExecutions => 0,
        DiagnosticExecutionBudgetResource::PlanningSteps => 1,
        DiagnosticExecutionBudgetResource::PlanCompilations => 2,
        DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited => 3,
        DiagnosticExecutionBudgetResource::RowsVisited => 4,
        DiagnosticExecutionBudgetResource::StoredBytesRead => 5,
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps => 6,
        DiagnosticExecutionBudgetResource::NestedValueSteps => 7,
        DiagnosticExecutionBudgetResource::DecodedBytes => 8,
        DiagnosticExecutionBudgetResource::MaterializedBytes => 9,
        DiagnosticExecutionBudgetResource::SortEntries => 10,
        DiagnosticExecutionBudgetResource::SortComparisons => 11,
        DiagnosticExecutionBudgetResource::SortTemporaryBytes => 12,
        DiagnosticExecutionBudgetResource::GroupDistinctEntries => 13,
        DiagnosticExecutionBudgetResource::GroupDistinctStateBytes => 14,
        DiagnosticExecutionBudgetResource::CursorSteps => 15,
        DiagnosticExecutionBudgetResource::TemporaryBytes => 16,
        DiagnosticExecutionBudgetResource::DiagnosticSteps => 17,
        DiagnosticExecutionBudgetResource::ResultRows => 18,
        DiagnosticExecutionBudgetResource::ResultBytes => 19,
        DiagnosticExecutionBudgetResource::InstructionUnits => 20,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use icydb_diagnostic_code::{DiagnosticDetail, DiagnosticFactTag, RuntimeBoundaryCode};

    const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
    const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::PublicRead,
        0x0102_0304_0506_0708,
    );

    #[test]
    fn every_resource_charges_monotonically_and_retains_rejected_work() {
        let budget = HardExecutionBudget::new([1; RESOURCE_COUNT], TEST_HEADROOM);
        for resource in DiagnosticExecutionBudgetResource::ALL {
            let mut tracker = HardExecutionBudgetTracker::new(&budget, TEST_CONTEXT);
            tracker
                .precharge(resource, 1)
                .expect("work at the hard ceiling should be admitted");
            let exhausted = tracker
                .charge_periodic(resource, 1)
                .expect_err("work above the hard ceiling should reject");

            assert_eq!(exhausted.resource(), resource);
            assert_eq!(exhausted.limit(), 1);
            assert_eq!(exhausted.observed(), 2);
            assert_eq!(tracker.observed(resource), 2);
        }
    }

    #[test]
    fn arithmetic_overflow_is_exhaustion_and_never_refunds_usage() {
        let budget = HardExecutionBudget::new([u64::MAX; RESOURCE_COUNT], TEST_HEADROOM);
        let resource = DiagnosticExecutionBudgetResource::PlanningSteps;
        let mut tracker = HardExecutionBudgetTracker::new(&budget, TEST_CONTEXT);
        tracker
            .precharge(resource, u64::MAX)
            .expect("the representable ceiling should be admitted");
        let exhausted = tracker
            .charge_periodic(resource, 1)
            .expect_err("counter overflow must reject");

        assert_eq!(exhausted.observed(), u64::MAX);
        assert_eq!(tracker.observed(resource), u64::MAX);
    }

    #[test]
    fn exhaustion_maps_to_complete_typed_diagnostic_facts() {
        let budget = HardExecutionBudget::new([0; RESOURCE_COUNT], TEST_HEADROOM);
        let mut tracker = HardExecutionBudgetTracker::new(&budget, TEST_CONTEXT);
        let exhausted = tracker
            .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
            .expect_err("zero query allowance should reject");
        let error = InternalError::from(exhausted);

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::BudgetResource, 1),
                (DiagnosticFactTag::Limit, 0),
                (DiagnosticFactTag::Actual, 1),
                (DiagnosticFactTag::ExecutionBudgetScope, 1),
                (DiagnosticFactTag::ExecutionLane, 1),
                (
                    DiagnosticFactTag::QueryShapeFingerprintPrefix,
                    0x0102_0304_0506_0708,
                ),
            ],
        );
        assert_eq!(tracker.failure_headroom(), TEST_HEADROOM);
        assert_eq!(TEST_HEADROOM.instruction_units(), 500);
        assert_eq!(TEST_HEADROOM.response_bytes(), 256);
    }
}
