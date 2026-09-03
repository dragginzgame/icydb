//! Module: db::session::request
//! Responsibility: one monotonic aggregate execution scope per request entry.
//! Does not own: caller authorization, per-execution limits, or physical charging sites.
//! Boundary: request roots issue shared scope handles that every derived session retains.

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use crate::db::executor::budget::{
    ExecutionBudgetExceeded, HardExecutionBudget, HardExecutionContext,
    HardExecutionFailureHeadroom, resource_index,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope};

const REQUEST_FAILURE_HEADROOM: HardExecutionFailureHeadroom =
    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1_024);
const REQUEST_HARD_BUDGET: HardExecutionBudget = HardExecutionBudget::new(
    [
        256,                 // query executions
        1_024,               // planning operations
        256,                 // plan compilations
        250_000,             // key/index entries visited
        250_000,             // rows visited
        128 * 1_024 * 1_024, // stored bytes read
        16_000_000,          // predicate/expression steps
        16_000_000,          // nested value steps
        128 * 1_024 * 1_024, // decoded bytes
        128 * 1_024 * 1_024, // materialized bytes
        250_000,             // sort entries
        32_000_000,          // sort comparisons
        128 * 1_024 * 1_024, // sort temporary bytes
        100_000,             // group/distinct entries
        128 * 1_024 * 1_024, // group/distinct state bytes
        1_000_000,           // cursor steps
        128 * 1_024 * 1_024, // temporary bytes
        100_000,             // result rows
        64 * 1_024 * 1_024,  // result bytes
        4_500_000_000,       // instruction units
    ],
    REQUEST_FAILURE_HEADROOM,
);

thread_local! {
    static CURRENT_REQUEST_SCOPE: RefCell<Option<RequestExecutionScope>> =
        const { RefCell::new(None) };
}

/// Non-cloneable capability owning one request's aggregate database counters.
///
/// Construct this once at request entry and derive every database session used
/// by the request from it. Sessions retain the counters, so dropping this
/// value does not reset work already attached to a derived session.
pub struct RequestExecutionRoot {
    scope: RequestExecutionScope,
}

impl RequestExecutionRoot {
    /// Mint the fixed production request profile.
    ///
    /// This constructor is runtime wiring for generated and guarded facade
    /// request entry. It is intentionally not a budget-policy configuration
    /// surface.
    #[doc(hidden)]
    #[must_use]
    pub fn __new_runtime_root() -> Self {
        Self::from_budget(REQUEST_HARD_BUDGET)
    }

    /// Reuse the active synchronous request scope or mint the production root.
    ///
    /// This is runtime wiring for the public scoped-entry helper. Re-entering
    /// that helper inside one active database segment must retain the existing
    /// counters instead of creating a budget-reset escape hatch.
    #[doc(hidden)]
    #[must_use]
    pub fn __new_or_current_runtime_root() -> Self {
        current_request_scope().map_or_else(Self::__new_runtime_root, |scope| Self { scope })
    }

    /// Make this root current only while one synchronous call tree executes.
    ///
    /// The previous scope is restored before this method returns, including
    /// during host unwinding. The scope is never retained ambiently across an
    /// async suspension point.
    #[doc(hidden)]
    pub fn __with_current_scope<T>(&self, run: impl FnOnce() -> T) -> T {
        let _guard = CurrentRequestScopeGuard::enter(self.scope());
        run()
    }

    /// Whether no root is active or this root owns the active counters.
    ///
    /// Generated facade wiring uses this before accepting an explicit root.
    /// A different active root would reset aggregate accounting inside a
    /// request and must fail closed.
    #[doc(hidden)]
    #[must_use]
    pub fn __is_compatible_with_current(&self) -> bool {
        match current_request_scope() {
            Some(current) => current.same_counters(&self.scope),
            None => true,
        }
    }

    /// Whether this root owns the counters currently installed for this poll.
    #[doc(hidden)]
    #[must_use]
    pub fn __is_current(&self) -> bool {
        current_request_scope().is_some_and(|current| current.same_counters(&self.scope))
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn new_for_tests(budget: HardExecutionBudget) -> Self {
        Self::from_budget(budget)
    }

    fn from_budget(budget: HardExecutionBudget) -> Self {
        Self {
            scope: RequestExecutionScope {
                counters: Rc::new(RequestExecutionCounters {
                    budget,
                    observed: [const { Cell::new(0) };
                        DiagnosticExecutionBudgetResource::ALL.len()],
                }),
            },
        }
    }

    pub(in crate::db) fn scope(&self) -> RequestExecutionScope {
        self.scope.clone()
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.scope.observed(resource)
    }
}

pub(in crate::db) fn current_request_scope() -> Option<RequestExecutionScope> {
    CURRENT_REQUEST_SCOPE.with(|current| current.borrow().clone())
}

struct CurrentRequestScopeGuard {
    previous: Option<RequestExecutionScope>,
}

impl CurrentRequestScopeGuard {
    fn enter(scope: RequestExecutionScope) -> Self {
        let previous = CURRENT_REQUEST_SCOPE.with(|current| current.replace(Some(scope)));
        Self { previous }
    }
}

impl Drop for CurrentRequestScopeGuard {
    fn drop(&mut self) {
        CURRENT_REQUEST_SCOPE.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

/// Shared internal handle retained by every session derived from one root.
#[derive(Clone)]
pub(in crate::db) struct RequestExecutionScope {
    counters: Rc<RequestExecutionCounters>,
}

impl RequestExecutionScope {
    fn same_counters(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.counters, &other.counters)
    }

    pub(in crate::db) fn charge(
        &self,
        context: HardExecutionContext,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        self.counters.charge(context, resource, amount)
    }

    /// Return how many complete equal-cost units remain across every named
    /// request resource without mutating request accounting.
    pub(in crate::db) fn remaining_budget_units(
        &self,
        per_unit: &[(DiagnosticExecutionBudgetResource, u64)],
    ) -> u64 {
        per_unit
            .iter()
            .filter(|(_resource, amount)| *amount != 0)
            .map(|(resource, amount)| {
                let index = resource_index(*resource);
                self.counters
                    .budget
                    .limit(*resource)
                    .saturating_sub(self.counters.observed[index].get())
                    / amount
            })
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Preflight one fixed resource bundle without retaining a rejected or
    /// speculative charge in the request scope.
    pub(in crate::db) fn can_charge_budget_bundle(
        &self,
        charges: &[(DiagnosticExecutionBudgetResource, u64)],
    ) -> bool {
        charges.iter().all(|(resource, amount)| {
            let index = resource_index(*resource);
            self.counters.observed[index]
                .get()
                .checked_add(*amount)
                .is_some_and(|observed| observed <= self.counters.budget.limit(*resource))
        })
    }

    /// Commit one complete bundle only when every request resource fits.
    /// `false` leaves every request counter unchanged.
    pub(in crate::db) fn try_commit_budget_bundle(
        &self,
        charges: &[(DiagnosticExecutionBudgetResource, u64)],
    ) -> bool {
        if !self.can_charge_budget_bundle(charges) {
            return false;
        }
        for (resource, amount) in charges {
            let counter = &self.counters.observed[resource_index(*resource)];
            counter.set(counter.get().saturating_add(*amount));
        }

        true
    }

    #[cfg(test)]
    fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.counters.observed[resource_index(resource)].get()
    }
}

struct RequestExecutionCounters {
    budget: HardExecutionBudget,
    observed: [Cell<u64>; DiagnosticExecutionBudgetResource::ALL.len()],
}

impl RequestExecutionCounters {
    fn charge(
        &self,
        context: HardExecutionContext,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        let index = resource_index(resource);
        let counter = &self.observed[index];
        let current = counter.get();
        let (observed, overflowed) = current.overflowing_add(amount);
        let observed = if overflowed { u64::MAX } else { observed };
        counter.set(observed);
        let limit = self.budget.limit(resource);
        if overflowed || observed > limit {
            return Err(ExecutionBudgetExceeded::new(
                resource,
                limit,
                observed,
                context.with_scope(DiagnosticExecutionBudgetScope::Request),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synchronous_scope_is_installed_then_removed() {
        assert!(current_request_scope().is_none());
        let root = RequestExecutionRoot::new_for_tests(REQUEST_HARD_BUDGET);

        root.__with_current_scope(|| {
            assert!(current_request_scope().is_some());
        });

        assert!(current_request_scope().is_none());
    }

    #[test]
    fn nested_entry_reuses_current_counters() {
        let resource = DiagnosticExecutionBudgetResource::QueryExecutions;
        let budget = REQUEST_HARD_BUDGET.with_limit_for_tests(resource, 1);
        let root = RequestExecutionRoot::new_for_tests(budget);
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
            0,
        );

        root.__with_current_scope(|| {
            let nested = RequestExecutionRoot::__new_or_current_runtime_root();
            nested
                .scope()
                .charge(context, resource, 1)
                .expect("first nested charge should fit");
            let exhausted = root
                .scope()
                .charge(context, resource, 1)
                .expect_err("parent should observe the nested charge");

            assert_eq!(exhausted.scope(), DiagnosticExecutionBudgetScope::Request);
            assert_eq!(exhausted.observed(), 2);
        });
    }

    #[test]
    fn budget_bundle_preflight_is_non_mutating_and_uses_remaining_capacity() {
        let entries = DiagnosticExecutionBudgetResource::GroupDistinctEntries;
        let bytes = DiagnosticExecutionBudgetResource::GroupDistinctStateBytes;
        let budget = REQUEST_HARD_BUDGET
            .with_limit_for_tests(entries, 5)
            .with_limit_for_tests(bytes, 60);
        let root = RequestExecutionRoot::new_for_tests(budget);
        let scope = root.scope();
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
            0,
        );
        scope
            .charge(context, entries, 2)
            .expect("initial request charge should fit");
        scope
            .charge(context, bytes, 20)
            .expect("initial request charge should fit");

        assert_eq!(
            scope.remaining_budget_units(&[(entries, 1), (bytes, 10)]),
            3
        );
        assert!(scope.can_charge_budget_bundle(&[(entries, 3), (bytes, 30)]));
        assert!(!scope.can_charge_budget_bundle(&[(entries, 4), (bytes, 30)]));
        assert_eq!(
            root.observed(entries),
            2,
            "preflight must not charge entries"
        );
        assert_eq!(root.observed(bytes), 20, "preflight must not charge bytes");
        assert!(!scope.try_commit_budget_bundle(&[(entries, 4), (bytes, 30)]));
        assert_eq!(root.observed(entries), 2);
        assert_eq!(root.observed(bytes), 20);
        assert!(scope.try_commit_budget_bundle(&[(entries, 3), (bytes, 30)]));
        assert_eq!(root.observed(entries), 5);
        assert_eq!(root.observed(bytes), 50);
    }

    #[test]
    fn explicit_root_compatibility_rejects_a_different_active_root() {
        let first = RequestExecutionRoot::new_for_tests(REQUEST_HARD_BUDGET);
        let second = RequestExecutionRoot::new_for_tests(REQUEST_HARD_BUDGET);

        assert!(first.__is_compatible_with_current());
        assert!(!first.__is_current());
        first.__with_current_scope(|| {
            assert!(first.__is_current());
            assert!(first.__is_compatible_with_current());
            assert!(!second.__is_compatible_with_current());
        });
        assert!(second.__is_compatible_with_current());
    }
}
