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
#[cfg(feature = "diagnostics")]
use crate::db::{
    diagnostics::{
        RequestDiagnosticResourceUsage, RequestDiagnostics, RequestDiagnosticsState,
        RequestQueryPlanEvidence,
    },
    session::query::QueryPlanCacheAttribution,
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
        1_000_000,           // diagnostic steps
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
                    #[cfg(feature = "diagnostics")]
                    diagnostics: RefCell::new(None),
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

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn enable_diagnostics(&self) -> bool {
        let mut diagnostics = self.counters.diagnostics.borrow_mut();
        if diagnostics.is_some() {
            return false;
        }
        *diagnostics = Some(RequestDiagnosticsState::default());
        true
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn diagnostics_enabled(&self) -> bool {
        self.counters.diagnostics.borrow().is_some()
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn diagnostics_snapshot(&self) -> Option<RequestDiagnostics> {
        let mut snapshot = self
            .counters
            .diagnostics
            .borrow()
            .as_ref()
            .map(RequestDiagnosticsState::snapshot)?;
        let response_bytes = request_diagnostics_bytes_estimate(&snapshot);
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Request,
            icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
            0,
        );
        let charged = self.counters.charge_fail_soft(
            context,
            DiagnosticExecutionBudgetResource::DiagnosticSteps,
            1,
        ) && self.counters.charge_fail_soft(
            context,
            DiagnosticExecutionBudgetResource::ResultBytes,
            response_bytes,
        );
        if !charged {
            self.suppress_diagnostics(1);
            snapshot.shapes.clear();
            snapshot.warnings.clear();
            snapshot.suppressed_observations = snapshot.suppressed_observations.saturating_add(1);
        }
        Some(snapshot)
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn record_query_plan(
        &self,
        evidence: RequestQueryPlanEvidence,
        cache: QueryPlanCacheAttribution,
    ) {
        if !self.diagnostics_enabled() {
            return;
        }
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Request,
            icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
            evidence.normalized_shape_fingerprint_prefix,
        );
        let retained_bytes = evidence.retained_bytes_estimate();
        let diagnostic_steps = evidence.work_steps_estimate();
        if !self.counters.charge_fail_soft(
            context,
            DiagnosticExecutionBudgetResource::DiagnosticSteps,
            diagnostic_steps,
        ) || !self.counters.charge_fail_soft(
            context,
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            retained_bytes,
        ) {
            self.suppress_diagnostics(1);
            return;
        }
        if let Some(diagnostics) = self.counters.diagnostics.borrow_mut().as_mut() {
            diagnostics.observe_plan(evidence, cache.hits, cache.misses);
        }
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn record_execution(
        &self,
        context: HardExecutionContext,
        usage: RequestDiagnosticResourceUsage,
    ) {
        if !self.diagnostics_enabled() {
            return;
        }
        if !self.counters.charge_fail_soft(
            context,
            DiagnosticExecutionBudgetResource::DiagnosticSteps,
            1,
        ) {
            self.suppress_diagnostics(1);
            return;
        }
        if let Some(diagnostics) = self.counters.diagnostics.borrow_mut().as_mut() {
            diagnostics.observe_execution(context.normalized_shape_fingerprint_prefix(), usage);
        }
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn record_exact_key_hashes(
        &self,
        context: HardExecutionContext,
        hashes: &[[u8; 16]],
    ) {
        if hashes.is_empty() || !self.diagnostics_enabled() {
            return;
        }
        let steps = u64::try_from(hashes.len()).unwrap_or(u64::MAX);
        let retained_bytes = steps.saturating_mul(16);
        if !self.counters.charge_fail_soft(
            context,
            DiagnosticExecutionBudgetResource::DiagnosticSteps,
            steps,
        ) || !self.counters.charge_fail_soft(
            context,
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            retained_bytes,
        ) {
            self.suppress_diagnostics(steps);
            return;
        }
        if let Some(diagnostics) = self.counters.diagnostics.borrow_mut().as_mut() {
            diagnostics
                .observe_exact_key_hashes(context.normalized_shape_fingerprint_prefix(), hashes);
        }
    }

    #[cfg(feature = "diagnostics")]
    fn suppress_diagnostics(&self, count: u64) {
        if let Some(diagnostics) = self.counters.diagnostics.borrow_mut().as_mut() {
            diagnostics.suppress(count);
        }
    }

    #[cfg(test)]
    fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.counters.observed[resource_index(resource)].get()
    }
}

struct RequestExecutionCounters {
    budget: HardExecutionBudget,
    observed: [Cell<u64>; DiagnosticExecutionBudgetResource::ALL.len()],
    #[cfg(feature = "diagnostics")]
    diagnostics: RefCell<Option<RequestDiagnosticsState>>,
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

    #[cfg(feature = "diagnostics")]
    fn charge_fail_soft(
        &self,
        _context: HardExecutionContext,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> bool {
        let index = resource_index(resource);
        let counter = &self.observed[index];
        let current = counter.get();
        let limit = self.budget.limit(resource);
        let Some(observed) = current.checked_add(amount) else {
            counter.set(limit);
            return false;
        };
        if observed > limit {
            counter.set(limit);
            return false;
        }
        counter.set(observed);
        true
    }
}

#[cfg(feature = "diagnostics")]
fn request_diagnostics_bytes_estimate(diagnostics: &RequestDiagnostics) -> u64 {
    let shape_bytes = diagnostics.shapes.iter().fold(0_u64, |total, shape| {
        total
            .saturating_add(u64::try_from(shape.entity.len()).unwrap_or(u64::MAX))
            .saturating_add(
                u64::try_from(shape.selected_index.as_ref().map_or(0, String::len))
                    .unwrap_or(u64::MAX),
            )
            .saturating_add(
                shape
                    .residual_fields
                    .iter()
                    .chain(shape.compound_index_candidate.iter())
                    .fold(0_u64, |bytes, field| {
                        bytes.saturating_add(u64::try_from(field.len()).unwrap_or(u64::MAX))
                    }),
            )
            .saturating_add(256)
    });
    diagnostics
        .warnings
        .iter()
        .fold(shape_bytes, |total, warning| {
            total
                .saturating_add(u64::try_from(warning.message.len()).unwrap_or(u64::MAX))
                .saturating_add(32)
        })
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

    #[cfg(feature = "diagnostics")]
    #[test]
    fn request_diagnostic_work_is_charged_to_the_shared_root() {
        let root = RequestExecutionRoot::new_for_tests(REQUEST_HARD_BUDGET);
        let scope = root.scope();
        assert!(scope.enable_diagnostics());
        scope.record_query_plan(
            RequestQueryPlanEvidence::bounded(
                12,
                "Token",
                crate::db::RequestDiagnosticAccessPath::ByKey,
                None,
                Vec::new(),
                Vec::new(),
                vec![[1; 16]],
            ),
            QueryPlanCacheAttribution {
                hits: 1,
                ..QueryPlanCacheAttribution::default()
            },
        );

        assert_eq!(
            root.observed(DiagnosticExecutionBudgetResource::DiagnosticSteps),
            2,
        );
        assert!(root.observed(DiagnosticExecutionBudgetResource::TemporaryBytes) >= 21);
    }

    #[cfg(feature = "diagnostics")]
    #[test]
    fn exhausted_diagnostic_allowance_suppresses_detail_without_an_error() {
        let budget = REQUEST_HARD_BUDGET
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::DiagnosticSteps, 0);
        let root = RequestExecutionRoot::new_for_tests(budget);
        let scope = root.scope();
        assert!(scope.enable_diagnostics());
        scope.record_query_plan(
            RequestQueryPlanEvidence::bounded(
                13,
                "Token",
                crate::db::RequestDiagnosticAccessPath::ByKey,
                None,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            QueryPlanCacheAttribution::default(),
        );

        let snapshot = scope
            .diagnostics_snapshot()
            .expect("enabled diagnostics should still return a bounded snapshot");
        assert!(snapshot.shapes.is_empty());
        assert!(snapshot.suppressed_observations >= 2);
    }
}
