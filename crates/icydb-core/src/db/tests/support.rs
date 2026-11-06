//! Module: db::tests::support
//! Responsibility: db-local test helper modules.
//! Does not own: runtime test fixtures or production support APIs.
//! Boundary: exposes helpers only inside db test code.

pub(in crate::db) mod index;
pub(in crate::db) mod source_guard;

use crate::db::{
    RequestExecutionRoot,
    resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionLane, DiagnosticFactTag,
};

/// Construct the shared uniform test profile with one resource overridden.
///
/// Exact-limit tests keep unrelated resource ceilings and failure headroom
/// fixed while exercising their selected admission boundary.
#[must_use]
pub(in crate::db) fn request_with_limit(
    resource: DiagnosticExecutionBudgetResource,
    limit: u64,
) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

/// Assert the selected resource in an internal or query error's diagnostic facts.
///
/// # Panics
///
/// Panics if the expected resource fact is absent.
#[track_caller]
pub(in crate::db) fn assert_budget_resource(
    facts: &[(DiagnosticFactTag, u64)],
    resource: DiagnosticExecutionBudgetResource,
) {
    assert!(facts.contains(&(DiagnosticFactTag::BudgetResource, resource.raw())));
}

/// Assert cumulative exhaustion facts supplied by the caller's boundary test.
///
/// Charge calculations and failure-order checks remain with each test.
///
/// # Panics
///
/// Panics if the observed charge is within the limit or an expected fact is absent.
#[track_caller]
pub(in crate::db) fn assert_budget_exceeded(
    facts: &[(DiagnosticFactTag, u64)],
    resource: DiagnosticExecutionBudgetResource,
    lane: DiagnosticExecutionLane,
    limit: u64,
    actual: u64,
) {
    assert!(actual > limit);
    assert_budget_resource(facts, resource);
    for fact in [
        (DiagnosticFactTag::ExecutionLane, lane.raw()),
        (DiagnosticFactTag::Limit, limit),
        (DiagnosticFactTag::Actual, actual),
    ] {
        assert!(facts.contains(&fact));
    }
}
