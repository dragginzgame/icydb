//! Ordered charging must preserve the single-resource owner's refusal semantics.

use super::*;
use crate::db::RequestExecutionRoot;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

const HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);

fn run_sequence(
    charges: &[(Resource, u64)],
    resource: Resource,
    execution_limit: u64,
    request_limit: u64,
    lane: DiagnosticExecutionLane,
    ordered: bool,
) -> (
    Result<(), InternalError>,
    ExecutionBudgetUsage,
    [u64; RESOURCE_COUNT],
) {
    // Installing the execution charges its entry before the tested sequence.
    let entry = u64::from(resource == Resource::QueryExecutions);
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(u64::MAX, HEADROOM)
            .with_limit_for_tests(resource, request_limit.saturating_add(entry)),
    );
    let mut tracker = HardExecutionBudgetTracker::new_for_tests(
        HardExecutionBudget::uniform_for_tests(u64::MAX, HEADROOM)
            .with_limit_for_tests(resource, execution_limit.saturating_add(entry)),
        HardExecutionContext::new(DiagnosticExecutionBudgetScope::Execution, lane, 0x293),
    );
    tracker.request_scope = Some(root.scope());
    let (result, usage) = with_execution_budget(
        tracker,
        || {
            let result = if ordered {
                charge_current_execution_budget_ordered(charges.iter().copied())
            } else {
                charges.iter().try_for_each(|&(resource, amount)| {
                    charge_current_execution_budget(resource, amount)
                })
            };
            Ok((result, current_execution_budget_usage()?))
        },
        std::convert::identity,
        ExecutionBudgetFinish::Explicit,
    )
    .unwrap();
    let request_usage = Resource::ALL.map(|resource| root.observed(resource));
    (result, usage, request_usage)
}

#[test]
fn ordered_charges_match_sequential_failure_and_repeated_resource_accounting() {
    for (index, resource) in Resource::ALL.into_iter().enumerate() {
        let next = Resource::ALL[(index + 1) % RESOURCE_COUNT];
        let last = Resource::ALL[(index + 2) % RESOURCE_COUNT];
        let charges = [
            (resource, 0),
            (next, 1),
            (resource, 2),
            (next, 0),
            (resource, 1),
            (last, 1),
            (resource, 3),
        ];
        for lane in [
            DiagnosticExecutionLane::PublicRead,
            DiagnosticExecutionLane::TrustedRead,
            DiagnosticExecutionLane::Diagnostic,
        ] {
            // Fail at each occurrence or admit the exact total. A failure in
            // either scope must preserve the same attempted prefix and facts.
            for limit in [0, 2, 3, 6] {
                for (execution_limit, request_limit) in
                    [(limit, u64::MAX), (u64::MAX, limit), (limit, limit)]
                {
                    let sequential = run_sequence(
                        &charges,
                        resource,
                        execution_limit,
                        request_limit,
                        lane,
                        false,
                    );
                    let ordered = run_sequence(
                        &charges,
                        resource,
                        execution_limit,
                        request_limit,
                        lane,
                        true,
                    );
                    assert_eq!(ordered.0.is_ok(), limit == 6);
                    let facts = |result: &Result<(), InternalError>| {
                        result.as_ref().err().map(|error| {
                            (
                                error.class(),
                                error.origin(),
                                error.diagnostic_code(),
                                error.diagnostic_facts(),
                            )
                        })
                    };
                    assert_eq!(facts(&ordered.0), facts(&sequential.0));
                    assert_eq!(ordered.1.observed, sequential.1.observed);
                    assert_eq!(ordered.2, sequential.2);
                }
            }
        }
    }
}

#[test]
fn ordered_charges_preserve_inactive_and_zero_only_behavior() {
    assert!(charge_current_execution_budget_ordered([(Resource::RowsVisited, 1)]).is_ok());
    ACTIVE_EXECUTION_BUDGET.with(|active| {
        let _borrow = active.borrow_mut();
        assert!(charge_current_execution_budget_ordered([]).is_ok());
        assert!(
            charge_current_execution_budget_ordered([
                (Resource::RowsVisited, 0),
                (Resource::StoredBytesRead, 0),
            ])
            .is_ok()
        );
        let error =
            charge_current_execution_budget_ordered([(Resource::RowsVisited, 1)]).unwrap_err();
        assert_eq!(
            error.diagnostic_code(),
            InternalError::query_executor_invariant().diagnostic_code(),
        );
    });
}
