//! Maintenance counters are explicit, cumulative, and fail closed before handoff.

use super::*;
use crate::db::query::construction::ConstructionBudget;
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};
use std::convert::identity;

fn assert_resource(error: InternalError, resource: Resource) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
    );
}

#[test]
fn maintenance_uses_the_existing_mutation_profile_for_every_resource() {
    for resource in Resource::ALL {
        let budget = MaintenanceConstructionBudget::new();
        let limit = MUTATION_HARD_BUDGET.limit(resource);
        budget.charge(resource, limit).unwrap();
        assert_resource(budget.charge(resource, 1).unwrap_err(), resource);
    }
}

#[test]
fn maintenance_segments_accumulate_and_first_exhaustion_is_sticky() {
    let budget =
        MaintenanceConstructionBudget::with_limit_for_tests(Resource::PredicateExpressionSteps, 3);
    for _ in 0..3 {
        budget
            .run(
                |work| work.charge(Resource::PredicateExpressionSteps, 1),
                identity,
            )
            .unwrap();
    }
    let mut published = false;
    let error = budget
        .run(
            |work| {
                // Even a faulty inner consumer cannot erase exhaustion into success.
                let _ = work.charge(Resource::PredicateExpressionSteps, 1);
                Ok::<_, InternalError>(())
            },
            identity,
        )
        .unwrap_err();
    assert_resource(error, Resource::PredicateExpressionSteps);
    assert_resource(
        budget
            .run(
                |_| {
                    published = true;
                    Ok::<_, InternalError>(())
                },
                identity,
            )
            .unwrap_err(),
        Resource::PredicateExpressionSteps,
    );
    assert!(!published);
    assert_resource(
        budget.charge(Resource::TemporaryBytes, 0).unwrap_err(),
        Resource::PredicateExpressionSteps,
    );
}

#[test]
fn maintenance_oversized_construction_rejects_instead_of_returning_an_empty_page() {
    for _ in 0..2 {
        let budget =
            MaintenanceConstructionBudget::with_limit_for_tests(Resource::TemporaryBytes, 7);
        let result = budget.run(
            |work| (work as &dyn ConstructionBudget).vec_with_capacity::<u8>(8),
            identity,
        );
        assert_resource(result.unwrap_err(), Resource::TemporaryBytes);
    }
}

#[test]
fn maintenance_failed_work_remains_charged_across_later_segments() {
    let budget = MaintenanceConstructionBudget::with_limit_for_tests(Resource::RowsVisited, 1);
    budget
        .run(
            |work| {
                work.charge(Resource::RowsVisited, 1)?;
                Err::<(), _>(InternalError::store_invariant())
            },
            identity,
        )
        .unwrap_err();
    assert_eq!(budget.tracker.borrow().observed(Resource::RowsVisited), 1);
    assert_resource(
        budget
            .run(|work| work.charge(Resource::RowsVisited, 1), identity)
            .unwrap_err(),
        Resource::RowsVisited,
    );
}

#[test]
fn maintenance_does_not_use_or_replace_an_active_execution_tracker() {
    let maintenance =
        MaintenanceConstructionBudget::with_limit_for_tests(Resource::TemporaryBytes, 4);
    with_query_execution_budget_for_tests(
        MUTATION_HARD_BUDGET.with_limit_for_tests(Resource::TemporaryBytes, 0),
        HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            99,
        ),
        || {
            maintenance.run(
                |work| {
                    work.charge(Resource::TemporaryBytes, 4)
                        .map_err(QueryError::execute)
                },
                QueryError::execute,
            )?;
            assert_resource(
                ExecutionConstructionBudget
                    .charge(Resource::TemporaryBytes, 1)
                    .unwrap_err(),
                Resource::TemporaryBytes,
            );
            Ok(())
        },
    )
    .unwrap();
    assert_resource(
        maintenance.charge(Resource::TemporaryBytes, 1).unwrap_err(),
        Resource::TemporaryBytes,
    );
}

#[test]
fn maintenance_borrow_conflicts_are_typed_errors_not_panics() {
    let maintenance = MaintenanceConstructionBudget::new();
    let _borrow = maintenance.tracker.borrow_mut();
    assert!(
        maintenance
            .run(|_| Ok::<_, InternalError>(()), identity)
            .is_err()
    );
}
