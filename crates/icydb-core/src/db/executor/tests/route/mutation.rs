use super::*;

#[test]
fn route_plan_mutation_is_materialized_with_no_fast_paths_or_hints() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.mode = QueryMode::Delete(DeleteSpec::new());

    let route_plan =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_mutation(&plan)
            .expect("mutation route plan should build");

    assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
    assert_eq!(route_plan.fast_path_order(), &MUTATION_FAST_PATH_ORDER);
    assert_eq!(route_plan.direction(), Direction::Asc);
    assert_eq!(route_plan.continuation_mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.window().effective_offset, 0);
    assert!(
        route_plan.scan_hints.physical_fetch_hint.is_none(),
        "mutation route should not emit physical fetch hints"
    );
    assert!(
        route_plan.scan_hints.load_scan_budget_hint.is_none(),
        "mutation route should not emit load scan-budget hints"
    );
}

#[test]
fn route_plan_mutation_rejects_non_delete_mode() {
    let plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    let result = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_mutation(&plan);
    let Err(err) = result else {
        panic!("mutation route must reject non-delete plans")
    };

    assert_eq!(err.class, crate::error::ErrorClass::InvariantViolation);
    assert!(
        err.message
            .contains("mutation route planning requires delete plans"),
        "mutation route rejection should return clear invariant message"
    );
}
