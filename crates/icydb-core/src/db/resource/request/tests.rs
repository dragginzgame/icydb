use super::*;

#[test]
fn request_budget_snapshots_cover_every_resource_without_mutating_the_owner() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let initial = root.request_budget();
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
        0,
    );
    for resource in DiagnosticExecutionBudgetResource::ALL {
        assert_eq!(initial.observed(resource), 0);
        assert_eq!(initial.limit(resource), REQUEST_HARD_BUDGET.limit(resource));
        assert_eq!(initial.remaining(resource), initial.limit(resource));
        root.scope()
            .charge(context, resource, resource.raw())
            .unwrap();
    }
    let charged = root.request_budget();
    assert_eq!(root.request_budget(), charged);
    for resource in DiagnosticExecutionBudgetResource::ALL {
        assert_eq!(charged.observed(resource), resource.raw());
        assert_eq!(
            charged.remaining(resource),
            charged.limit(resource) - resource.raw()
        );
        assert_eq!(initial.observed(resource), 0, "snapshots are frozen");
    }
    root.__with_current_scope(|| {
        let nested = RequestExecutionRoot::__new_or_current_runtime_root();
        assert_eq!(nested.request_budget(), charged);
    });
    let isolated = RequestExecutionRoot::__new_runtime_root();
    isolated.__with_current_scope(|| {
        assert_eq!(root.request_budget(), charged);
        assert_eq!(isolated.request_budget(), initial);
    });
}

#[test]
fn request_budget_headroom_saturates_after_failed_and_overflowing_charges() {
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
        0,
    );
    for resource in DiagnosticExecutionBudgetResource::ALL {
        let root = RequestExecutionRoot::new_for_tests(
            REQUEST_HARD_BUDGET.with_limit_for_tests(resource, 2),
        );
        let scope = root.scope();
        scope.charge(context, resource, 2).unwrap();
        assert_eq!(root.request_budget().remaining(resource), 0);
        let failure = scope.charge(context, resource, 1).unwrap_err();
        assert_eq!(failure.scope(), DiagnosticExecutionBudgetScope::Request);
        assert_eq!(failure.observed(), 3);
        assert_eq!(root.request_budget().observed(resource), 3);
        assert_eq!(root.request_budget().remaining(resource), 0);
        let overflow = scope.charge(context, resource, u64::MAX).unwrap_err();
        assert_eq!(overflow.observed(), u64::MAX);
        assert_eq!(root.request_budget().observed(resource), u64::MAX);
        assert_eq!(root.request_budget().remaining(resource), 0);
    }
}

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
fn repeated_budget_bundle_resources_use_total_remaining_capacity() {
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
        0,
    );
    for resource in DiagnosticExecutionBudgetResource::ALL {
        let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
            10,
            READ_HARD_BUDGET.failure_headroom(),
        ));
        let scope = root.scope();
        scope.charge(context, resource, 2).unwrap();
        let before = root.request_budget();
        let per_unit = [(resource, 2), (resource, 0), (resource, 2)];
        assert_eq!(scope.remaining_budget_units(&per_unit), 2);
        assert_eq!(root.request_budget(), before);

        let rejected = [(resource, 5), (resource, 4)];
        assert!(!scope.can_charge_budget_bundle(&rejected));
        assert!(!scope.try_commit_budget_bundle(&rejected));
        assert_eq!(root.request_budget(), before);

        let admitted = [(resource, 5), (resource, 0), (resource, 3)];
        assert!(scope.can_charge_budget_bundle(&admitted));
        assert_eq!(root.request_budget(), before);
        assert!(scope.try_commit_budget_bundle(&admitted));
        assert_eq!(root.observed(resource), 10);
        assert_eq!(scope.remaining_budget_units(&per_unit), 0);
    }
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

#[test]
fn current_request_scope_restores_after_nested_return_and_unwind() {
    let original = current_request_scope();
    let outer = RequestExecutionRoot::__new_runtime_root();
    let inner = RequestExecutionRoot::__new_runtime_root();
    outer.__with_current_scope(|| {
        assert!(outer.__is_current());
        inner.__with_current_scope(|| {
            assert!(inner.__is_current());
            assert!(!outer.__is_current());
        });
        assert!(outer.__is_current());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inner.__with_current_scope(|| {
                assert!(inner.__is_current());
                panic!("exercise scope restoration");
            });
        }));
        assert!(result.is_err());
        assert!(outer.__is_current());
        assert!(!inner.__is_current());
    });
    match (original, current_request_scope()) {
        (Some(before), Some(after)) => assert!(before.same_counters(&after)),
        (None, None) => {}
        _ => panic!("ambient request scope was not restored"),
    }
}
