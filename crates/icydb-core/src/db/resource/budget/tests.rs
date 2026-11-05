use super::*;
use crate::db::RequestExecutionRoot;
use icydb_diagnostic_code::{DiagnosticDetail, DiagnosticFactTag, RuntimeBoundaryCode};

const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn read_entry_accounts_for_the_first_instruction_interval() {
    let instructions = DiagnosticExecutionBudgetResource::InstructionUnits;
    for small_charges in [0, 1, INSTRUCTION_WATERMARK_CHARGE_INTERVAL - 1] {
        let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            TEST_HEADROOM,
        ));
        for expected_total in [101, 202] {
            with_read_execution_budget(&root.scope(), TEST_CONTEXT, || {
                for _ in 0..small_charges {
                    charge_current_execution_budget(
                        DiagnosticExecutionBudgetResource::RowsVisited,
                        1,
                    )?;
                }
                // Supply a logical IC counter sample, not a native timing
                // measurement. Host execution starts at counter zero.
                ACTIVE_EXECUTION_BUDGET.with(|budget| {
                    let mut budget = budget.borrow_mut();
                    let tracker = budget.as_mut().unwrap();
                    tracker.record_instruction_watermark(101).unwrap();
                    assert_eq!(tracker.observed(instructions), 101);
                    assert_eq!(root.observed(instructions), expected_total);
                });
                Ok(())
            })
            .unwrap();
            assert_eq!(root.observed(instructions), expected_total);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn read_first_instruction_interval_enforces_both_budget_scopes() {
    let instructions = DiagnosticExecutionBudgetResource::InstructionUnits;
    let execution_limit = READ_HARD_BUDGET.limit(instructions);
    for (request_limit, sample, expected_scope, expected_limit) in [
        (100, 100, None, 100),
        (100, 101, Some(DiagnosticExecutionBudgetScope::Request), 100),
        (
            u64::MAX,
            execution_limit + 1,
            Some(DiagnosticExecutionBudgetScope::Execution),
            execution_limit,
        ),
    ] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
                .with_limit_for_tests(instructions, request_limit),
        );
        let result = with_read_execution_budget(&root.scope(), TEST_CONTEXT, || {
            ACTIVE_EXECUTION_BUDGET.with(|budget| {
                budget
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .record_instruction_watermark(sample)
                    .map_err(InternalError::from)
            })
        });
        assert_eq!(root.observed(instructions), sample);
        if let Some(scope) = expected_scope {
            let error = result.unwrap_err();
            let facts = error.diagnostic_facts();
            for fact in [
                (DiagnosticFactTag::BudgetResource, instructions.raw()),
                (DiagnosticFactTag::Limit, expected_limit),
                (DiagnosticFactTag::Actual, sample),
                (DiagnosticFactTag::ExecutionBudgetScope, scope.raw()),
            ] {
                assert!(facts.contains(&fact));
            }
        } else {
            result.unwrap();
        }
        assert!(ACTIVE_EXECUTION_BUDGET.with(|budget| budget.borrow().is_none()));
    }
}

#[test]
fn construction_requires_active_authority_and_shares_execution_request_counters() {
    use crate::db::resource::construction::ConstructionBudget;
    let resource = DiagnosticExecutionBudgetResource::TemporaryBytes;
    let construction = &ExecutionConstructionBudget as &dyn ConstructionBudget;
    assert!(construction.charge(resource, 0).is_err());
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(resource, 3),
    );
    with_execution_budget(
        HardExecutionBudgetTracker::new_with_request_scope(
            &BUNDLE_EXECUTION_BUDGET,
            TEST_CONTEXT,
            &root.scope(),
        ),
        || {
            construction.copy_text("abc")?;
            assert_eq!(current_execution_budget_usage()?.observed(resource), 3);
            assert_eq!(root.observed(resource), 3);
            let err = construction.copy_text("x").unwrap_err();
            assert!(
                err.diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
            Ok::<_, InternalError>(())
        },
        std::convert::identity,
        ExecutionBudgetFinish::Automatic,
    )
    .unwrap();
    assert!(construction.copy_text("x").is_err());
}
const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
    DiagnosticExecutionBudgetScope::Execution,
    DiagnosticExecutionLane::PublicRead,
    0x0102_0304_0506_0708,
);
static PAIR_FIRST_FAILURE_BUDGET: HardExecutionBudget =
    HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 0);
static BUNDLE_EXECUTION_BUDGET: HardExecutionBudget =
    HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::GroupDistinctEntries, 5)
        .with_limit_for_tests(
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            50,
        );

#[test]
fn every_resource_charges_monotonically_and_retains_rejected_work() {
    let budget = HardExecutionBudget::new([1; RESOURCE_COUNT], TEST_HEADROOM);
    for resource in DiagnosticExecutionBudgetResource::ALL {
        let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, TEST_CONTEXT);
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
fn mutation_execution_policy_identity_covers_limits_and_failure_reserves() {
    assert_eq!(MUTATION_EXECUTION_BUDGET_POLICY_IDENTITY, 0x35cc_94d9);
    assert_eq!(
        MUTATION_HARD_BUDGET.limit(DiagnosticExecutionBudgetResource::InstructionUnits),
        MUTATION_EXECUTION_INSTRUCTION_LIMIT,
    );
    assert_eq!(
        MUTATION_HARD_BUDGET.failure_headroom().instruction_units(),
        MUTATION_EXECUTION_INSTRUCTION_FAILURE_RESERVE,
    );

    for resource in DiagnosticExecutionBudgetResource::ALL {
        let changed = MUTATION_HARD_BUDGET.with_limit_for_tests(
            resource,
            MUTATION_HARD_BUDGET.limit(resource).wrapping_add(1),
        );
        assert_ne!(
            hard_execution_budget_policy_identity(&changed),
            MUTATION_EXECUTION_BUDGET_POLICY_IDENTITY,
            "resource {resource:?} must participate in mutation execution policy identity",
        );
    }

    for headroom in [
        HardExecutionFailureHeadroom::new(
            MUTATION_EXECUTION_INSTRUCTION_FAILURE_RESERVE + 1,
            MUTATION_FAILURE_HEADROOM.response_bytes,
        ),
        HardExecutionFailureHeadroom::new(
            MUTATION_EXECUTION_INSTRUCTION_FAILURE_RESERVE,
            MUTATION_FAILURE_HEADROOM.response_bytes + 1,
        ),
    ] {
        let changed = HardExecutionBudget::new(MUTATION_HARD_BUDGET.limits, headroom);
        assert_ne!(
            hard_execution_budget_policy_identity(&changed),
            MUTATION_EXECUTION_BUDGET_POLICY_IDENTITY,
        );
    }
}

#[test]
fn arithmetic_overflow_is_exhaustion_and_never_refunds_usage() {
    let budget = HardExecutionBudget::new([u64::MAX; RESOURCE_COUNT], TEST_HEADROOM);
    let resource = DiagnosticExecutionBudgetResource::PlanningSteps;
    let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, TEST_CONTEXT);
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
    let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, TEST_CONTEXT);
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

#[test]
fn result_charges_preserve_exact_limits_and_sequential_failure_accounting() {
    use DiagnosticExecutionBudgetResource::{
        MaterializedBytes, NestedValueSteps, ResultBytes, ResultRows,
    };

    const RESULT_BUDGET: HardExecutionBudget =
        HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(ResultRows, 2)
            .with_limit_for_tests(NestedValueSteps, 3)
            .with_limit_for_tests(MaterializedBytes, 11)
            .with_limit_for_tests(ResultBytes, 11);
    static BUDGETS: [HardExecutionBudget; 5] = [
        RESULT_BUDGET,
        RESULT_BUDGET.with_limit_for_tests(ResultRows, 1),
        RESULT_BUDGET.with_limit_for_tests(NestedValueSteps, 2),
        RESULT_BUDGET.with_limit_for_tests(MaterializedBytes, 10),
        RESULT_BUDGET.with_limit_for_tests(ResultBytes, 10),
    ];

    for (case, budget) in BUDGETS.iter().enumerate() {
        let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            TEST_HEADROOM,
        ));
        let result = with_execution_budget(
            HardExecutionBudgetTracker::new_with_request_scope(budget, TEST_CONTEXT, &root.scope()),
            || charge_runtime_result_work(2, 11, 3),
            std::convert::identity,
            ExecutionBudgetFinish::Automatic,
        );
        assert_eq!(result.is_ok(), case == 0);
        for (index, (resource, expected)) in [
            (ResultRows, 2),
            (NestedValueSteps, 3),
            (MaterializedBytes, 11),
            (ResultBytes, 11),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                root.observed(resource),
                if case == 0 || index < case {
                    expected
                } else {
                    0
                },
            );
        }
    }
}

#[test]
fn paired_budget_charges_preserve_sequential_failure_order() {
    let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        TEST_HEADROOM,
    ));
    let error = with_execution_budget(
        HardExecutionBudgetTracker::new_with_request_scope(
            &PAIR_FIRST_FAILURE_BUDGET,
            TEST_CONTEXT,
            &root.scope(),
        ),
        || {
            charge_current_execution_budget_ordered([
                (DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1),
                (DiagnosticExecutionBudgetResource::CursorSteps, 1),
            ])
        },
        std::convert::identity,
        ExecutionBudgetFinish::Automatic,
    )
    .expect_err("the first paired charge should retain its ordinary hard limit");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    assert_eq!(
        root.observed(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited),
        1,
    );
    assert_eq!(
        root.observed(DiagnosticExecutionBudgetResource::CursorSteps),
        0,
        "the second charge must not run after the first fails",
    );
}

#[test]
fn decoded_row_charges_preserve_exact_limits_and_failure_accounting() {
    use DiagnosticExecutionBudgetResource::{DecodedBytes, NestedValueSteps};

    const DECODE_BUDGET: HardExecutionBudget =
        HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(DecodedBytes, 7)
            .with_limit_for_tests(NestedValueSteps, 3);
    static DECODE_BUDGETS: [HardExecutionBudget; 3] = [
        DECODE_BUDGET,
        DECODE_BUDGET.with_limit_for_tests(DecodedBytes, 6),
        DECODE_BUDGET.with_limit_for_tests(NestedValueSteps, 2),
    ];

    for (budget, rejected_resource, observed_steps) in [
        (&DECODE_BUDGETS[0], None, 3),
        (&DECODE_BUDGETS[1], Some(DecodedBytes), 0),
        (&DECODE_BUDGETS[2], Some(NestedValueSteps), 3),
    ] {
        let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            TEST_HEADROOM,
        ));
        let result = with_execution_budget(
            HardExecutionBudgetTracker::new_with_request_scope(budget, TEST_CONTEXT, &root.scope()),
            || charge_decoded_row(7, 3),
            std::convert::identity,
            ExecutionBudgetFinish::Automatic,
        );

        if let Some(resource) = rejected_resource {
            let error = result.expect_err("decoding above either ceiling must reject");
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw())),
            );
        } else {
            result.expect("decoding at both ceilings must succeed");
        }
        assert_eq!(root.observed(DecodedBytes), 7);
        assert_eq!(root.observed(NestedValueSteps), observed_steps);
    }
}

#[test]
fn materialized_data_row_charge_owns_byte_resources() {
    use DiagnosticExecutionBudgetResource::{MaterializedBytes, StoredBytesRead};

    const ROW_BUDGET: HardExecutionBudget =
        HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(StoredBytesRead, 7)
            .with_limit_for_tests(MaterializedBytes, 7);
    static BUDGETS: [HardExecutionBudget; 3] = [
        ROW_BUDGET,
        ROW_BUDGET.with_limit_for_tests(StoredBytesRead, 6),
        ROW_BUDGET.with_limit_for_tests(MaterializedBytes, 6),
    ];
    for (case, budget) in BUDGETS.iter().enumerate() {
        let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            TEST_HEADROOM,
        ));
        let result = with_execution_budget(
            HardExecutionBudgetTracker::new_with_request_scope(budget, TEST_CONTEXT, &root.scope()),
            || charge_materialized_data_row(7),
            std::convert::identity,
            ExecutionBudgetFinish::Automatic,
        );
        assert_eq!(root.observed(StoredBytesRead), 7);
        assert_eq!(
            root.observed(MaterializedBytes),
            if case == 1 { 0 } else { 7 }
        );
        if case == 0 {
            result.expect("exact row byte ceilings should admit");
        } else {
            let resource = if case == 1 {
                StoredBytesRead
            } else {
                MaterializedBytes
            };
            assert!(
                result
                    .unwrap_err()
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
            );
        }
    }
    charge_materialized_data_row(7).expect("inactive accounting remains a no-op");
}

#[test]
fn semantic_budget_bundle_is_atomic_across_execution_and_request() {
    let entries = DiagnosticExecutionBudgetResource::GroupDistinctEntries;
    let bytes = DiagnosticExecutionBudgetResource::GroupDistinctStateBytes;
    let request_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(entries, 3)
        .with_limit_for_tests(bytes, 30);
    let root = RequestExecutionRoot::new_for_tests(request_budget);

    with_execution_budget(
        HardExecutionBudgetTracker::new_with_request_scope(
            &BUNDLE_EXECUTION_BUDGET,
            TEST_CONTEXT,
            &root.scope(),
        ),
        || {
            charge_current_execution_budget(entries, 1)?;
            charge_current_execution_budget(bytes, 10)?;
            let before = current_execution_budget_usage()?;

            assert_eq!(
                current_execution_remaining_budget_units(&[(entries, 1), (entries, 1)])?,
                1
            );
            assert!(!try_charge_current_execution_budget_bundle(&[
                (entries, 2),
                (bytes, 10),
                (entries, 1),
            ])?);
            let duplicate_rejected = current_execution_budget_usage()?;
            assert_eq!(
                duplicate_rejected.observed(entries),
                before.observed(entries)
            );
            assert_eq!(duplicate_rejected.observed(bytes), before.observed(bytes));
            assert_eq!(root.observed(entries), 1);
            assert_eq!(root.observed(bytes), 10);

            assert!(!try_charge_current_execution_budget_bundle(&[
                (entries, 3),
                (bytes, 10),
            ])?);
            let rejected = current_execution_budget_usage()?;
            assert_eq!(rejected.observed(entries), before.observed(entries));
            assert_eq!(rejected.observed(bytes), before.observed(bytes));
            assert_eq!(root.observed(entries), 1);
            assert_eq!(root.observed(bytes), 10);

            assert!(try_charge_current_execution_budget_bundle(&[
                (entries, 2),
                (bytes, 20),
            ])?);
            let committed = current_execution_budget_usage()?;
            assert_eq!(committed.observed(entries), 3);
            assert_eq!(committed.observed(bytes), 30);
            assert_eq!(root.observed(entries), 3);
            assert_eq!(root.observed(bytes), 30);

            Ok::<_, InternalError>(())
        },
        std::convert::identity,
        ExecutionBudgetFinish::Automatic,
    )
    .expect("atomic bundle proof should complete");
}

#[test]
fn repeated_budget_bundle_resources_are_atomic_for_each_limiting_scope() {
    let entries = DiagnosticExecutionBudgetResource::GroupDistinctEntries;
    let bytes = DiagnosticExecutionBudgetResource::GroupDistinctStateBytes;
    for (execution_limit, request_limit) in [(10, 100), (100, 10), (10, 10)] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
                .with_limit_for_tests(entries, request_limit),
        );
        let mut tracker = HardExecutionBudgetTracker::new_for_tests(
            HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
                .with_limit_for_tests(entries, execution_limit),
            TEST_CONTEXT,
        );
        tracker.request_scope = Some(root.scope());
        tracker.precharge(entries, 2).unwrap();
        tracker.precharge(bytes, 1).unwrap();
        let before_execution = tracker.observed;
        let before_request = root.request_budget();

        for bundle in [
            [(entries, 5), (bytes, 1), (entries, 4)],
            [(entries, 4), (entries, 5), (bytes, 1)],
        ] {
            assert!(!tracker.try_charge_budget_bundle(&bundle).unwrap());
            assert_eq!(tracker.observed, before_execution);
            assert_eq!(root.request_budget(), before_request);
        }
        assert_eq!(
            tracker.remaining_budget_units(&[(entries, 2), (entries, 2)]),
            2
        );
        assert!(
            tracker
                .try_charge_budget_bundle(&[(entries, 5), (bytes, 1), (entries, 0), (entries, 3),])
                .unwrap()
        );
        assert_eq!(tracker.observed(entries), 10);
        assert_eq!(root.observed(entries), 10);
        assert_eq!(tracker.observed(bytes), 2);
        assert_eq!(root.observed(bytes), 2);
        let committed_request = root.request_budget();
        let committed_execution = tracker.observed;
        assert!(
            !tracker
                .try_charge_budget_bundle(&[(entries, 1), (bytes, 1)])
                .unwrap()
        );
        assert_eq!(root.request_budget(), committed_request);
        assert_eq!(tracker.observed, committed_execution);
    }
}

#[test]
fn budget_bundle_instruction_exhaustion_preserves_semantic_counters() {
    let instructions = DiagnosticExecutionBudgetResource::InstructionUnits;
    let entries = DiagnosticExecutionBudgetResource::GroupDistinctEntries;
    for (execution_limit, request_limit, expected_scope) in [
        (0, u64::MAX, DiagnosticExecutionBudgetScope::Execution),
        (u64::MAX, 0, DiagnosticExecutionBudgetScope::Request),
    ] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
                .with_limit_for_tests(instructions, request_limit),
        );
        let mut tracker = HardExecutionBudgetTracker::new_for_tests(
            HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
                .with_limit_for_tests(instructions, execution_limit),
            TEST_CONTEXT,
        );
        tracker.request_scope = Some(root.scope());
        tracker.precharge(instructions, 1).unwrap_err();
        let before_request = root.request_budget();
        let before_execution = tracker.observed;
        let failure = tracker
            .try_charge_budget_bundle(&[(entries, 1), (entries, 1)])
            .unwrap_err();
        assert_eq!(failure.resource(), instructions);
        assert_eq!(failure.scope(), expected_scope);
        assert_eq!(failure.observed(), 1);
        assert_eq!(root.request_budget(), before_request);
        assert_eq!(tracker.observed, before_execution);
    }
}

#[test]
fn budget_bundle_overflow_is_rejected_without_saturating_into_admission() {
    let resource = DiagnosticExecutionBudgetResource::ResultBytes;
    let unlimited = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let root = RequestExecutionRoot::new_for_tests(unlimited);
    let mut tracker = HardExecutionBudgetTracker::new_for_tests(unlimited, TEST_CONTEXT);
    let scope = root.scope();
    let overflow = [(resource, u64::MAX), (resource, 1)];
    // The execution-only owner must enforce the same totals as an attached request.
    assert!(!tracker.try_charge_budget_bundle(&overflow).unwrap());
    assert_eq!(tracker.observed(resource), 0);
    tracker.request_scope = Some(root.scope());
    assert!(!scope.can_charge_budget_bundle(&overflow));
    assert!(!scope.try_commit_budget_bundle(&overflow));
    assert!(!tracker.try_charge_budget_bundle(&overflow).unwrap());
    assert_eq!(scope.remaining_budget_units(&overflow), 0);
    assert_eq!(tracker.remaining_budget_units(&overflow), 0);
    assert_eq!(root.observed(resource), 0);
    assert_eq!(tracker.observed(resource), 0);
    assert_eq!(scope.remaining_budget_units(&[]), u64::MAX);
    assert_eq!(tracker.remaining_budget_units(&[(resource, 0)]), u64::MAX);
    assert!(scope.try_commit_budget_bundle(&[]));
    assert!(tracker.try_charge_budget_bundle(&[(resource, 0)]).unwrap());
    assert!(
        tracker
            .try_charge_budget_bundle(&[(resource, u64::MAX - 1), (resource, 1)])
            .unwrap()
    );
    let exhausted = root.request_budget();
    assert!(!tracker.try_charge_budget_bundle(&[(resource, 1)]).unwrap());
    assert_eq!(root.request_budget(), exhausted);
    assert_eq!(tracker.observed(resource), u64::MAX);
}

#[test]
fn derived_execution_trackers_cannot_reset_the_shared_request_scope() {
    let request_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::QueryExecutions, 2);
    let root = RequestExecutionRoot::new_for_tests(request_budget);
    let scope = root.scope();

    for _ in 0..2 {
        HardExecutionBudgetTracker::new_with_request_scope(&READ_HARD_BUDGET, TEST_CONTEXT, &scope)
            .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
            .expect("work at the aggregate request ceiling should admit");
    }
    let exhausted = HardExecutionBudgetTracker::new_with_request_scope(
        &READ_HARD_BUDGET,
        TEST_CONTEXT,
        &root.scope(),
    )
    .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
    .expect_err("a fresh derived scope handle must not reset request counters");

    assert_eq!(exhausted.scope(), DiagnosticExecutionBudgetScope::Request);
    assert_eq!(exhausted.observed(), 3);
    assert_eq!(
        root.observed(DiagnosticExecutionBudgetResource::QueryExecutions),
        3,
    );
}

#[test]
fn failures_retries_and_nested_executions_remain_charged() {
    let request_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let root = RequestExecutionRoot::new_for_tests(request_budget);
    let scope = root.scope();
    let failed = with_execution_budget(
        HardExecutionBudgetTracker::new_with_request_scope(&READ_HARD_BUDGET, TEST_CONTEXT, &scope),
        || Err::<(), _>(InternalError::query_executor_invariant()),
        std::convert::identity,
        ExecutionBudgetFinish::Automatic,
    );
    assert!(failed.is_err());

    let retried = with_execution_budget(
        HardExecutionBudgetTracker::new_with_request_scope(&READ_HARD_BUDGET, TEST_CONTEXT, &scope),
        || {
            with_execution_budget(
                HardExecutionBudgetTracker::new_with_request_scope(
                    &READ_HARD_BUDGET,
                    TEST_CONTEXT,
                    &scope,
                ),
                || Ok::<_, InternalError>(()),
                std::convert::identity,
                ExecutionBudgetFinish::Automatic,
            )
        },
        std::convert::identity,
        ExecutionBudgetFinish::Automatic,
    );
    assert!(retried.is_ok());
    assert_eq!(
        root.observed(DiagnosticExecutionBudgetResource::QueryExecutions),
        3,
        "the failed attempt, retry, and nested execution all stay charged",
    );
}

#[test]
fn planning_and_compilation_charges_share_the_request_scope() {
    let request_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::PlanningSteps, 2)
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::PlanCompilations, 1);
    let root = RequestExecutionRoot::new_for_tests(request_budget);
    let first_scope = root.scope();
    first_scope
        .charge(
            TEST_CONTEXT,
            DiagnosticExecutionBudgetResource::PlanningSteps,
            1,
        )
        .expect("the first planning operation should be admitted");
    first_scope
        .charge(
            TEST_CONTEXT,
            DiagnosticExecutionBudgetResource::PlanCompilations,
            1,
        )
        .expect("the first compilation should be admitted");

    let second_scope = root.scope();
    second_scope
        .charge(
            TEST_CONTEXT,
            DiagnosticExecutionBudgetResource::PlanningSteps,
            1,
        )
        .expect("planning at the aggregate ceiling should be admitted");
    let exhausted = second_scope
        .charge(
            TEST_CONTEXT,
            DiagnosticExecutionBudgetResource::PlanCompilations,
            1,
        )
        .expect_err("a derived scope must retain the earlier compilation charge");

    assert_eq!(exhausted.scope(), DiagnosticExecutionBudgetScope::Request);
    assert_eq!(exhausted.limit(), 1);
    assert_eq!(exhausted.observed(), 2);
    assert_eq!(
        root.observed(DiagnosticExecutionBudgetResource::PlanningSteps),
        2,
    );
    assert_eq!(
        root.observed(DiagnosticExecutionBudgetResource::PlanCompilations),
        2,
    );
}

#[test]
fn collection_scale_n_plus_one_work_fails_at_the_aggregate_request_boundary() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let scope = root.scope();
    let mut rejected = None;
    for _ in 0..257 {
        let charge = HardExecutionBudgetTracker::new_with_request_scope(
            &READ_HARD_BUDGET,
            HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::TrustedRead,
                0x746f_6b6f_2d6e_2b31,
            ),
            &scope,
        )
        .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1);
        if let Err(exhausted) = charge {
            rejected = Some(exhausted);
            break;
        }
    }
    let exhausted = rejected.expect("the 257th individually bounded query should reject");

    assert_eq!(exhausted.scope(), DiagnosticExecutionBudgetScope::Request);
    assert_eq!(exhausted.lane(), DiagnosticExecutionLane::TrustedRead);
    assert_eq!(exhausted.limit(), 256);
    assert_eq!(exhausted.observed(), 257);
}
