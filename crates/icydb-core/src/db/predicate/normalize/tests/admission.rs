use crate::{
    db::{
        predicate::normalize::admission::admit_enum_input_construction,
        query::preparation::PreparationWork,
        tests::support::{assert_budget_resource, request_with_limit},
    },
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn enum_input_preflight_covers_nested_payloads_and_preserves_canonical_detection() {
    for (value, expected_enum) in [
        (
            Value::List(vec![Value::Map(vec![(
                Value::Text("k".repeat(128)),
                Value::Blob(vec![1; 4096]),
            )])]),
            false,
        ),
        (
            Value::List(vec![Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Blob(vec![1; 4096]),
            ))]),
            true,
        ),
        (
            Value::Map(vec![(
                Value::Enum(ValueEnum::test_unit(1, 1)),
                Value::Text("v".into()),
            )]),
            true,
        ),
        (
            Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::List(vec![
                    Value::Text("界".repeat(128)),
                    Value::Blob(vec![1; 4096]),
                ]),
            )),
            true,
        ),
    ] {
        for resource in [
            Resource::TemporaryBytes,
            Resource::NestedValueSteps,
            Resource::PredicateExpressionSteps,
            Resource::SortEntries,
            Resource::SortComparisons,
        ] {
            let root = request_with_limit(resource, 16_000_000);
            let admitted = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap();
            assert_eq!(admitted, expected_enum);
            let exact = root.observed(resource);
            if exact == 0 {
                continue;
            }
            let root = request_with_limit(resource, exact - 1);
            let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap_err();
            assert_budget_resource(&error.diagnostic_facts(), resource);
        }
    }
}

#[test]
fn enum_preflight_admits_nested_collection_ordering_before_payload_work() {
    let values = vec![Value::Text("界".repeat(128)), Value::Text("a".into())];
    let map = Value::Map(vec![
        (values[0].clone(), Value::Blob(vec![0; 4096])),
        (values[1].clone(), Value::Blob(vec![1; 4096])),
    ]);
    for (value, entries, comparisons, canonical) in [
        (Value::List(values), 2, 3, false),
        (map.clone(), 2, 3, false),
        (Value::List(vec![map.clone(); 2]), 6, 9, false),
        (Value::Enum(ValueEnum::test_payload(1, 1, map)), 2, 3, true),
    ] {
        for (resource, amount) in [
            (Resource::SortEntries, entries),
            (Resource::SortComparisons, comparisons),
        ] {
            let root = request_with_limit(resource, amount);
            assert_eq!(
                PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                    admit_enum_input_construction(&value, work)
                })
                .unwrap(),
                canonical
            );
            assert_eq!(root.observed(resource), amount);
            assert_eq!(root.observed(Resource::SortTemporaryBytes), 0);
            let root = request_with_limit(resource, 0);
            let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap_err();
            assert_budget_resource(&error.diagnostic_facts(), resource);
            // Only the root and an optional enum shell are visited. The
            // refused collection never inspects or copies its large payloads.
            assert_eq!(
                root.observed(Resource::NestedValueSteps),
                if canonical { 4 } else { 2 }
            );
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        }
    }
}

#[test]
fn enum_preflight_compares_map_keys_without_charging_value_payloads_as_keys() {
    let observed = |payload| {
        let value = Value::Enum(ValueEnum::test_payload(
            1,
            1,
            Value::Map(vec![
                (Value::Text("aa".into()), Value::Blob(vec![0; payload])),
                (Value::Text("ab".into()), Value::Blob(vec![1; payload])),
            ]),
        ));
        let root = request_with_limit(Resource::PredicateExpressionSteps, u64::MAX);
        PreparationWork::run(&root.scope(), Lane::Mutation, |work| {
            admit_enum_input_construction(&value, work)
        })
        .unwrap();
        root.observed(Resource::PredicateExpressionSteps)
    };
    // Two two-byte keys are inspected, then their maximum extent is reserved
    // for ordering/uniqueness (10), plus separate catalog label checks (4).
    assert_eq!(observed(0), 14);
    assert_eq!(observed(4096), 14);
}

#[test]
fn enum_preflight_empty_and_singleton_containers_need_no_ordering_allowance() {
    for value in [
        Value::List(vec![]),
        Value::List(vec![Value::Enum(ValueEnum::test_unit(1, 1))]),
        Value::Map(vec![]),
        Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![0; 4096]))]),
    ] {
        let root = request_with_limit(Resource::SortEntries, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            admit_enum_input_construction(&value, work)
        })
        .unwrap();
        assert_eq!(root.observed(Resource::SortEntries), 0);
        assert_eq!(root.observed(Resource::SortComparisons), 0);
    }
}

#[test]
fn canonical_enum_preflight_admits_text_and_record_labels_before_later_payloads() {
    let text = "界".repeat(4);
    let bytes = text.len() as u64;
    for value in [
        Value::Enum(ValueEnum::test_payload(1, 1, Value::Text(text.clone()))),
        Value::Map(vec![(
            Value::Text(text),
            Value::Enum(ValueEnum::test_payload(1, 1, Value::Blob(vec![0; 4096]))),
        )]),
    ] {
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let root = request_with_limit(Resource::PredicateExpressionSteps, bytes - 1);
            let error = PreparationWork::run(&root.scope(), lane, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap_err();
            for fact in [
                (
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                ),
                (DiagnosticFactTag::ExecutionLane, lane.raw()),
                (DiagnosticFactTag::Limit, bytes - 1),
                (DiagnosticFactTag::Actual, bytes),
            ] {
                assert!(error.diagnostic_facts().contains(&fact));
            }
            // Parent and text only: refusal precedes any later enum/blob walk.
            assert_eq!(root.observed(Resource::NestedValueSteps), 4);
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            let root = request_with_limit(Resource::PredicateExpressionSteps, bytes);
            assert!(
                PreparationWork::run(&root.scope(), lane, |work| {
                    admit_enum_input_construction(&value, work)
                })
                .unwrap()
            );
            assert_eq!(root.observed(Resource::PredicateExpressionSteps), bytes);
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        }
    }
}
