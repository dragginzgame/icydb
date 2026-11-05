//! Membership ordering shares cumulative admission without changing set values.

use super::canonical_value_set_list;
use crate::{
    db::{query::preparation::PreparationWork, tests::support::request_with_limit},
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn membership_ordering_is_exact_cumulative_and_typed() {
    for values in [vec![3, 1, 3], vec![1, 3, 5]] {
        let expected = if values[0] == 3 {
            vec![1, 3]
        } else {
            values.clone()
        };
        let expected = Value::List(expected.into_iter().map(Value::Nat64).collect());
        assert_admission(
            &values.iter().copied().map(Value::Nat64).collect::<Vec<_>>(),
            &expected,
            &[(Resource::SortEntries, 3), (Resource::SortComparisons, 10)],
        );
    }
}

#[test]
fn empty_and_singleton_membership_need_no_ordering_admission() {
    for values in [
        vec![],
        vec![Value::Nat64(7)],
        vec![Value::Text("λ".repeat(1024))],
    ] {
        for resource in [
            Resource::SortEntries,
            Resource::SortComparisons,
            Resource::NestedValueSteps,
            Resource::PredicateExpressionSteps,
        ] {
            let root = request_with_limit(resource, 0);
            let actual = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                canonical_value_set_list(values.clone(), &mut |resource, amount| {
                    work.charge(resource, amount)
                })
            })
            .unwrap();
            assert_eq!(actual, Value::List(values.clone()));
            assert_eq!(root.observed(Resource::SortEntries), 0);
            assert_eq!(root.observed(Resource::SortComparisons), 0);
        }
    }
}

fn assert_admission(values: &[Value], expected: &Value, charges: &[(Resource, u64)]) {
    for &(resource, amount) in charges {
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            for limit in [amount - 1, amount, amount * 2] {
                let root = request_with_limit(resource, limit);
                for attempt in 1..=3 {
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        canonical_value_set_list(values.to_vec(), &mut |resource, amount| {
                            work.charge(resource, amount)
                        })
                    });
                    assert_eq!(root.observed(Resource::SortTemporaryBytes), 0);
                    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                    if attempt * amount <= limit {
                        assert_eq!(result.unwrap(), *expected);
                        assert_eq!(root.observed(resource), attempt * amount);
                    } else {
                        let error = result.unwrap_err();
                        let actual = root.observed(resource);
                        assert!(actual > limit && actual <= attempt * amount);
                        if matches!(resource, Resource::SortEntries | Resource::SortComparisons) {
                            assert_eq!(actual, attempt * amount);
                        }
                        for fact in [
                            (DiagnosticFactTag::BudgetResource, resource.raw()),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                            (DiagnosticFactTag::Actual, actual),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                        break;
                    }
                }
            }
        }
    }
}

#[test]
fn set_payload_comparison_allowance_covers_nested_values_and_duplicate_scans() {
    for (value, bytes, nodes) in [
        (Value::Text("λ".repeat(128)), 256, 2),
        (Value::Blob(vec![1; 256]), 256, 2),
        (
            Value::List(vec![Value::Text("k".into()), Value::Blob(vec![1; 256])]),
            257,
            6,
        ),
        (
            Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1; 256]))]),
            257,
            6,
        ),
        (
            Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1; 256]))]),
            )),
            257,
            8,
        ),
    ] {
        // Three admission walks plus the existing ten-comparison allowance.
        assert_admission(
            &vec![value.clone(); 3],
            &Value::List(vec![value]),
            &[
                (Resource::NestedValueSteps, 13 * nodes),
                (Resource::PredicateExpressionSteps, 13 * bytes),
            ],
        );
    }
}

#[test]
fn set_payload_allowance_uses_largest_operand_and_preserves_order_fast_path() {
    let a = Value::Text("a".repeat(64));
    let b = Value::Text("b".into());
    let c = Value::Text("c".repeat(32));
    let expected = Value::List(vec![a.clone(), b.clone(), c.clone()]);
    for values in [
        vec![a.clone(), b.clone(), c.clone()],
        vec![c, a.clone(), b.clone()],
    ] {
        assert_admission(
            &values,
            &expected,
            &[
                (Resource::NestedValueSteps, 26),
                // Admission visits each operand once; repetitions use the maximum,
                // not their sum, and do not require uniform payload sizes.
                (Resource::PredicateExpressionSteps, 64 + 1 + 32 + 10 * 64),
            ],
        );
    }
    assert_admission(
        &[a.clone(), b.clone(), a.clone()],
        &Value::List(vec![a, b]),
        &[
            (Resource::NestedValueSteps, 26),
            (Resource::PredicateExpressionSteps, 64 + 1 + 64 + 10 * 64),
        ],
    );
}
