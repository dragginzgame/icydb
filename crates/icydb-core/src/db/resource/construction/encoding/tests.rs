//! Canonical map encoding admits ordering without changing bytes or hash identity.

use crate::{
    db::{
        QueryError,
        predicate::CoercionId,
        query::preparation::PreparationWork,
        resource::construction::{ConstructionBudget, encoded_value_capacity},
        tests::support::request_with_limit,
    },
    value::{Value, ValueEnum, fingerprint_value},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn entries() -> Vec<(Value, Value)> {
    vec![
        (Value::Text("k".into()), Value::Text("b".repeat(128))),
        (Value::Text("a".into()), Value::Text("c".repeat(128))),
        (Value::Text("k".into()), Value::Text("a".repeat(128))),
    ]
}

#[test]
fn encoded_map_ordering_and_payloads_have_exact_cumulative_limits() {
    let unordered = entries();
    let ordered = vec![
        unordered[1].clone(),
        unordered[2].clone(),
        unordered[0].clone(),
    ];
    for entries in [unordered, ordered] {
        let value = Value::Map(entries);
        // Three entries: six sort comparisons plus two canonical-input checks.
        // Each entry compares its key and possibly its value. Shared extent:
        // six scalar visits plus sixteen repeated maximum operand extents.
        for (resource, amount) in [
            (Resource::SortEntries, 3),
            (Resource::SortComparisons, 8),
            (Resource::NestedValueSteps, 7 + 12 + 16 * 2),
            (Resource::PredicateExpressionSteps, 3 * 129 + 16 * 128),
        ] {
            assert_capacity_boundary(&value, resource, amount, 7 * 64 + 3 * 129);
        }
    }
}

fn assert_capacity_boundary(value: &Value, resource: Resource, amount: u64, bytes: u64) {
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
                    encoded_value_capacity(value, CoercionId::Strict, work)
                        .map_err(QueryError::execute)
                });
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::SortTemporaryBytes), 0);
                if attempt * amount <= limit {
                    assert_eq!(result.unwrap(), bytes);
                    assert_eq!(root.observed(resource), attempt * amount);
                } else {
                    let error = result.unwrap_err();
                    let actual = root.observed(resource);
                    assert!(actual > limit && actual <= attempt * amount);
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

#[test]
fn nested_map_encoding_accumulates_before_any_hash_or_ordering() {
    let map = Value::Map(entries());
    let repeated = Value::List(vec![map; 2]);
    let value = Value::Enum(ValueEnum::test_payload(1, 1, repeated));
    for (resource, amount) in [
        (Resource::SortEntries, 6),
        (Resource::SortComparisons, 16),
        (Resource::NestedValueSteps, 2 + 2 * 51),
        (Resource::PredicateExpressionSteps, 2 * (3 * 129 + 16 * 128)),
    ] {
        assert_capacity_boundary(&value, resource, amount, 2 * 64 + 2 * (7 * 64 + 3 * 129));
    }
}

#[test]
fn map_hash_preserves_canonical_key_and_value_ties_after_admission() {
    let original = entries();
    let expected = fingerprint_value(&Value::Map(original.clone())).unwrap();
    for entries in [original.clone(), original.into_iter().rev().collect()] {
        let value = Value::Map(entries);
        let before = value.clone();
        let exact = request_with_limit(Resource::SortComparisons, 8);
        let digest = PreparationWork::run(&exact.scope(), Lane::PublicRead, |work| {
            (work as &dyn ConstructionBudget)
                .fingerprint_value(&value)
                .map_err(QueryError::execute)
        })
        .unwrap();
        assert_eq!(digest, expected);
        assert_eq!(value, before);
        let short = request_with_limit(Resource::SortComparisons, 7);
        assert!(
            PreparationWork::run(&short.scope(), Lane::PublicRead, |work| {
                (work as &dyn ConstructionBudget)
                    .fingerprint_value(&value)
                    .map_err(QueryError::execute)
            })
            .is_err()
        );
        assert_eq!(value, before);
    }
}

#[test]
fn empty_and_singleton_map_encoding_skips_ordering_allowances() {
    for entries in [
        vec![],
        vec![(Value::Text("k".into()), Value::Text("v".repeat(4096)))],
    ] {
        for resource in [Resource::SortEntries, Resource::SortComparisons] {
            let root = request_with_limit(resource, 0);
            PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                encoded_value_capacity(&Value::Map(entries.clone()), CoercionId::Strict, work)
                    .map_err(QueryError::execute)
            })
            .unwrap();
            assert_eq!(root.observed(resource), 0);
        }
    }
}
