//! Canonical ordering must admit scratch and repeated encoding before sorting.

use crate::{
    db::{
        QueryError,
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, normalize, normalize_with_budget,
        },
        query::preparation::PreparationWork,
        test_support::request_with_limit,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn predicates() -> [Predicate; 2] {
    [
        Predicate::Or(vec![
            Predicate::IsNull { field: "z".into() },
            Predicate::IsNull {
                field: "界".repeat(128),
            },
            Predicate::IsNull { field: "a".into() },
        ]),
        Predicate::Or(
            ["left", "right"]
                .map(|field| {
                    Predicate::Compare(ComparePredicate::with_coercion(
                        field,
                        CompareOp::In,
                        Value::List(vec![
                            Value::Text("İ".into()),
                            Value::Text("A".into()),
                            Value::Text("İ".into()),
                        ]),
                        CoercionId::TextCasefold,
                    ))
                })
                .into(),
        ),
    ]
}

#[test]
fn sorting_resources_are_exact_cumulative_and_preserve_canonical_output() {
    for predicate in predicates() {
        for resource in [
            Resource::TemporaryBytes,
            Resource::SortEntries,
            Resource::SortComparisons,
            Resource::SortTemporaryBytes,
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
        ] {
            assert_resource_boundary(&predicate, resource);
        }
    }
}

fn assert_resource_boundary(predicate: &Predicate, resource: Resource) {
    let expected = normalize(predicate.clone());
    let baseline = request_with_limit(resource, u64::MAX);
    let actual = PreparationWork::run(&baseline.scope(), Lane::PublicRead, |work| {
        normalize_with_budget(predicate.clone(), work).map_err(QueryError::execute)
    })
    .unwrap();
    assert_eq!(actual, expected);
    let amount = baseline.observed(resource);
    if amount == 0 {
        return;
    }
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
                    normalize_with_budget(predicate.clone(), work).map_err(QueryError::execute)
                });
                if attempt * amount <= limit {
                    assert_eq!(result.unwrap(), expected);
                    assert_eq!(root.observed(resource), attempt * amount);
                    continue;
                }
                let error = result.unwrap_err();
                let actual = root.observed(resource);
                assert!(actual > limit);
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

#[test]
fn sort_refusal_preserves_authored_children_before_canonical_ordering() {
    for predicate in predicates() {
        let Predicate::Or(mut children) = predicate else {
            unreachable!()
        };
        let before = children.clone();
        for resource in [Resource::SortEntries, Resource::TemporaryBytes] {
            let root = request_with_limit(resource, 0);
            let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                crate::db::predicate::normalize::sort::canonicalize_with_budget(&mut children, work)
                    .map_err(QueryError::execute)
            });
            assert!(result.is_err());
            assert_eq!(children, before);
        }
    }
}
