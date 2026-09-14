//! Exact intersection selection borrows until at least two distinct prefixes qualify.

use super::exact_index_intersection_candidate;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{AccessPath, AccessPlan, SemanticIndexAccessContract},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::{
                OrderDirection, OrderSpec, OrderTerm, VisibleIndexes,
                pipeline::tests::exact_metadata_schema,
            },
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn prefix(index: &SemanticIndexAccessContract, values: &[i64]) -> AccessPlan<Value> {
    AccessPlan::index_prefix_from_contract(
        index.clone(),
        values.iter().map(|value| Value::Int64(*value)).collect(),
    )
}

#[test]
fn exact_intersection_admits_only_retained_prefixes_and_preserves_order_and_cap() {
    let schema = exact_metadata_schema(
        &[
            ("a", &["age", "id"]),
            ("b", &["rank", "id"]),
            ("c", &["maybe", "id"]),
            ("d", &["age", "rank", "id"]),
        ],
        &[],
    );
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let selected = prefix(&indexes[0], &[7]);
    let second = prefix(&indexes[1], &[8]);
    let third = prefix(&indexes[2], &[9]);
    let fourth = prefix(&indexes[3], &[10, 11]);
    for (selected, children, expected, operands) in [
        (
            &selected,
            vec![
                AccessPlan::full_scan(),
                prefix(&indexes[0], &[99]),
                second.clone(),
                third.clone(),
                fourth.clone(),
            ],
            vec![selected.clone(), second.clone(), third],
            3,
        ),
        (
            &selected,
            vec![second.clone()],
            vec![selected.clone(), second.clone()],
            2,
        ),
        (
            &fourth,
            vec![second.clone()],
            vec![fourth.clone(), second],
            3,
        ),
    ] {
        let before = children.clone();
        let count = expected.len();
        let bytes = (count * (size_of::<AccessPlan<Value>>() + size_of::<AccessPath<Value>>())
            + operands * size_of::<Value>()) as u64;
        let steps = 5 * (children.len() as u64 + 1);
        // The existing constructor agrees with the already-flat emitted shape.
        let expected = AccessPlan::intersection(expected);
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::PredicateExpressionSteps, steps),
                (Resource::NestedValueSteps, operands as u64),
            ] {
                for limit in [0, exact - 1, exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 1..=3 {
                            let result = exact_index_intersection_candidate(
                                &schema,
                                None,
                                false,
                                Some(selected),
                                &children,
                                work,
                            );
                            if attempt * exact <= limit {
                                assert_eq!(result.unwrap().unwrap(), expected);
                                assert_eq!(root.observed(resource), attempt * exact);
                            } else {
                                assert!(
                                    QueryError::execute(result.unwrap_err())
                                        .diagnostic_facts()
                                        .contains(&(
                                            DiagnosticFactTag::BudgetResource,
                                            resource.raw()
                                        ))
                                );
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(children, before);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn absent_exact_intersections_do_not_copy_selected_or_rejected_operands() {
    let schema = exact_metadata_schema(
        &[("a", &["age", "id"]), ("wrong_suffix", &["id", "age"])],
        &[],
    );
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let selected = prefix(&indexes[0], &[1]);
    let full_scan = AccessPlan::full_scan();
    let wrong = prefix(&indexes[1], &[2]);
    let empty = prefix(&indexes[0], &[]);
    let fully_bound = prefix(&indexes[0], &[1, 2]);
    for (selected, children) in [
        (None, vec![selected.clone()]),
        (Some(&selected), Vec::new()),
        (
            Some(&selected),
            vec![prefix(&indexes[0], &[99]), wrong.clone(), full_scan.clone()],
        ),
        (Some(&wrong), vec![selected.clone()]),
        (Some(&empty), vec![selected.clone()]),
        (Some(&fully_bound), vec![selected.clone()]),
        (Some(&full_scan), vec![selected.clone()]),
    ] {
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert!(
                exact_index_intersection_candidate(&schema, None, false, selected, &children, work)
                    .unwrap()
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    }
}

#[test]
fn exact_intersection_keeps_order_and_grouping_admission() {
    let schema = exact_metadata_schema(&[("a", &["age", "id"]), ("b", &["rank", "id"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let selected = prefix(&indexes[0], &[1]);
    let children = [prefix(&indexes[1], &[2])];
    for field in ["id", "age"] {
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            for grouped in [false, true] {
                let order = OrderSpec {
                    fields: vec![OrderTerm::field(field, direction)],
                };
                let eligible = field == "id" && !grouped;
                let root = request(
                    Resource::TemporaryBytes,
                    if eligible { 16_000_000 } else { 0 },
                );
                PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                    assert_eq!(
                        exact_index_intersection_candidate(
                            &schema,
                            Some(&order),
                            grouped,
                            Some(&selected),
                            &children,
                            work
                        )
                        .unwrap()
                        .is_some(),
                        eligible
                    );
                    Ok(())
                })
                .unwrap();
                if !eligible {
                    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
                    assert_eq!(root.observed(Resource::NestedValueSteps), 0);
                }
            }
        }
    }
}
