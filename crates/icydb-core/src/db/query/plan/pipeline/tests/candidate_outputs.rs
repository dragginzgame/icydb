//! Candidate output lists and names are admitted before retained construction.

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{AccessPath, AccessPlan, SemanticIndexAccessContract},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{MissingRowPolicy, Predicate},
        query::{
            plan::{
                AccessPlannedQuery, CardinalityTiebreakCandidate, LogicalPlan, VisibleIndexes,
                access_choice::{
                    AccessChoiceCandidateExplainSummary, AccessChoiceRejectedIndex,
                    exact_cardinality_tiebreak_candidates,
                },
                exact_metadata_schema,
            },
            preparation::PreparationWork,
        },
        schema::SchemaInfo,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn fixture() -> (SchemaInfo, VisibleIndexes, AccessPlannedQuery) {
    let schema = exact_metadata_schema(
        &[
            ("a_âge", &["age"]),
            ("b_age", &["age"]),
            ("z_rank", &["rank"]),
        ],
        &[],
    );
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    plan.access = AccessPlan::index_prefix_from_contract(
        visible.accepted_semantic_index_contracts()[0].clone(),
        vec![Value::Int64(1)],
    );
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!("scalar fixture");
    };
    scalar.predicate = Some(Predicate::eq("age".into(), Value::Int64(1)));
    (schema, visible, plan)
}

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn assert_resource(error: crate::error::InternalError, resource: Resource) {
    assert!(
        QueryError::execute(error)
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
    );
}

#[test]
fn ordered_range_selection_admits_one_operand_slots_and_path() {
    use crate::db::query::plan::planner::{
        PlannerError, plan_access_selection_with_order_and_semantic_indexes,
    };
    use std::ops::Bound;

    let schema = exact_metadata_schema(
        &[
            ("z_age", &["age"]),
            ("a_age", &["age"]),
            ("a_rank", &["rank"]),
        ],
        &[],
    );
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let value = Value::Int64(3);
    let cases = [
        (
            Predicate::gt("age".into(), value.clone()),
            Bound::Excluded(value.clone()),
            Bound::Unbounded,
        ),
        (
            Predicate::gte("age".into(), value.clone()),
            Bound::Included(value.clone()),
            Bound::Unbounded,
        ),
        (
            Predicate::lt("age".into(), value.clone()),
            Bound::Unbounded,
            Bound::Excluded(value.clone()),
        ),
        (
            Predicate::lte("age".into(), value.clone()),
            Bound::Unbounded,
            Bound::Included(value),
        ),
    ];
    let bytes = (std::mem::size_of_val(indexes)
        + size_of::<usize>()
        + size_of::<AccessPath<Value>>()) as u64;
    // Candidate visits plus completeness checks of every accepted key component.
    let steps = 1
        + 2 * indexes.len() as u64
        + indexes
            .iter()
            .map(|index| index.key_arity() as u64)
            .sum::<u64>();
    for (predicate, lower, upper) in cases {
        let before = predicate.clone();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::PredicateExpressionSteps, steps),
                (Resource::NestedValueSteps, 1),
            ] {
                for limit in [0, exact - 1, exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 0..3 {
                            let result = plan_access_selection_with_order_and_semantic_indexes(
                                indexes,
                                &schema,
                                Some(&predicate),
                                None,
                                false,
                                work,
                            );
                            if limit >= exact && attempt < 2 {
                                let (access, _) =
                                    result.unwrap().into_access_and_non_index_reason();
                                let spec = access.as_index_range_path().unwrap();
                                assert_eq!(spec.index_ref().name(), "a_age");
                                assert_eq!(spec.field_slots(), &[0]);
                                assert!(spec.prefix_values().is_empty());
                                assert_eq!(spec.lower(), &lower);
                                assert_eq!(spec.upper(), &upper);
                                assert_eq!(root.observed(resource), exact * (attempt + 1));
                            } else {
                                let PlannerError::Internal(error) = result.unwrap_err() else {
                                    panic!("range construction exhaustion must remain typed");
                                };
                                assert_resource(*error, resource);
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(predicate, before);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn secondary_lookup_selection_admits_only_one_output_list_and_path() {
    use crate::db::query::plan::planner::{
        PlannerError, plan_access_selection_with_order_and_semantic_indexes,
    };

    let schema = exact_metadata_schema(
        &[
            ("a_composite", &["age", "rank"]),
            ("b_exact", &["age"]),
            ("c_exact", &["age"]),
            ("z_rank", &["rank"]),
        ],
        &[],
    );
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let cases = [
        (Predicate::eq("age".into(), Value::Int64(3)), 1),
        (
            Predicate::in_(
                "age".into(),
                vec![Value::Int64(3), Value::Int64(1), Value::Int64(3)],
            ),
            3,
        ),
    ];
    for (predicate, count) in cases {
        let before = predicate.clone();
        let bytes = (std::mem::size_of_val(indexes)
            + count * size_of::<Value>()
            + size_of::<AccessPath<Value>>()) as u64;
        let steps = 1
            + if count == 1 { 0 } else { count as u64 }
            + 2 * indexes.len() as u64
            + indexes
                .iter()
                .map(|index| index.key_arity() as u64)
                .sum::<u64>();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::PredicateExpressionSteps, steps),
                (Resource::NestedValueSteps, count as u64),
            ] {
                for limit in [0, exact - 1, exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 0..3 {
                            let result = plan_access_selection_with_order_and_semantic_indexes(
                                indexes,
                                &schema,
                                Some(&predicate),
                                None,
                                false,
                                work,
                            );
                            if limit >= exact && attempt < 2 {
                                let (access, _) =
                                    result.unwrap().into_access_and_non_index_reason();
                                assert_eq!(
                                    access.selected_index_contract().unwrap().name(),
                                    "b_exact"
                                );
                                if count == 1 {
                                    assert_eq!(
                                        access.as_index_prefix_contract_path().unwrap().1,
                                        &[Value::Int64(3)]
                                    );
                                } else {
                                    assert_eq!(
                                        access.as_index_multi_lookup_contract_path().unwrap().1,
                                        &[Value::Int64(1), Value::Int64(3)]
                                    );
                                }
                                assert_eq!(root.observed(resource), exact * (attempt + 1));
                            } else {
                                let PlannerError::Internal(error) = result.unwrap_err() else {
                                    panic!("construction exhaustion must remain typed");
                                };
                                assert_resource(*error, resource);
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(predicate, before);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn primary_key_candidate_exhaustion_propagates_without_fallback() {
    use crate::db::query::plan::planner::{
        PlannerError, plan_access_selection_with_order_and_semantic_indexes,
    };

    let schema = exact_metadata_schema(&[], &[]);
    let cases = [
        (Predicate::eq("id".into(), Value::Int64(7)), 1),
        (
            Predicate::in_(
                "id".into(),
                vec![Value::Int64(3), Value::Int64(1), Value::Int64(3)],
            ),
            3,
        ),
    ];
    for (predicate, visits) in cases {
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for resource in [Resource::TemporaryBytes, Resource::NestedValueSteps] {
                for limit in [0, 16_000_000] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        let result = plan_access_selection_with_order_and_semantic_indexes(
                            &[],
                            &schema,
                            Some(&predicate),
                            None,
                            false,
                            work,
                        );
                        if limit == 0 {
                            let PlannerError::Internal(error) = result.unwrap_err() else {
                                panic!("copy exhaustion must not become semantic absence");
                            };
                            assert_resource(*error, resource);
                        } else {
                            let (access, _) = result.unwrap().into_access_and_non_index_reason();
                            let path = access.as_path().unwrap();
                            if visits == 1 {
                                assert_eq!(path.as_by_key(), Some(&Value::Int64(7)));
                            } else {
                                // The existing normalizer, not copying, owns IN order/dedup.
                                assert_eq!(
                                    path.as_by_keys().unwrap(),
                                    &[Value::Int64(1), Value::Int64(3)]
                                );
                            }
                            assert_eq!(root.observed(Resource::NestedValueSteps), visits);
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn and_range_construction_admission_precedes_child_recursion() {
    use crate::db::query::plan::planner::{
        PlannerError, plan_access_selection_with_order_and_semantic_indexes,
    };

    let schema = exact_metadata_schema(&[("age_idx", &["age"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let predicate = Predicate::And(vec![
        Predicate::gte("age".into(), Value::Int64(2)),
        Predicate::lt("age".into(), Value::Int64(5)),
    ]);
    let bytes = (std::mem::size_of_val(indexes) + 2 * size_of::<AccessPlan<Value>>()) as u64;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let root = request(Resource::TemporaryBytes, bytes);
        PreparationWork::run(&root.scope(), lane, |work| {
            let result = plan_access_selection_with_order_and_semantic_indexes(
                indexes,
                &schema,
                Some(&predicate),
                None,
                false,
                work,
            );
            let PlannerError::Internal(error) = result.unwrap_err() else {
                panic!("range construction admission must remain typed")
            };
            assert_resource(*error, Resource::TemporaryBytes);
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn recursive_candidate_lists_and_dispatch_obey_request_admission() {
    use crate::db::query::plan::planner::{
        PlannerError, plan_access_selection_with_order_and_semantic_indexes,
    };

    let schema = exact_metadata_schema(&[], &[]);
    for width in [0, 1, 16, 128] {
        let children = vec![
            Predicate::IsMissing {
                field: "age".into()
            };
            width
        ];
        let shapes = [
            (Predicate::And(children.clone()), width, 7 * width + 1),
            (Predicate::Or(children.clone()), width, width + 1),
            (
                Predicate::Or(vec![Predicate::And(children)]),
                width + 1,
                7 * width + 2,
            ),
        ];
        for (predicate, slots, steps) in shapes {
            let before = predicate.clone();
            let bytes = (slots * size_of::<AccessPlan<Value>>()) as u64;
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, exact) in [
                    (Resource::TemporaryBytes, bytes),
                    (Resource::PredicateExpressionSteps, steps as u64),
                ] {
                    for limit in [0, exact.saturating_sub(1), exact * 2] {
                        let root = request(resource, limit);
                        PreparationWork::run(&root.scope(), lane, |work| {
                            for attempt in 0..3 {
                                let result = plan_access_selection_with_order_and_semantic_indexes(
                                    &[], &schema, Some(&predicate), None, false, work,
                                );
                                if exact == 0 || limit >= exact && attempt < 2 {
                                    let (access, _) = result.unwrap().into_access_and_non_index_reason();
                                    assert!(access.is_single_full_scan());
                                } else {
                                    let PlannerError::Internal(error) = result.unwrap_err() else {
                                        panic!("construction exhaustion must remain an internal resource error");
                                    };
                                    assert_resource(*error, resource);
                                    break;
                                }
                            }
                            Ok(())
                        })
                        .unwrap();
                        assert_eq!(predicate, before);
                        assert_eq!(root.observed(Resource::RowsVisited), 0);
                        // A failed root dispatch cannot allocate its child list;
                        // a failed list reservation cannot visit its children.
                        if limit == 0 && exact != 0 {
                            let (other, expected) = match resource {
                                Resource::TemporaryBytes => (Resource::PredicateExpressionSteps, 1),
                                Resource::PredicateExpressionSteps => (Resource::TemporaryBytes, 0),
                                _ => unreachable!("fixture resources"),
                            };
                            assert_eq!(root.observed(other), expected);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn and_branch_lists_propagate_cumulative_admission_and_preserve_selection() {
    use crate::db::query::plan::planner::{
        PlannerError, plan_access_selection_with_order_and_semantic_indexes,
    };

    let schema = exact_metadata_schema(&[("by_age_rank", &["age", "rank"])], &[]);
    let visible = VisibleIndexes::accepted_schema_visible(&schema).expect("valid indexes");
    for remaining in [1, 2] {
        let predicate = Predicate::And(vec![
            Predicate::eq("age".into(), Value::Int64(7)),
            Predicate::in_(
                "rank".into(),
                vec![
                    Value::Int64(3),
                    Value::Int64(2),
                    Value::Int64(1),
                    Value::Int64(1),
                ],
            ),
            Predicate::ne("rank".into(), Value::Int64(3)),
            Predicate::not_in(
                "rank".into(),
                if remaining == 1 {
                    vec![Value::Int64(2)]
                } else {
                    Vec::new()
                },
            ),
        ]);
        let baseline = request(Resource::TemporaryBytes, 16_000_000);
        let expected = PreparationWork::run(&baseline.scope(), Lane::Diagnostic, |work| {
            Ok(plan_access_selection_with_order_and_semantic_indexes(
                visible.accepted_semantic_index_contracts(),
                &schema,
                Some(&predicate),
                None,
                false,
                work,
            )
            .unwrap()
            .into_access_and_non_index_reason()
            .0)
        })
        .unwrap();
        if remaining == 1 {
            assert_eq!(
                expected.as_index_prefix_contract_path().unwrap().1,
                &[Value::Int64(7), Value::Int64(1)]
            );
        } else {
            assert_eq!(
                expected
                    .as_path()
                    .unwrap()
                    .as_index_branch_set_spec()
                    .unwrap()
                    .branch_values(),
                &[Value::Int64(1), Value::Int64(2)]
            );
        }
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for resource in [
                Resource::TemporaryBytes,
                Resource::PredicateExpressionSteps,
                Resource::NestedValueSteps,
            ] {
                // Owner tests pin exact operand charges; this covers the whole
                // collector/candidate route and cumulative error propagation.
                let exact = baseline.observed(resource);
                assert!(exact > 0);
                for limit in [0, exact - 1, exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 1..=3 {
                            let result = plan_access_selection_with_order_and_semantic_indexes(
                                visible.accepted_semantic_index_contracts(),
                                &schema,
                                Some(&predicate),
                                None,
                                false,
                                work,
                            );
                            if attempt * exact <= limit {
                                assert_eq!(
                                    result.unwrap().into_access_and_non_index_reason().0,
                                    expected
                                );
                                assert_eq!(root.observed(resource), attempt * exact);
                            } else {
                                let PlannerError::Internal(error) = result.unwrap_err() else {
                                    panic!("typed construction failure expected");
                                };
                                assert_resource(*error, resource);
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn candidate_snapshot_lists_and_names_obey_exact_and_cumulative_admission() {
    let (schema, visible, plan) = fixture();
    let indexes = visible.accepted_semantic_index_contracts();
    let list_bytes = indexes.len()
        * (size_of::<AccessChoiceCandidateExplainSummary>()
            + size_of::<String>()
            + size_of::<AccessChoiceRejectedIndex>());
    let name_bytes = "a_âge".len() + 3 * "b_age".len() + "z_rank".len();
    let total_bytes = (list_bytes
        + name_bytes
        + 2 * "age".len()
        + size_of::<SemanticIndexAccessContract>()
        + size_of::<Value>()
        + size_of::<AccessPath<Value>>()) as u64;
    // Proof-owner tests pin traversal admission. This integration check includes
    // that work without duplicating the proof's internal visit formula.
    let baseline = request(Resource::PredicateExpressionSteps, 16_000_000);
    PreparationWork::run(&baseline.scope(), Lane::Diagnostic, |work| {
        plan.clone()
            .finalize_access_choice_with_semantic_indexes_and_schema(indexes, &schema, work)
            .map_err(QueryError::execute)
    })
    .unwrap();
    let total_steps = baseline.observed(Resource::PredicateExpressionSteps);
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (resource, total) in [
            (Resource::TemporaryBytes, total_bytes),
            (Resource::PredicateExpressionSteps, total_steps),
        ] {
            for limit in [0, total - 1, total * 2] {
                let root = request(resource, limit);
                let mut current = plan.clone();
                PreparationWork::run(&root.scope(), lane, |work| {
                    for attempt in 0..3 {
                        let before = current.clone();
                        let result = current
                            .finalize_access_choice_with_semantic_indexes_and_schema(
                                indexes, &schema, work,
                            );
                        if limit >= total && attempt < 2 {
                            result.unwrap();
                            assert_eq!(
                                current
                                    .access_choice
                                    .candidates
                                    .iter()
                                    .map(AccessChoiceCandidateExplainSummary::index_name)
                                    .collect::<Vec<_>>(),
                                ["a_âge", "b_age"]
                            );
                            assert_eq!(current.access_choice.alternatives, ["b_age"]);
                            assert_eq!(
                                current
                                    .access_choice
                                    .rejected
                                    .iter()
                                    .map(AccessChoiceRejectedIndex::index_name)
                                    .collect::<Vec<_>>(),
                                ["b_age", "z_rank"]
                            );
                            assert_eq!(root.observed(resource), (attempt + 1) * total);
                        } else {
                            assert_resource(result.unwrap_err(), resource);
                            assert_eq!(current, before);
                            break;
                        }
                    }
                    Ok(())
                })
                .unwrap();
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn cardinality_candidate_list_is_admitted_before_route_copies() {
    let (schema, visible, plan) = fixture();
    let indexes = visible.accepted_semantic_index_contracts();
    let list_bytes = (indexes.len() * size_of::<CardinalityTiebreakCandidate>()) as u64;
    let residual_bytes = "age".len() as u64;
    let total = list_bytes
        + 2 * residual_bytes
        + (size_of::<SemanticIndexAccessContract>()
            + size_of::<Value>()
            + size_of::<AccessPath<Value>>()) as u64;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        // The chosen residual precedes list admission; the alternative's
        // route and residual cannot be copied until that list is admitted.
        for limit in [residual_bytes + list_bytes - 1, total] {
            let root = request(Resource::TemporaryBytes, limit);
            PreparationWork::run(&root.scope(), lane, |work| {
                let result = exact_cardinality_tiebreak_candidates(indexes, &schema, &plan, work);
                if limit < residual_bytes + list_bytes {
                    assert_resource(result.unwrap_err(), Resource::TemporaryBytes);
                    // One residual copy plus its two-visit comparison extent;
                    // the alternative's route/value work has not started.
                    assert_eq!(root.observed(Resource::NestedValueSteps), 3);
                } else {
                    assert_eq!(result.unwrap().unwrap().len(), 2);
                    assert_eq!(root.observed(Resource::TemporaryBytes), total);
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn cardinality_candidates_borrow_the_current_route_and_move_alternatives() {
    use crate::db::query::{
        plan::CardinalityTiebreakFamily as Family, preparation::with_preparation_work,
    };

    for family in [Family::Prefix, Family::MultiLookup, Family::BranchSet] {
        let fields: &[&str] = if family == Family::BranchSet {
            &["age", "rank"]
        } else {
            &["age"]
        };
        let schema = exact_metadata_schema(&[("a_index", fields), ("b_index", fields)], &[]);
        let visible =
            VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
        let indexes = visible.accepted_semantic_index_contracts();
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        let (access, predicate) = match family {
            Family::Prefix => (
                AccessPlan::index_prefix_from_contract(indexes[0].clone(), vec![Value::Int64(1)]),
                Predicate::eq("age".into(), Value::Int64(1)),
            ),
            Family::MultiLookup => (
                AccessPlan::index_multi_lookup_from_contract(
                    indexes[0].clone(),
                    vec![Value::Int64(1), Value::Int64(2)],
                ),
                Predicate::in_("age".into(), vec![Value::Int64(1), Value::Int64(2)]),
            ),
            Family::BranchSet => (
                AccessPlan::index_branch_set_from_contract(
                    indexes[0].clone(),
                    vec![Value::Int64(1)],
                    vec![Value::Int64(2), Value::Int64(3)],
                ),
                Predicate::And(vec![
                    Predicate::eq("age".into(), Value::Int64(1)),
                    Predicate::in_("rank".into(), vec![Value::Int64(2), Value::Int64(3)]),
                ]),
            ),
        };
        plan.access = access;
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!("scalar fixture");
        };
        scalar.predicate = Some(predicate);
        let before = plan.clone();
        for _ in 0..3 {
            let candidates = with_preparation_work(|work| {
                exact_cardinality_tiebreak_candidates(indexes, &schema, &plan, work)
            })
            .unwrap()
            .unwrap();
            assert_eq!(candidates.len(), 2, "{family:?}");
            for (position, candidate) in candidates.into_iter().enumerate() {
                if position == 0 {
                    assert!(std::ptr::eq(candidate.access(), &raw const plan.access));
                    assert!(candidate.into_replacement_access().is_none());
                } else {
                    let path = std::ptr::from_ref(candidate.access().as_path().unwrap());
                    let replacement = candidate.into_replacement_access().unwrap();
                    assert!(std::ptr::eq(path, replacement.as_path().unwrap()));
                    assert_eq!(
                        replacement.selected_index_contract().unwrap().name(),
                        "b_index"
                    );
                }
            }
            assert_eq!(plan, before);
        }
    }
}
