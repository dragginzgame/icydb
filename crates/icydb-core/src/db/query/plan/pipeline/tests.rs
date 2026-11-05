mod candidate_outputs;
mod residual_filters;

use super::{VisibleIndexes, exact_first_component_metadata_index};
use crate::db::query::plan::exact_metadata_schema;

#[test]
fn empty_access_preserves_reason_precedence() {
    use crate::db::{
        access::AccessPlan,
        predicate::{MissingRowPolicy, Predicate},
        query::{
            intent::QueryModel, plan::PlannedNonIndexAccessReason,
            preparation::with_preparation_work,
        },
    };

    let schema = exact_metadata_schema(&[], &[]);
    for (limit, predicate, reason) in [
        (0, None, PlannedNonIndexAccessReason::LimitZeroWindow),
        (
            0,
            Some(Predicate::False),
            PlannedNonIndexAccessReason::LimitZeroWindow,
        ),
        (
            1,
            Some(Predicate::False),
            PlannedNonIndexAccessReason::ConstantFalsePredicate,
        ),
    ] {
        let query = QueryModel::new(MissingRowPolicy::Ignore).limit(limit);
        let selection = with_preparation_work(|work| {
            super::plan_access_from_normalized_predicate(
                &query,
                &[],
                &schema,
                predicate.as_ref(),
                None,
                work,
            )
        })
        .unwrap();
        let (access, actual_reason) = selection.into_access_and_non_index_reason();
        assert_eq!(access, AccessPlan::by_keys(Vec::new()));
        assert_eq!(actual_reason, Some(reason));
    }
}

#[test]
fn primary_key_summary_preserves_scope_and_propagates_exhaustion() {
    use crate::{
        db::{
            RequestExecutionRoot,
            predicate::Predicate,
            query::preparation::PreparationWork,
            resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        },
        value::Value,
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag,
    };

    let schema = exact_metadata_schema(&[], &[]);
    let predicate = Predicate::And(vec![
        Predicate::in_("id".into(), vec![Value::Int64(1), Value::Int64(2)]),
        Predicate::Or(vec![Predicate::in_("id".into(), vec![Value::Int64(3)])]),
        Predicate::in_("age".into(), vec![Value::Int64(4)]),
    ]);
    // AND + matching compare/name/two values + OR shell + other compare.
    let exact = 8;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for limit in [exact - 1, exact] {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::PredicateExpressionSteps, limit)
                .with_limit_for_tests(Resource::TemporaryBytes, 0),
            );
            let result = PreparationWork::run(&root.scope(), lane, |work| {
                super::primary_key_input_resource_from_predicate(&schema, Some(&predicate), work)
            });
            if limit == exact {
                let summary = result.unwrap().unwrap();
                assert_eq!(summary.raw_term_count(), 2);
                assert_eq!(summary.estimated_payload_bytes(), 16);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                )));
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn residual_reranking_keeps_first_best_candidate_and_preserves_the_source() {
    use crate::{
        db::{
            access::AccessPlan,
            predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
            query::plan::{
                AccessPlannedQuery, LogicalPlan,
                access_choice::rerank_access_plan_by_residual_burden_with_semantic_indexes,
            },
        },
        value::Value,
    };

    for alternative_count in [0, 1, 8, 64] {
        let names = (0..alternative_count)
            .map(|index| format!("b_age_{index:02}"))
            .collect::<Vec<_>>();
        let age_fields = ["age"];
        let mut definitions: Vec<(&str, &[&str])> = vec![("a_rank", &["rank"])];
        definitions.extend(
            names
                .iter()
                .map(|name| (name.as_str(), age_fields.as_slice())),
        );
        let schema = exact_metadata_schema(&definitions, &[]);
        let visible = crate::db::query::preparation::with_preparation_work(|work| {
            VisibleIndexes::accepted_schema_visible(&schema, work)
        })
        .expect("valid accepted index fixture");
        let indexes = visible.accepted_semantic_index_contracts();
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.access =
            AccessPlan::index_prefix_from_contract(indexes[0].clone(), vec![Value::Int64(2)]);
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!("scalar fixture");
        };
        scalar.predicate = Some(Predicate::And(vec![
            Predicate::eq("age".into(), Value::Int64(1)),
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int64(0),
                CoercionId::Strict,
            )),
            Predicate::eq("rank".into(), Value::Int64(2)),
        ]));
        let before = plan.clone();
        for _ in 0..3 {
            let preferred = crate::db::query::preparation::with_preparation_work(|work| {
                rerank_access_plan_by_residual_burden_with_semantic_indexes(
                    indexes, &schema, &plan, work,
                )
                .map_err(crate::db::QueryError::execute)
            })
            .unwrap();
            assert_eq!(
                preferred
                    .as_ref()
                    .and_then(AccessPlan::selected_index_contract)
                    .map(|index| index.name().to_string()),
                (alternative_count > 0).then(|| "b_age_00".to_string()),
            );
            assert_eq!(plan, before);
            if let Some(access) = preferred {
                let mut already_best = plan.clone();
                already_best.access = access;
                assert!(
                    crate::db::query::preparation::with_preparation_work(|work| {
                        rerank_access_plan_by_residual_burden_with_semantic_indexes(
                            indexes,
                            &schema,
                            &already_best,
                            work,
                        )
                        .map_err(crate::db::QueryError::execute)
                    })
                    .unwrap()
                    .is_none()
                );
            }
        }
    }
}

#[test]
fn eligible_index_list_admission_is_upfront_and_cumulative() {
    use crate::db::{
        QueryError, RequestExecutionRoot,
        access::SemanticIndexAccessContract,
        predicate::Predicate,
        query::{plan::planner::eligible_sorted_index_contracts, preparation::PreparationWork},
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag,
    };

    let schema = exact_metadata_schema(
        &[
            ("a_age", &["age"]),
            ("b_age", &["age"]),
            ("c_maybe", &["maybe"]),
        ],
        &["maybe"],
    );
    let visible = crate::db::query::preparation::with_preparation_work(|work| {
        VisibleIndexes::accepted_schema_visible(&schema, work)
    })
    .expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let baseline = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
        16_000_000,
        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
    ));
    PreparationWork::run(&baseline.scope(), Lane::PublicRead, |work| {
        eligible_sorted_index_contracts(indexes, &schema, &Predicate::True, work)
            .map_err(QueryError::execute)
    })
    .unwrap();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let allowance = if resource == Resource::TemporaryBytes {
                std::mem::size_of_val(indexes) as u64
            } else {
                baseline.observed(resource)
            };
            for limit in [allowance - 1, allowance * 2] {
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(resource, limit),
                );
                PreparationWork::run(&root.scope(), lane, |work| {
                    assert!(
                        eligible_sorted_index_contracts(&[], &schema, &Predicate::True, work)
                            .unwrap()
                            .is_empty()
                    );
                    for attempt in 0..3 {
                        let result = eligible_sorted_index_contracts(
                            indexes,
                            &schema,
                            &Predicate::True,
                            work,
                        );
                        if limit >= allowance && attempt < 2 {
                            let eligible = result.unwrap();
                            assert_eq!(
                                eligible
                                    .iter()
                                    .map(SemanticIndexAccessContract::name)
                                    .collect::<Vec<_>>(),
                                ["a_age", "b_age"]
                            );
                            assert_eq!(eligible.capacity(), indexes.len());
                        } else {
                            let error = QueryError::execute(result.unwrap_err());
                            assert!(
                                error
                                    .diagnostic_facts()
                                    .contains(
                                        &(DiagnosticFactTag::BudgetResource, resource.raw(),)
                                    )
                            );
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
fn candidate_exhaustion_propagates_without_fallback_or_snapshot_publication() {
    use crate::{
        db::{
            QueryError, RequestExecutionRoot,
            access::AccessPlan,
            predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
            query::{
                plan::{
                    AccessPlannedQuery, LogicalPlan,
                    access_choice::{
                        exact_cardinality_tiebreak_candidates,
                        rerank_access_plan_by_residual_burden_with_semantic_indexes,
                    },
                },
                preparation::PreparationWork,
            },
            resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        },
        value::Value,
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag,
    };

    let schema = exact_metadata_schema(&[("a_age", &["age"]), ("z_rank", &["rank"])], &[]);
    let visible = crate::db::query::preparation::with_preparation_work(|work| {
        VisibleIndexes::accepted_schema_visible(&schema, work)
    })
    .expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    plan.access = AccessPlan::index_prefix_from_contract(indexes[1].clone(), vec![Value::Int64(2)]);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!("scalar fixture");
    };
    scalar.predicate = Some(Predicate::And(vec![
        Predicate::eq("age".into(), Value::Int64(1)),
        Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Int64(0),
            CoercionId::Strict,
        )),
        Predicate::eq("rank".into(), Value::Int64(2)),
    ]));
    let before = plan.clone();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(resource, 0),
            );
            PreparationWork::run(&root.scope(), lane, |work| {
                let errors = [
                    rerank_access_plan_by_residual_burden_with_semantic_indexes(
                        indexes, &schema, &plan, work,
                    )
                    .unwrap_err(),
                    exact_cardinality_tiebreak_candidates(indexes, &schema, &plan, work)
                        .unwrap_err(),
                    plan.finalize_access_choice_with_semantic_indexes_and_schema(
                        indexes, &schema, work,
                    )
                    .unwrap_err(),
                ];
                for error in errors {
                    assert!(
                        QueryError::execute(error)
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                }
                assert_eq!(plan, before);
                Ok(())
            })
            .unwrap();
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn exact_metadata_index_selection_is_complete_deterministic_and_visibility_bound() {
    let schema = exact_metadata_schema(
        &[
            ("z_age_id", &["age", "id"]),
            ("long_age_rank_id", &["age", "rank", "id"]),
            ("a_age_rank", &["age", "rank"]),
        ],
        &[],
    );
    let visible = crate::db::query::preparation::with_preparation_work(|work| {
        VisibleIndexes::accepted_schema_visible(&schema, work)
    })
    .expect("valid accepted index fixture");

    assert_eq!(
        visible
            .accepted_semantic_index_contracts()
            .iter()
            .map(crate::db::access::SemanticIndexAccessContract::name)
            .collect::<Vec<_>>(),
        ["a_age_rank", "long_age_rank_id", "z_age_id"],
        "visible semantic contracts must establish canonical name order once",
    );
    assert_eq!(
        exact_first_component_metadata_index(&visible, &schema, "age")
            .map(|index| index.name().to_string()),
        Some("a_age_rank".to_string()),
        "shortest arity and then stable name must be the sole selection rule",
    );
    assert!(
        exact_first_component_metadata_index(&VisibleIndexes::none(), &schema, "age").is_none(),
        "store-not-ready visibility must not expose a metadata target",
    );
}

#[test]
fn exact_metadata_index_selection_rejects_nullable_compound_suffixes() {
    let schema = exact_metadata_schema(&[("age_maybe", &["age", "maybe"])], &["maybe"]);
    let visible = crate::db::query::preparation::with_preparation_work(|work| {
        VisibleIndexes::accepted_schema_visible(&schema, work)
    })
    .expect("valid accepted index fixture");

    assert!(exact_first_component_metadata_index(&visible, &schema, "age").is_none());
}
