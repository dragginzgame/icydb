use super::{residual_burden_for_candidate, residual_burden_for_plan};
use crate::db::{
    QueryError, RequestExecutionRoot,
    access::AccessPlan,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::{MissingRowPolicy, Predicate},
    query::plan::{
        AccessPlannedQuery, LogicalPlan, VisibleIndexes, exact_metadata_schema,
        expr::{Expr, FieldId, FieldPath, ProjectionSelection, ProjectionSpec},
    },
    query::preparation::{PreparationWork, with_preparation_work},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn candidate_fixture(width: usize) -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!("scalar fixture");
    };
    scalar.predicate = Some(Predicate::And(
        (0..width)
            .map(|index| Predicate::IsNull {
                field: format!("field_{index}"),
            })
            .collect(),
    ));
    scalar.filter_expr = Some(Expr::Field(FieldId::new("enabled")));
    plan.projection_selection = ProjectionSelection::Fields(
        (0..width)
            .map(|index| FieldId::new(format!("field_{index}")))
            .collect(),
    );
    plan
}

#[test]
fn order_only_candidates_distinguish_absent_from_incompatible_order() {
    use super::{
        evaluator::evaluate_index_candidate,
        model::{AccessChoiceFamily, AccessChoiceRejectedReason, CandidateEvaluation},
    };
    use crate::db::query::plan::{
        OrderDirection, OrderSpec, OrderTerm, order_contract::CandidateOrderContract,
    };

    let schema = exact_metadata_schema(&[("by_age", &["age", "id"])], &[]);
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let index = &visible.accepted_semantic_index_contracts()[0];
    let incompatible = OrderSpec {
        fields: vec![
            OrderTerm::field("age", OrderDirection::Asc),
            OrderTerm::field("id", OrderDirection::Desc),
        ],
    };
    for grouped in [false, true] {
        for order in [None, Some(&incompatible)] {
            let contract = crate::db::query::preparation::with_preparation_work(|work| {
                CandidateOrderContract::prepare(&schema, order, grouped, work).unwrap()
            });
            assert!(contract.is_none());
            with_preparation_work(|work| {
                let evaluation = evaluate_index_candidate(
                    AccessChoiceFamily::Range,
                    index,
                    &schema,
                    None,
                    order.is_some(),
                    contract.as_ref(),
                    work,
                )
                .unwrap();
                match evaluation {
                    CandidateEvaluation::Eligible(score) => {
                        assert!(order.is_some());
                        assert!(!score.order_compatible);
                    }
                    CandidateEvaluation::Rejected(reason) => {
                        assert!(order.is_none());
                        assert_eq!(reason, AccessChoiceRejectedReason::PredicateAbsent);
                    }
                }
            });
        }
    }
}

#[test]
fn candidate_residual_matches_plan_semantics_without_mutating_inputs() {
    with_preparation_work(|work| {
        let access = AccessPlan::full_scan();
        for width in [0, 1, 8] {
            for filter_expr in [
                None,
                Some(Expr::Field(FieldId::new("enabled"))),
                Some(Expr::FieldPath(FieldPath::new(
                    FieldId::new("account"),
                    vec!["enabled".into()],
                ))),
            ] {
                for covered in [false, true] {
                    let mut plan = candidate_fixture(width);
                    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
                        unreachable!("scalar fixture");
                    };
                    scalar.filter_expr = filter_expr.clone();
                    scalar.predicate_covers_filter_expr = covered;
                    if width == 0 {
                        scalar.predicate = None;
                    }
                    let before = plan.clone();
                    let expected = residual_burden_for_plan(&plan, work).unwrap();
                    assert_eq!(
                        residual_burden_for_candidate(&plan, &access, work).unwrap(),
                        expected
                    );
                    assert_eq!(plan, before);
                    assert_eq!(
                        residual_burden_for_candidate(&plan, &access, work).unwrap(),
                        expected
                    );
                }
            }
        }
    });
}

#[test]
fn candidate_residual_preserves_predicate_and_expression_categories() {
    with_preparation_work(|work| {
        let mut plan = candidate_fixture(8);
        let access = AccessPlan::full_scan();
        let mixed = residual_burden_for_candidate(&plan, &access, work).unwrap();
        assert_eq!((mixed.kind_rank, mixed.predicate_term_count), (2, 8));

        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!("scalar fixture");
        };
        scalar.predicate_covers_filter_expr = true;
        let predicate = residual_burden_for_candidate(&plan, &access, work).unwrap();
        assert_eq!(
            (predicate.kind_rank, predicate.predicate_term_count),
            (1, 8)
        );
        assert!(predicate < mixed);

        let empty = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
        assert!(
            residual_burden_for_candidate(&empty, &access, work)
                .unwrap()
                .is_empty()
        );
    });
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

#[test]
fn residual_construction_rejects_before_returning_a_profile_and_is_cumulative() {
    let plan = candidate_fixture(8);
    let before = plan.clone();
    let access = AccessPlan::full_scan();
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let baseline = request(resource, 16_000_000);
        PreparationWork::run(&baseline.scope(), Lane::Diagnostic, |work| {
            residual_burden_for_candidate(&plan, &access, work).map_err(QueryError::execute)
        })
        .unwrap();
        let required = baseline.observed(resource);
        assert!(required > 0);
        for limit in [0, required - 1, required] {
            let root = request(resource, limit);
            PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                let result = residual_burden_for_candidate(&plan, &access, work);
                let error = if limit == required {
                    assert!(result.is_ok());
                    residual_burden_for_candidate(&plan, &access, work).unwrap_err()
                } else {
                    result.unwrap_err()
                };
                assert!(
                    QueryError::execute(error)
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                );
                Ok(())
            })
            .unwrap();
            assert_eq!(plan, before);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn empty_residual_needs_no_predicate_construction() {
    let plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let root = request(Resource::TemporaryBytes, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        assert!(residual_burden_for_plan(&plan, work).unwrap().is_empty());
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn finalized_residual_borrows_frozen_predicate_but_counts_current_work() {
    let schema = exact_metadata_schema(&[], &["maybe"]);
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!("scalar fixture");
    };
    scalar.predicate = Some(Predicate::IsNull {
        field: "maybe".into(),
    });
    with_preparation_work(|work| {
        plan.finalize_static_execution_planning_contract_with_schema(
            &schema,
            ProjectionSpec::new(Vec::new()),
            work,
        )
        .unwrap();
    });
    let root = request(Resource::TemporaryBytes, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        for _ in 0..2 {
            let burden = residual_burden_for_plan(&plan, work).unwrap();
            assert_eq!((burden.kind_rank, burden.predicate_term_count), (1, 1));
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 2);

    let root = request(Resource::PredicateExpressionSteps, 0);
    let error = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        residual_burden_for_plan(&plan, work).map_err(QueryError::execute)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw(),
    )));
}

#[test]
fn residual_budget_failure_is_not_an_absent_reranking_candidate() {
    let schema = exact_metadata_schema(&[], &[]);
    let plan = candidate_fixture(8);
    let root = request(Resource::TemporaryBytes, 0);
    let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        super::rerank_access_plan_by_residual_burden_from_authority(&[], &schema, &plan, work)
            .map_err(QueryError::execute)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
}

#[test]
fn candidate_bound_residual_preserves_owned_derivation_results() {
    let schema = exact_metadata_schema(&[("by_age", &["age"])], &[]);
    let index = VisibleIndexes::accepted_schema_visible(&schema)
        .unwrap()
        .accepted_semantic_index_contracts()[0]
        .clone();
    let access = AccessPlan::index_prefix_from_contract(index, vec![crate::value::Value::Int64(7)]);
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!("scalar fixture");
    };
    scalar.predicate = Some(Predicate::And(vec![
        Predicate::eq("age".into(), crate::value::Value::Int64(7)),
        Predicate::IsNull {
            field: "maybe".into(),
        },
    ]));
    with_preparation_work(|work| {
        let burden = residual_burden_for_candidate(&plan, &access, work).unwrap();
        assert_eq!((burden.kind_rank, burden.predicate_term_count), (1, 1));
    });
}
