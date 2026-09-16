//! Retained ordering shares copy admission without changing semantics or identity.

use super::*;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::MissingRowPolicy,
        query::{
            builder::sum,
            plan::{
                AccessPlannedQuery, LogicalPlan, OrderTerm,
                expr::{CaseWhenArm, Expr},
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

#[test]
fn initial_order_contracts_share_bounded_labels_and_cumulative_authority() {
    use crate::db::query::plan::exact_metadata_schema;

    let schema = exact_metadata_schema(&[], &[]);
    let names = schema.shared_primary_key_names();
    let order = OrderSpec {
        fields: vec![
            OrderTerm::field("账户".repeat(64), OrderDirection::Desc),
            OrderTerm::new(
                Expr::Literal(Value::List(vec![Value::Text("quoted\"".repeat(64))])),
                OrderDirection::Desc,
            ),
            OrderTerm::field("id", OrderDirection::Desc),
        ],
    };
    let before = order.clone();
    for grouped in [false, true] {
        let labels: Vec<_> = order.fields[..if grouped { 3 } else { 2 }]
            .iter()
            .map(OrderTerm::rendered_label)
            .collect();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
                let baseline = request(resource, 16_000_000);
                PreparationWork::run(&baseline.scope(), lane, |budget| {
                    let contract =
                        CandidateOrderContract::prepare(&schema, Some(&order), grouped, budget)
                            .unwrap()
                            .unwrap();
                    match contract {
                        CandidateOrderContract::Scalar(contract) => {
                            assert_eq!(contract.non_primary_key_terms, labels);
                            assert!(Rc::ptr_eq(&contract.primary_key_terms, &names));
                            assert_eq!(contract.direction(), OrderDirection::Desc);
                        }
                        CandidateOrderContract::Grouped(contract) => {
                            assert_eq!(contract.terms, labels);
                        }
                    }
                    Ok(())
                })
                .unwrap();
                let exact = baseline.observed(resource);
                assert!(exact > 0);
                for limit in [0, exact - 1, exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |budget| {
                        for attempt in 1..=3 {
                            let result = CandidateOrderContract::prepare(
                                &schema,
                                Some(&order),
                                grouped,
                                budget,
                            );
                            if attempt * exact <= limit {
                                assert!(result.unwrap().is_some());
                                assert_eq!(root.observed(resource), attempt * exact);
                            } else {
                                let Err(error) = result else {
                                    panic!("exhaustion must not become an incompatible order")
                                };
                                assert!(QueryError::execute(error).diagnostic_facts().contains(&(
                                    DiagnosticFactTag::BudgetResource,
                                    resource.raw()
                                )));
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                    assert_eq!(order, before);
                }
            }
        }
    }
}

#[test]
fn initial_order_rejection_does_not_materialize_labels() {
    use crate::db::query::plan::exact_metadata_schema;

    let schema = exact_metadata_schema(&[], &[]);
    for grouped in [false, true] {
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |budget| {
            assert!(
                CandidateOrderContract::prepare(&schema, None, grouped, budget)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
            for order in [
                OrderSpec { fields: vec![] },
                OrderSpec {
                    fields: vec![
                        OrderTerm::field("age", OrderDirection::Asc),
                        OrderTerm::field("id", OrderDirection::Desc),
                    ],
                },
            ] {
                assert!(
                    CandidateOrderContract::prepare(&schema, Some(&order), grouped, budget)
                        .unwrap()
                        .is_none()
                );
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn route_profile_publication_rejects_unadmitted_order_construction() {
    use crate::db::query::plan::exact_metadata_schema;

    let schema = exact_metadata_schema(&[], &[]);
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.order = Some(OrderSpec {
        fields: vec![
            OrderTerm::field("age", OrderDirection::Asc),
            OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    let before = plan.planner_route_profile().clone();
    let root = request(Resource::TemporaryBytes, 0);
    let result = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        plan.finalize_planner_route_profile_for_model_with_schema(&schema, work)
            .map_err(QueryError::execute)
    });
    assert!(result.unwrap_err().diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    assert_eq!(plan.planner_route_profile(), &before);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
}

fn build(
    order: Option<&OrderSpec>,
    grouped: bool,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<ExecutionOrderContract, QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        ExecutionOrderContract::from_plan(grouped, order, work).map_err(QueryError::execute)
    })
}

#[test]
fn retained_order_shares_exact_and_cumulative_expression_copy_admission() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Literal(Value::Bool(true)),
            Expr::Aggregate(
                sum("amount")
                    .distinct()
                    .with_filter_expr(Expr::Literal(Value::Bool(false))),
            ),
        )],
        else_expr: Box::new(Expr::Literal(Value::Map(vec![(
            Value::Text("key".repeat(128)),
            Value::List(vec![Value::Nat64(9)]),
        )]))),
    };
    let order = OrderSpec {
        fields: vec![
            OrderTerm::new(expr.clone(), OrderDirection::Desc),
            OrderTerm::new(expr, OrderDirection::Asc),
        ],
    };
    let before = order.clone();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::TemporaryBytes, 16_000_000);
        let copied =
            PreparationWork::run(&measured.scope(), lane, |work| work.copy_order_spec(&order))
                .unwrap();
        assert_eq!(copied, order);
        for grouped in [false, true] {
            for resource in [
                Resource::TemporaryBytes,
                Resource::PredicateExpressionSteps,
                Resource::NestedValueSteps,
            ] {
                let exact = measured.observed(resource);
                assert!(exact > 0);
                for limit in [exact - 1, exact, 2 * exact] {
                    let root = request(resource, limit);
                    for attempt in 1..=3 {
                        let result = build(Some(&order), grouped, &root, lane);
                        if attempt * exact <= limit {
                            let contract = result.unwrap();
                            assert_eq!(contract.order_spec(), Some(&before));
                            assert_eq!(contract.direction(), Direction::Desc);
                            assert_eq!(contract.is_grouped(), grouped);
                            assert!(contract.supports_cursor);
                        } else {
                            let facts = result.unwrap_err().diagnostic_facts();
                            assert!(
                                facts
                                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                            );
                            assert!(
                                facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
                            );
                            break;
                        }
                    }
                    assert_eq!(order, before);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                    assert_eq!(root.observed(Resource::QueryExecutions), 0);
                }
            }
        }
    }
}

#[test]
fn absent_and_empty_order_preserve_distinct_contracts_without_backing() {
    let empty = OrderSpec { fields: vec![] };
    for order in [None, Some(&empty)] {
        for grouped in [false, true] {
            let root = request(Resource::TemporaryBytes, 0);
            let contract = build(order, grouped, &root, Lane::Diagnostic).unwrap();
            assert_eq!(contract.order_spec(), order);
            assert_eq!(contract.supports_cursor, grouped || order.is_some());
            assert_eq!(contract.is_grouped(), grouped);
            assert_eq!(contract.direction(), Direction::Asc);
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
            assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        }
    }
}

#[test]
fn retained_order_rejection_prevents_a_continuation_after_hashing() {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.order = Some(OrderSpec {
        fields: vec![OrderTerm::field("label", OrderDirection::Desc)],
    });
    let signature = PreparationWork::run(
        &request(Resource::TemporaryBytes, 0).scope(),
        Lane::PublicRead,
        |work| {
            plan.continuation_signature("tests::Entity", work)
                .map_err(QueryError::execute)
        },
    )
    .unwrap();
    let make = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
    };
    let error = make(&request(Resource::TemporaryBytes, 0)).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    let measured = request(Resource::TemporaryBytes, 16_000_000);
    assert_eq!(
        make(&measured).unwrap().unwrap().continuation_signature(),
        signature
    );
    let exact = measured.observed(Resource::TemporaryBytes);
    let root = request(Resource::TemporaryBytes, 2 * exact);
    for _ in 0..2 {
        assert_eq!(
            make(&root).unwrap().unwrap().continuation_signature(),
            signature
        );
    }
    assert!(make(&root).is_err());
    assert_eq!(
        make(&request(Resource::TemporaryBytes, exact))
            .unwrap()
            .unwrap()
            .continuation_signature(),
        signature
    );
}
