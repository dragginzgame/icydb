//! Pin planner-owned HAVING identity bytes.

use super::*;
use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use crate::{
    db::{
        codec::{hex::encode_hex_lower, new_hash_sha256},
        query::{
            builder::{count, min_by, sum},
            fingerprint::finalize_sha256_digest,
            plan::{
                FieldSlot,
                expr::{FieldPath, Function},
            },
            preparation::{PreparationWork, with_preparation_work},
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticExecutionLane as Lane, DiagnosticFactTag};

fn aggregates() -> Vec<AggregateExpr> {
    let sum = sum("amount").with_filter_expr(Expr::Literal(Value::Bool(true)));
    vec![min_by("amount").distinct(), sum.clone(), sum]
}

#[test]
fn having_hash_propagates_literal_failure() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};
    let source = GroupHavingFingerprintSource {
        expr: &Expr::Literal(Value::Bool(true)),
        group_fields: &GroupFieldSet::Direct(vec![]),
        aggregates: &[],
    };
    with_test_hash_override(Err(test_hash_budget_error), || {
        let error = with_preparation_work(|work| {
            hash_group_having_projection(&mut new_hash_sha256(), Some(&source), work)
        })
        .unwrap_err();
        assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
}

fn cases() -> Vec<Expr> {
    let mut cases = vec![
        Expr::Field("owner".into()),
        Expr::Field("missing".into()),
        Expr::FieldPath(FieldPath::new("profile", vec!["tag".into()])),
        Expr::Literal(Value::Text("quote's λ".into())),
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Literal(Value::Bool(false))),
        },
        Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![Expr::Field("owner".into()), Expr::Literal(Value::Null)],
        },
        Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Literal(Value::Bool(true)),
                Expr::Aggregate(sum("amount")),
            )],
            else_expr: Box::new(Expr::Literal(Value::Nat64(1))),
        },
        Expr::Aggregate(count()),
        Expr::Aggregate(sum("amount").with_filter_expr(Expr::Literal(Value::Bool(false)))),
        Expr::Aggregate(AggregateExpr::from_expression_input(
            crate::db::query::plan::AggregateKind::Sum,
            Expr::Literal(Value::Text("λ'\\n".repeat(64))),
        )),
    ];
    cases.extend(aggregates().into_iter().map(Expr::Aggregate));
    for op in [
        BinaryOp::Eq,
        BinaryOp::Ne,
        BinaryOp::Lt,
        BinaryOp::Lte,
        BinaryOp::Gt,
        BinaryOp::Gte,
        BinaryOp::And,
        BinaryOp::Or,
        BinaryOp::Add,
        BinaryOp::Sub,
        BinaryOp::Mul,
        BinaryOp::Div,
    ] {
        cases.push(Expr::Binary {
            op,
            left: Box::new(Expr::Field("owner".into())),
            right: Box::new(Expr::Literal(Value::Nat64(7))),
        });
    }
    cases
}

#[test]
fn having_hash_preserves_planner_expression_grammar() {
    let aggregates = aggregates();
    let planned: Vec<_> = aggregates
        .iter()
        .cloned()
        .map(GroupAggregateSpec::from_aggregate_expr)
        .collect();
    let plan_fields = GroupFieldSet::Direct(vec![FieldSlot::unresolved(2, "owner")]);
    let mut plan_hash = new_hash_sha256();
    with_preparation_work(|work| {
        hash_group_having_projection(&mut plan_hash, None, work).unwrap();
        for expr in cases() {
            hash_group_having_projection(
                &mut plan_hash,
                Some(&GroupHavingFingerprintSource {
                    expr: &expr,
                    group_fields: &plan_fields,
                    aggregates: &planned,
                }),
                work,
            )
            .unwrap();
        }
    });
    assert_eq!(
        encode_hex_lower(&finalize_sha256_digest(plan_hash)),
        "d32de1031cc115b3dd2e6bf3928cf9511717b0957c8dd01dd073ad5de0587960",
    );
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

fn admitted_hash(
    expr: &Expr,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<[u8; 32], QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        let mut hasher = new_hash_sha256();
        hash_group_having_projection(
            &mut hasher,
            Some(&GroupHavingFingerprintSource {
                expr,
                group_fields: &GroupFieldSet::Direct(vec![]),
                aggregates: &[],
            }),
            work,
        )
        .map_err(QueryError::execute)?;
        Ok(finalize_sha256_digest(hasher))
    })
}

#[test]
fn having_hash_admission_is_cumulative_across_read_lanes() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::FieldPath(FieldPath::new("profile", vec!["tag".into()])),
            Expr::FunctionCall {
                function: Function::Coalesce,
                args: vec![
                    Expr::Field("owner".into()),
                    Expr::Literal(Value::Map(vec![(
                        Value::Text("key".repeat(128)),
                        Value::List(vec![Value::Nat64(7), Value::Bool(true)]),
                    )])),
                ],
            },
        )],
        else_expr: Box::new(Expr::Literal(Value::Null)),
    };
    let before = expr.clone();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::PredicateExpressionSteps, 16_000_000);
        let expected = admitted_hash(&expr, &measured, lane).unwrap();
        for resource in [
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
            Resource::TemporaryBytes,
        ] {
            let exact = measured.observed(resource);
            assert!(exact > 0);
            for limit in [exact - 1, exact, 2 * exact] {
                let root = request(resource, limit);
                for attempt in 1..=3 {
                    let result = admitted_hash(&expr, &root, lane);
                    if attempt * exact <= limit {
                        assert_eq!(result.unwrap(), expected);
                    } else {
                        let facts = result.unwrap_err().diagnostic_facts();
                        assert!(
                            facts.contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                        assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
            }
        }
    }
    assert_eq!(expr, before);
}

#[test]
fn having_literal_rejects_before_entering_the_value_writer() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};

    with_test_hash_override(Err(test_hash_budget_error), || {
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let root = request(Resource::NestedValueSteps, 0);
            let error = admitted_hash(&Expr::Literal(Value::Bool(true)), &root, lane).unwrap_err();
            let facts = error.diagnostic_facts();
            assert!(facts.contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::NestedValueSteps.raw(),
            )));
            assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
        }
    });
}

#[test]
fn having_exhaustion_prevents_a_grouped_continuation_contract() {
    use crate::db::{
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, GroupPlan, GroupSpec, GroupedExecutionConfig, LogicalPlan,
        },
        schema::AcceptedFieldKind,
    };

    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    plan.logical = LogicalPlan::Grouped(GroupPlan {
        scalar: plan.scalar_plan().clone(),
        group: GroupSpec {
            group_fields: GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
                0,
                "key",
                AcceptedFieldKind::Int32,
            )]),
            aggregates: vec![GroupAggregateSpec::from_aggregate_expr(count())],
            execution: GroupedExecutionConfig::planner_default_bounded(),
        },
        having_expr: Some(Expr::Binary {
            op: BinaryOp::Gte,
            left: Box::new(Expr::Aggregate(count())),
            right: Box::new(Expr::Literal(Value::Nat64(7))),
        }),
    });
    let build = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
    };
    // The existing profile hashes grouping twice. Both occurrences must use the
    // same allowance, and neither may publish a partial contract on exhaustion.
    for limit in [0, 1] {
        let error = build(&request(Resource::NestedValueSteps, limit)).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::NestedValueSteps.raw(),
        )));
    }
    let root = request(Resource::NestedValueSteps, 4);
    let first = build(&root).unwrap().unwrap().continuation_signature();
    assert_eq!(
        build(&root).unwrap().unwrap().continuation_signature(),
        first
    );
    assert!(build(&root).is_err());
    assert_eq!(
        build(&request(Resource::NestedValueSteps, 2))
            .unwrap()
            .unwrap()
            .continuation_signature(),
        first,
    );
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(root.observed(Resource::QueryExecutions), 0);
}

#[test]
fn having_borrowed_labels_and_absence_need_no_value_or_allocation_budget() {
    let expr = Expr::FieldPath(FieldPath::new("profile", vec!["tag".into(), String::new()]));
    // Two expression dispatches, root bytes, segment visits and segment bytes.
    let exact = 2 + 7 + 2 + 3;
    for limit in [exact - 1, exact] {
        let root = request(Resource::PredicateExpressionSteps, limit);
        assert_eq!(
            admitted_hash(&expr, &root, Lane::Diagnostic).is_ok(),
            limit == exact
        );
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
    let root = request(Resource::PredicateExpressionSteps, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        hash_group_having_projection(&mut new_hash_sha256(), None, work)
            .map_err(QueryError::execute)
    })
    .unwrap();
    assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn missing_aggregate_labels_are_admitted_but_matched_slots_need_no_rendering() {
    let aggregate = AggregateExpr::from_expression_input(
        crate::db::query::plan::AggregateKind::Sum,
        Expr::Literal(Value::NatBig("18446744073709551616".parse().unwrap())),
    )
    .with_filter_expr(Expr::Literal(Value::Text("quote's λ".into())));
    let expr = Expr::Aggregate(aggregate.clone());
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let matched = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&matched.scope(), lane, |work| {
            let mut hasher = new_hash_sha256();
            hash_group_having_projection(
                &mut hasher,
                Some(&GroupHavingFingerprintSource {
                    expr: &expr,
                    group_fields: &GroupFieldSet::Direct(vec![]),
                    aggregates: &[GroupAggregateSpec::from_aggregate_expr(aggregate.clone())],
                }),
                work,
            )
            .map_err(QueryError::execute)
        })
        .unwrap();
        assert_eq!(matched.observed(Resource::TemporaryBytes), 0);
        let rejected = request(Resource::TemporaryBytes, 27);
        assert!(
            admitted_hash(&expr, &rejected, lane)
                .unwrap_err()
                .diagnostic_facts()
                .contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw(),
                ))
        );
        assert_eq!(rejected.observed(Resource::TemporaryBytes), 28);

        let measured = request(Resource::TemporaryBytes, 16_000_000);
        let expected = admitted_hash(&expr, &measured, lane).unwrap();
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let exact = measured.observed(resource);
            let root = request(resource, 2 * exact);
            for _ in 0..2 {
                assert_eq!(admitted_hash(&expr, &root, lane).unwrap(), expected);
            }
            assert!(
                admitted_hash(&expr, &root, lane)
                    .unwrap_err()
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
            );
            assert_eq!(
                admitted_hash(&expr, &request(resource, exact), lane).unwrap(),
                expected
            );
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn aggregate_slot_comparison_admits_payloads_without_rendering_or_changing_identity() {
    use crate::{db::query::plan::AggregateKind, value::ValueEnum};

    let mut operands = cases();
    operands.push(Expr::Literal(Value::List(vec![
        Value::Map(vec![(
            Value::Text("key".into()),
            Value::Enum(ValueEnum::test_payload(1, 2, Value::Blob(vec![7; 512]))),
        )]),
        Value::IntBig("-18446744073709551616".parse().unwrap()),
        Value::NatBig("18446744073709551616".parse().unwrap()),
        Value::Null,
    ])));
    for operand in operands {
        let aggregate = AggregateExpr::from_expression_input(AggregateKind::Sum, operand)
            .with_filter_expr(Expr::Literal(Value::Bool(true)));
        // Same header but different payload, then two equal slots. The first
        // structural match wins; none of these cases needs a rendered label.
        let slots = [
            GroupAggregateSpec::from_aggregate_expr(
                sum("different").with_filter_expr(Expr::Literal(Value::Bool(true))),
            ),
            GroupAggregateSpec::from_aggregate_expr(aggregate.clone()),
            GroupAggregateSpec::from_aggregate_expr(aggregate.clone()),
        ];
        let source = GroupHavingFingerprintSource {
            expr: &Expr::Literal(Value::Null),
            group_fields: &GroupFieldSet::Direct(vec![]),
            aggregates: &slots,
        };
        let mut expected = new_hash_sha256();
        write_tag(&mut expected, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);
        write_u32(&mut expected, 1);
        let expected = finalize_sha256_digest(expected);
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let hash = |root: &RequestExecutionRoot| {
                PreparationWork::run(&root.scope(), lane, |work| {
                    let mut hasher = new_hash_sha256();
                    source
                        .hash_aggregate_expr(&mut hasher, &aggregate, work)
                        .map_err(QueryError::execute)?;
                    Ok(finalize_sha256_digest(hasher))
                })
            };
            let measured = request(Resource::TemporaryBytes, 0);
            assert_eq!(hash(&measured).unwrap(), expected);
            assert_eq!(measured.observed(Resource::TemporaryBytes), 0);
            for resource in [
                Resource::PredicateExpressionSteps,
                Resource::NestedValueSteps,
            ] {
                let exact = measured.observed(resource);
                assert!(exact > 0);
                for limit in [exact - 1, exact, 2 * exact] {
                    let root = request(resource, limit);
                    for attempt in 1..=3 {
                        let result = hash(&root);
                        if attempt * exact <= limit {
                            assert_eq!(result.unwrap(), expected);
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
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                    assert_eq!(root.observed(Resource::QueryExecutions), 0);
                }
                assert_eq!(hash(&request(resource, exact)).unwrap(), expected);
            }
        }
    }
}

#[test]
fn aggregate_slot_lookup_skips_scalar_mismatches_and_stops_at_first_match() {
    let aggregate = sum("amount");
    let slots = [
        count(),
        sum("amount").distinct(),
        aggregate.clone(),
        aggregate.clone(),
    ]
    .map(GroupAggregateSpec::from_aggregate_expr);
    let source = GroupHavingFingerprintSource {
        expr: &Expr::Literal(Value::Null),
        group_fields: &GroupFieldSet::Direct(vec![]),
        aggregates: &slots,
    };
    // Three candidate visits, one field's admission/equality visits and bytes.
    let exact = 3 + 2 + "amount".len() as u64;
    let root = request(Resource::PredicateExpressionSteps, exact);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        source
            .hash_aggregate_expr(&mut new_hash_sha256(), &aggregate, work)
            .map_err(QueryError::execute)
    })
    .unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), exact);
    assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);

    // COUNT(non-null literal) is row count; discarded raw payloads are not
    // compared. Admission must use the same normalized key as equality.
    let count_literal = AggregateExpr::from_expression_input(
        crate::db::query::plan::AggregateKind::Count,
        Expr::Literal(Value::Text("x".repeat(1024))),
    );
    let root = request(Resource::PredicateExpressionSteps, 1);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        source
            .hash_aggregate_expr(&mut new_hash_sha256(), &count_literal, work)
            .map_err(QueryError::execute)
    })
    .unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 1);
    assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn group_field_lookup_admits_candidates_and_preserves_first_match_and_missing_identity() {
    use crate::db::{query::plan::GroupField, schema::AcceptedFieldKind};

    let fields = GroupFieldSet::PathAware(vec![
        GroupField::scalar_path_for_test(
            "profile.tag",
            "profile",
            vec!["tag".into()],
            3,
            AcceptedFieldKind::Text { max_len: None },
        ),
        GroupField::Direct(FieldSlot::from_test_accepted_kind(
            9,
            "owner",
            AcceptedFieldKind::Text { max_len: None },
        )),
        GroupField::Direct(FieldSlot::from_test_accepted_kind(
            17,
            "owner",
            AcceptedFieldKind::Text { max_len: None },
        )),
    ]);
    for (name, slot, visits) in [("owner", 9, 18), ("missing", u32::MAX, 24)] {
        let expr = Expr::Field(name.into());
        let source = GroupHavingFingerprintSource {
            expr: &expr,
            group_fields: &fields,
            aggregates: &[],
        };
        let exact = 1 + visits + name.len() as u64;
        let mut expected = new_hash_sha256();
        write_tag(&mut expected, 0x78);
        write_u32(&mut expected, slot);
        write_str(&mut expected, name);
        let expected = finalize_sha256_digest(expected);
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let hash = |root: &RequestExecutionRoot| {
                PreparationWork::run(&root.scope(), lane, |work| {
                    let mut hasher = new_hash_sha256();
                    hash_group_having_value_expr(&mut hasher, &expr, &source, work)
                        .map_err(QueryError::execute)?;
                    Ok(finalize_sha256_digest(hasher))
                })
            };
            // Reject during lookup, before either the matched or missing encoding.
            let error = hash(&request(Resource::PredicateExpressionSteps, visits)).unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::PredicateExpressionSteps.raw()
            )));
            assert!(hash(&request(Resource::PredicateExpressionSteps, exact - 1)).is_err());
            let root = request(Resource::PredicateExpressionSteps, 2 * exact);
            for _ in 0..2 {
                assert_eq!(hash(&root).unwrap(), expected);
            }
            assert!(hash(&root).is_err());
            assert_eq!(
                hash(&request(Resource::PredicateExpressionSteps, exact)).unwrap(),
                expected
            );
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        }
    }
}
