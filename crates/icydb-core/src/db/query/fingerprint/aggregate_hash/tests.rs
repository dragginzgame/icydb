//! Grouped aggregate identity uses planned semantics and admitted label construction.

use super::hash_group_aggregate_structural_fingerprint;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        codec::{new_hash_sha256, write_hash_str_u32, write_hash_tag_u8},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            builder::{AggregateExpr, count, count_by, min_by, sum},
            fingerprint::finalize_sha256_digest,
            plan::{AggregateKind, GroupAggregateSpec, expr::Expr},
            preparation::{PreparationWork, with_preparation_work},
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn formatted_aggregate() -> AggregateExpr {
    AggregateExpr::from_expression_input(
        AggregateKind::Sum,
        Expr::Literal(Value::NatBig("18446744073709551616".parse().unwrap())),
    )
    .with_filter_expr(Expr::Literal(Value::Text("quote's λ".into())))
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
    aggregate: &GroupAggregateSpec,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<[u8; 32], QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        let mut hasher = new_hash_sha256();
        hash_group_aggregate_structural_fingerprint(&mut hasher, aggregate, work)
            .map_err(QueryError::execute)?;
        Ok(finalize_sha256_digest(hasher))
    })
}

#[test]
fn planned_aggregates_preserve_target_input_filter_and_distinct_framing() {
    for (expr, target, input, filter, distinct) in [
        (count(), None, None, None, false),
        (
            count_by("rank").distinct(),
            Some("rank"),
            Some("rank"),
            None,
            true,
        ),
        (
            min_by("rank").distinct(),
            Some("rank"),
            Some("rank"),
            None,
            false,
        ),
        (
            sum("rank").with_filter_expr(Expr::Literal(Value::Bool(true))),
            Some("rank"),
            Some("rank"),
            Some("TRUE"),
            false,
        ),
        (
            formatted_aggregate(),
            None,
            Some("18_446_744_073_709_551_616"),
            Some("'quote''s λ'"),
            false,
        ),
    ] {
        let aggregate = GroupAggregateSpec::from_aggregate_expr(expr);
        // Pin the maintained framing independently of production label rendering.
        let mut expected = new_hash_sha256();
        write_hash_tag_u8(&mut expected, 0x01);
        write_hash_tag_u8(&mut expected, aggregate.kind().fingerprint_tag());
        write_hash_tag_u8(&mut expected, u8::from(target.is_some()));
        if let Some(target) = target {
            write_hash_str_u32(&mut expected, target);
        }
        write_hash_tag_u8(&mut expected, if distinct { 0x02 } else { 0x03 });
        if let Some(input) = input {
            write_hash_tag_u8(&mut expected, 0x04);
            write_hash_str_u32(&mut expected, input);
        }
        write_hash_tag_u8(&mut expected, if filter.is_some() { 0x05 } else { 0x06 });
        if let Some(filter) = filter {
            write_hash_str_u32(&mut expected, filter);
        }
        assert_eq!(
            admitted_hash(
                &aggregate,
                &request(Resource::TemporaryBytes, 16_000_000),
                Lane::Diagnostic
            )
            .unwrap(),
            finalize_sha256_digest(expected)
        );
    }
}

#[test]
fn semantic_modifiers_operands_and_projection_order_keep_their_identity_rules() {
    let hash = |exprs: Vec<AggregateExpr>| {
        with_preparation_work(|work| {
            let mut hasher = new_hash_sha256();
            for expr in exprs {
                hash_group_aggregate_structural_fingerprint(
                    &mut hasher,
                    &GroupAggregateSpec::from_aggregate_expr(expr),
                    work,
                )
                .unwrap();
            }
            finalize_sha256_digest(hasher)
        })
    };
    assert_eq!(
        hash(vec![min_by("rank")]),
        hash(vec![min_by("rank").distinct()])
    );
    assert_ne!(
        hash(vec![count_by("rank")]),
        hash(vec![count_by("rank").distinct()])
    );
    assert_ne!(
        hash(vec![count(), sum("rank")]),
        hash(vec![sum("rank"), count()])
    );
    assert_ne!(hash(vec![sum("rank")]), hash(vec![sum("other")]));
    assert_ne!(hash(vec![sum("rank")]), hash(vec![formatted_aggregate()]));
    let filtered = |value| sum("rank").with_filter_expr(Expr::Literal(Value::Bool(value)));
    assert_ne!(hash(vec![filtered(true)]), hash(vec![filtered(false)]));
    assert_ne!(hash(vec![sum("rank")]), hash(vec![filtered(true)]));
}

#[test]
fn aggregate_label_admission_is_exact_cumulative_and_allocation_free_for_fields() {
    let direct = GroupAggregateSpec::from_aggregate_expr(sum("账户"));
    let root = request(Resource::TemporaryBytes, 0);
    admitted_hash(&direct, &root, Lane::PublicRead).unwrap();
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    assert_eq!(
        root.observed(Resource::PredicateExpressionSteps),
        1 + 2 * "账户".len() as u64
    );

    let aggregate = GroupAggregateSpec::from_aggregate_expr(formatted_aggregate());
    let before = aggregate.clone();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::TemporaryBytes, 16_000_000);
        let expected = admitted_hash(&aggregate, &measured, lane).unwrap();
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let exact = measured.observed(resource);
            assert!(exact > 0);
            for limit in [exact - 1, exact, 2 * exact] {
                let root = request(resource, limit);
                for attempt in 1..=3 {
                    let result = admitted_hash(&aggregate, &root, lane);
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
            assert_eq!(
                admitted_hash(&aggregate, &request(resource, exact), lane).unwrap(),
                expected
            );
        }
    }
    assert_eq!(aggregate, before);
}

#[test]
fn aggregate_label_failure_prevents_grouped_continuation_publication() {
    use crate::db::{
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, FieldSlot, GroupFieldSet, GroupPlan, GroupSpec,
            GroupedExecutionConfig, LogicalPlan,
        },
        schema::AcceptedFieldKind,
    };

    // Exercise both the planned aggregate and missing HAVING-slot label edges.
    for missing_having_slot in [false, true] {
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.logical = LogicalPlan::Grouped(GroupPlan {
            scalar: plan.scalar_plan().clone(),
            group: GroupSpec {
                group_fields: GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
                    0,
                    "key",
                    AcceptedFieldKind::Int32,
                )]),
                aggregates: vec![GroupAggregateSpec::from_aggregate_expr(
                    if missing_having_slot {
                        count()
                    } else {
                        formatted_aggregate()
                    },
                )],
                execution: GroupedExecutionConfig::planner_default_bounded(),
            },
            having_expr: missing_having_slot.then(|| Expr::Aggregate(formatted_aggregate())),
        });
        let build = |root: &RequestExecutionRoot| {
            PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                plan.planned_continuation_contract_with_accepted_identity(
                    "tests::Entity",
                    None,
                    work,
                )
                .map_err(QueryError::execute)
            })
        };
        let rejected = request(Resource::TemporaryBytes, 27);
        assert!(build(&rejected).unwrap_err().diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        )));
        assert_eq!(rejected.observed(Resource::TemporaryBytes), 28);
        let measured = request(Resource::TemporaryBytes, 16_000_000);
        let expected = build(&measured).unwrap().unwrap().continuation_signature();
        let exact = measured.observed(Resource::TemporaryBytes);
        assert!(build(&request(Resource::TemporaryBytes, exact - 1)).is_err());
        let root = request(Resource::TemporaryBytes, 2 * exact);
        for _ in 0..2 {
            assert_eq!(
                build(&root).unwrap().unwrap().continuation_signature(),
                expected
            );
        }
        assert!(build(&root).is_err());
        assert_eq!(
            build(&request(Resource::TemporaryBytes, exact))
                .unwrap()
                .unwrap()
                .continuation_signature(),
            expected
        );
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::QueryExecutions), 0);
    }
}

#[test]
fn aggregate_comparison_failure_prevents_grouped_continuation_publication() {
    use crate::db::{
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, FieldSlot, GroupFieldSet, GroupPlan, GroupSpec,
            GroupedExecutionConfig, LogicalPlan,
        },
        schema::AcceptedFieldKind,
    };

    let aggregate = sum("amount").with_filter_expr(Expr::Literal(Value::Bool(true)));
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    plan.logical = LogicalPlan::Grouped(GroupPlan {
        scalar: plan.scalar_plan().clone(),
        group: GroupSpec {
            group_fields: GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
                0,
                "key",
                AcceptedFieldKind::Int32,
            )]),
            aggregates: vec![GroupAggregateSpec::from_aggregate_expr(aggregate.clone())],
            execution: GroupedExecutionConfig::planner_default_bounded(),
        },
        having_expr: Some(Expr::Aggregate(aggregate)),
    });
    let build = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
    };
    // These direct labels do not visit literal values. The matched slot's
    // filter comparison is the nested-value admission boundary in this plan.
    let rejected = request(Resource::NestedValueSteps, 0);
    assert!(build(&rejected).unwrap_err().diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::NestedValueSteps.raw(),
    )));
    let measured = request(Resource::NestedValueSteps, 16_000_000);
    let expected = build(&measured).unwrap().unwrap().continuation_signature();
    let exact = measured.observed(Resource::NestedValueSteps);
    assert!(exact > 0);
    assert!(build(&request(Resource::NestedValueSteps, exact - 1)).is_err());
    let root = request(Resource::NestedValueSteps, 2 * exact);
    for _ in 0..2 {
        assert_eq!(
            build(&root).unwrap().unwrap().continuation_signature(),
            expected
        );
    }
    assert!(build(&root).is_err());
    assert_eq!(
        build(&request(Resource::NestedValueSteps, exact))
            .unwrap()
            .unwrap()
            .continuation_signature(),
        expected,
    );
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(root.observed(Resource::QueryExecutions), 0);
}
