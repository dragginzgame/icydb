//! Borrowed aggregate facts retain canonical identity and strategy semantics.

use super::*;
use crate::db::{
    predicate::MissingRowPolicy,
    query::{
        plan::{
            AccessPlannedQuery, AggregateShape, GroupFieldSet, GroupSpec,
            GroupedAggregateExecutionSpec, GroupedExecutionConfig, GroupedPlanAggregateFamily,
            LogicalPlan, expr::BinaryOp, grouped_plan_strategy,
        },
        preparation::with_preparation_work,
    },
};

fn aggregate(kind: AggregateKind, input: Option<Expr>, distinct: bool) -> GroupAggregateSpec {
    let shape = input.map_or_else(
        || AggregateShape::terminal(kind),
        |input| AggregateShape::from_expression_input(kind, input),
    );
    GroupAggregateSpec::from_shape(shape.with_raw_distinct(distinct))
}

#[test]
fn borrowed_grouped_aggregate_facts_preserve_all_identity_families() {
    let mut deep = Expr::Field("amount".into());
    for _ in 1..crate::db::query::admission::input::MAX_QUERY_INPUT_DEPTH {
        deep = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(deep),
            right: Box::new(Expr::Literal(Value::Nat64(1))),
        };
    }
    let inputs = [
        None,
        Some(Expr::Literal(Value::Null)),
        Some(Expr::Literal(Value::Nat64(0))),
        Some(Expr::Literal(Value::Text("λ".repeat(16_384)))),
        Some(Expr::Literal(Value::List(vec![Value::Null]))),
        Some(Expr::Field("amount".into())),
        Some(deep),
    ];
    for kind in [
        AggregateKind::Count,
        AggregateKind::Sum,
        AggregateKind::Avg,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::Exists,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        for distinct in [false, true] {
            for input in &inputs {
                let mut spec = aggregate(kind, input.clone(), distinct);
                let expected_count = kind == AggregateKind::Count
                    && !distinct
                    && match spec.input_expr() {
                        None => true,
                        Some(Expr::Literal(value)) => !matches!(value, Value::Null),
                        Some(_) => false,
                    };
                let expected_distinct =
                    distinct && !matches!(kind, AggregateKind::Min | AggregateKind::Max);
                // Filters remain a separate semantic dimension; they do not
                // change these aggregate-identity facts.
                for filtered in [false, true] {
                    if filtered {
                        spec = GroupAggregateSpec::from_shape(
                            spec.shape()
                                .clone()
                                .with_filter_expr(Expr::Literal(Value::Bool(false))),
                        );
                    }
                    let identity = spec.identity();
                    let expr = group_aggregate_spec_expr(&spec);
                    let execution = GroupedAggregateExecutionSpec::from_aggregate_expr(&expr);
                    assert_eq!(
                        spec.semantic_key(),
                        AggregateSemanticKeyRef::from_aggregate_expr(&expr)
                    );
                    assert_eq!(spec.semantic_key(), execution.semantic_key());
                    assert!(execution.matches_aggregate_identity(&spec));
                    assert!(execution.matches_aggregate_expr(&expr));
                    assert_eq!(
                        matches!(
                            identity,
                            AggregateIdentity::Count {
                                input_expr: None,
                                distinct: false,
                            }
                        ),
                        expected_count
                    );
                    assert_eq!(identity.distinct(), expected_distinct);
                    assert_eq!(spec.is_count_rows_only(), expected_count);
                    assert_eq!(spec.semantic_distinct(), expected_distinct);
                    assert_eq!(
                        spec.streaming_compatible(),
                        kind.supports_grouped_streaming(
                            spec.target_field().is_some(),
                            identity.distinct()
                        )
                    );
                    let expected_family = if expected_count {
                        GroupedPlanAggregateFamily::CountRowsOnly
                    } else {
                        kind.grouped_plan_family(spec.target_field().is_some())
                    };
                    assert_eq!(
                        GroupedPlanAggregateFamily::from_grouped_aggregates(std::slice::from_ref(
                            &spec
                        )),
                        expected_family
                    );
                }
            }
        }
    }
}

#[test]
fn borrowed_grouped_aggregate_facts_preserve_list_and_explain_profiles() {
    let count = aggregate(
        AggregateKind::Count,
        Some(Expr::Literal(Value::Nat64(1))),
        false,
    );
    let field = aggregate(AggregateKind::Min, Some(Expr::Field("amount".into())), true);
    let generic = aggregate(
        AggregateKind::Sum,
        Some(Expr::Literal(Value::Nat64(2))),
        false,
    );
    for (aggregates, family, fallback) in [
        (
            vec![],
            GroupedPlanAggregateFamily::FieldTargetRows,
            "group_key_order_unavailable",
        ),
        (
            vec![count.clone()],
            GroupedPlanAggregateFamily::CountRowsOnly,
            "group_key_order_unavailable",
        ),
        (
            vec![field.clone(), field.clone()],
            GroupedPlanAggregateFamily::FieldTargetRows,
            "group_key_order_unavailable",
        ),
        (
            vec![count, field],
            GroupedPlanAggregateFamily::GenericRows,
            "group_key_order_unavailable",
        ),
        (
            vec![generic],
            GroupedPlanAggregateFamily::GenericRows,
            "aggregate_streaming_not_supported",
        ),
    ] {
        assert_eq!(
            GroupedPlanAggregateFamily::from_grouped_aggregates(&aggregates),
            family
        );
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.logical = LogicalPlan::Grouped(GroupPlan {
            scalar: plan.scalar_plan().clone(),
            group: GroupSpec {
                group_fields: GroupFieldSet::Direct(vec![]),
                aggregates,
                execution: GroupedExecutionConfig::planner_default_bounded(),
            },
            having_expr: None,
        });
        let signature = plan.continuation_signature("tests::Entity").unwrap();
        let strategy = grouped_plan_strategy(&plan).unwrap();
        assert_eq!(strategy.aggregate_family(), family);
        assert_eq!(strategy.code(), "hash_group");
        assert_eq!(strategy.fallback_reason().unwrap().code(), fallback);
        let explained = with_preparation_work(|work| plan.project_explain(work)).unwrap();
        assert!(explained.render_json_canonical().is_ok());
        assert_eq!(
            plan.continuation_signature("tests::Entity").unwrap(),
            signature
        );
        assert_eq!(
            with_preparation_work(|work| plan.project_explain(work)).unwrap(),
            explained
        );
    }
}
