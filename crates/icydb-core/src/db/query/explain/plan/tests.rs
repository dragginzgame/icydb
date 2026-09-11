//! Public canonical renderers share one detached output policy.

use super::*;
use crate::db::query::explain::writer::MAX_LOGICAL_RENDER_BYTES;
use icydb_diagnostic_code::DiagnosticFactTag;

use crate::db::{
    RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::Predicate,
    query::{
        builder::{count, min_by},
        plan::{
            GroupAggregateSpec, GroupField, GroupFieldSet, GroupPlan, GroupSpec,
            GroupedExecutionConfig, OrderTerm, expr::BinaryOp,
            render_scalar_filter_expr_plan_label,
        },
    },
    schema::AcceptedFieldKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane as Lane;

fn plan() -> ExplainPlan {
    project(
        &AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore),
        &request(Resource::TemporaryBytes, 16_000_000),
    )
    .unwrap()
}

#[test]
fn logical_renderers_enforce_the_exact_public_output_limit() {
    for render in [
        ExplainPlan::render_text_canonical,
        ExplainPlan::render_json_canonical,
    ] {
        let mut plan = plan();
        // Rejection text is rendered without a large intermediate summary;
        // JSON escaping is independently exercised in the shared writer tests.
        plan.access_decision.selected.reason = "";
        plan.access_decision.selected.index_name = Some(String::new());
        let overhead = render(&plan).unwrap().len();
        plan.access_decision.selected.index_name =
            Some("x".repeat(MAX_LOGICAL_RENDER_BYTES - overhead));
        assert_eq!(render(&plan).unwrap().len(), MAX_LOGICAL_RENDER_BYTES);
        plan.access_decision
            .selected
            .index_name
            .as_mut()
            .unwrap()
            .push('x');
        let error = render(&plan).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::Limit, MAX_LOGICAL_RENDER_BYTES as u64,))
        );
        // The DTO and future calls retain neither failure nor a session budget.
        plan.access_decision.selected.index_name = None;
        assert!(render(&plan).is_ok());
    }
}

#[test]
fn logical_text_and_json_share_ordered_plan_summaries() {
    let plan = plan();
    let text = plan.render_text_canonical().unwrap();
    let json = plan.render_json_canonical().unwrap();
    let fields: Vec<_> = text
        .lines()
        .map(|line| line.split_once('=').unwrap())
        .collect();
    assert_eq!(
        fields.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        [
            "mode",
            "access",
            "access_decision",
            "filter_expr",
            "has_predicate",
            "order_by",
            "distinct",
            "grouping",
            "order_pushdown",
            "page",
            "delete_limit",
            "consistency",
        ]
    );
    assert_eq!(fields[0].1, "Load(LoadSpec { limit: None, offset: 0 })");
    assert_eq!(fields[1].1, r#"{"type":"FullScan"}"#);
    assert!(json.contains(r#""has_predicate":false"#));
    for index in [1, 2, 7] {
        let (name, value) = fields[index];
        assert!(json.contains(&format!("\"{name}\":{value}")));
    }
}

#[test]
fn logical_summaries_depend_on_plan_shape_not_operand_payload_size() {
    fn paths(value: Value) -> Vec<ExplainAccessPath> {
        vec![
            ExplainAccessPath::ByKey { key: value.clone() },
            ExplainAccessPath::ByKeys {
                keys: vec![value.clone()],
            },
            ExplainAccessPath::KeyRange {
                start: value.clone(),
                end: value.clone(),
            },
            ExplainAccessPath::IndexPrefix {
                name: "by_owner".into(),
                fields: vec!["owner".into(), "amount".into()],
                prefix_len: 1,
                values: vec![value.clone()],
            },
            ExplainAccessPath::IndexMultiLookup {
                name: "by_owner".into(),
                fields: vec!["owner".into()],
                values: vec![value.clone()],
            },
            ExplainAccessPath::IndexBranchSet {
                name: "by_owner".into(),
                fields: vec!["owner".into(), "amount".into()],
                fixed_values: vec![value.clone()],
                branch_values: vec![value.clone()],
                branch_field: Some("amount".into()),
            },
            ExplainAccessPath::IndexRange {
                name: "by_owner".into(),
                fields: vec!["owner".into(), "amount".into()],
                prefix_len: 1,
                prefix: vec![value.clone()],
                lower: Bound::Included(value.clone()),
                upper: Bound::Excluded(value),
            },
        ]
    }
    let large = Value::List(vec![
        Value::Text("x".repeat(MAX_LOGICAL_RENDER_BYTES + 1)),
        Value::NatBig(crate::types::NatBig::from_biguint(
            num_bigint::BigUint::from(1_u8) << 524_288,
        )),
    ]);
    for (small_access, large_access) in paths(Value::Null).into_iter().zip(paths(large.clone())) {
        let mut small = project(
            &grouped_query(),
            &request(Resource::TemporaryBytes, 16_000_000),
        )
        .unwrap();
        let mut large_plan = small.clone();
        small.access = small_access;
        large_plan.access = large_access;
        small.predicate = ExplainPredicate::TextContains {
            field: "note".into(),
            value: Value::Null,
        };
        large_plan.predicate = ExplainPredicate::TextContains {
            field: "note".into(),
            value: large.clone(),
        };
        if let ExplainGrouping::Grouped {
            having: Some(having),
            ..
        } = &mut large_plan.grouping
        {
            having.expr = Expr::Literal(large.clone());
        }
        for render in [
            ExplainPlan::render_text_canonical,
            ExplainPlan::render_json_canonical,
        ] {
            assert_eq!(render(&small).unwrap(), render(&large_plan).unwrap());
        }
        let json = large_plan.render_json_canonical().unwrap();
        for expected in [
            r#""has_predicate":true"#,
            r#""has_having":true"#,
            r#""filter_expr":"TRUE"#,
            r#""field":"profile.name"#,
            "quote''s λ",
        ] {
            assert!(json.contains(expected), "missing {expected} in {json}");
        }
    }
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

fn scalar_query() -> AccessPlannedQuery {
    let mut query = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut query.logical else {
        unreachable!()
    };
    scalar.filter_expr = Some(Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field("owner".into())),
        right: Box::new(Expr::Literal(Value::Text("quote's λ".into()))),
    });
    scalar.predicate = Some(Predicate::eq(
        "owner".into(),
        Value::Text("quote's λ".into()),
    ));
    scalar.order = Some(OrderSpec {
        fields: vec![OrderTerm::new(
            Expr::Field("owner".into()),
            OrderDirection::Asc,
        )],
    });
    query
}

fn grouped_query() -> AccessPlannedQuery {
    let mut query = scalar_query();
    query.logical = LogicalPlan::Grouped(GroupPlan {
        scalar: query.scalar_plan().clone(),
        group: GroupSpec {
            group_fields: GroupFieldSet::PathAware(vec![GroupField::scalar_path_for_test(
                "profile.name",
                "profile",
                vec!["name".into()],
                1,
                AcceptedFieldKind::Text { max_len: Some(64) },
            )]),
            aggregates: vec![
                GroupAggregateSpec::from_aggregate_expr(count()),
                GroupAggregateSpec::from_aggregate_expr(
                    min_by("amount")
                        .distinct()
                        .with_filter_expr(Expr::Literal(Value::Bool(true))),
                ),
            ],
            execution: GroupedExecutionConfig::planner_default_bounded(),
        },
        having_expr: Some(Expr::Literal(Value::Bool(true))),
    });
    query
}

fn project(
    query: &AccessPlannedQuery,
    root: &RequestExecutionRoot,
) -> Result<ExplainPlan, QueryError> {
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        query.project_explain(work)
    })
}

#[test]
fn explain_preserves_scalar_and_grouped_continuation_identity() {
    let mut predicate_only = scalar_query();
    if let LogicalPlan::Scalar(scalar) = &mut predicate_only.logical {
        scalar.filter_expr = None;
    }
    let mut grouped_having = grouped_query();
    if let LogicalPlan::Grouped(grouped) = &mut grouped_having.logical {
        grouped.having_expr = Some(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Aggregate(
                min_by("amount")
                    .distinct()
                    .with_filter_expr(Expr::Literal(Value::Bool(true))),
            )),
            right: Box::new(Expr::Literal(Value::Nat64(7))),
        });
    }
    for (continuation, query) in [
        (
            "93e5a878df31ba54250a3a275a81191859b2aa0e63b68b8602628b11ad8d2f9c",
            AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore),
        ),
        (
            "5ef8f23e8a427a0df7a43d2ee49c9443e06da077acfed0732b48686efde58866",
            scalar_query(),
        ),
        (
            "bf2a010f1bca6f7cfd85658aa26c2fd2c680deca4caea914a200603afb2752d3",
            predicate_only,
        ),
        (
            "d650ce52f3c6c0b4a9824e24ecce67c739fe05173a58bb7e4618f62a534493bf",
            grouped_query(),
        ),
        (
            "53e168dfd90b00a36e91b6a889c629ff1f14e07baa16cfffc5b113fb3608eae9",
            grouped_having,
        ),
    ] {
        let root = request(Resource::TemporaryBytes, 16_000_000);
        let plan = project(&query, &root).unwrap();
        assert_eq!(
            query.continuation_signature("tests::Entity").to_string(),
            continuation
        );
        drop(query);
        let bytes = root.observed(Resource::TemporaryBytes);
        let steps = root.observed(Resource::PredicateExpressionSteps);
        let detached = plan.clone();
        assert_eq!(detached, plan);
        assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), steps);
    }
}

#[test]
fn scalar_diagnostic_construction_needs_only_the_retained_operand_allowance() {
    let query = scalar_query();
    let root = request(Resource::NestedValueSteps, 1);
    let projected = project(&query, &root).unwrap();
    assert!(projected.filter_expr().is_some());
    assert!(matches!(
        projected.predicate(),
        ExplainPredicate::Compare { .. }
    ));
    assert_eq!(root.observed(Resource::NestedValueSteps), 1);
    let text = projected.render_text_canonical().unwrap();
    assert!(project(&query, &root).is_err());
    assert_eq!(projected.render_text_canonical().unwrap(), text);
}

#[test]
fn explain_projection_charges_repeated_borrowed_calls_without_changing_identity() {
    let mut predicate_only = scalar_query();
    let LogicalPlan::Scalar(scalar) = &mut predicate_only.logical else {
        unreachable!()
    };
    scalar.filter_expr = None;
    for query in [scalar_query(), grouped_query(), predicate_only] {
        let before = query.clone();
        let signature = query.continuation_signature("tests::Entity");
        let generous = request(Resource::TemporaryBytes, 16_000_000);
        let expected = project(&query, &generous).unwrap();
        assert_eq!(
            expected.filter_expr,
            query
                .scalar_plan()
                .filter_expr
                .as_ref()
                .map(render_scalar_filter_expr_plan_label),
        );

        for resource in [
            Resource::TemporaryBytes,
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
        ] {
            let used = generous.observed(resource);
            assert!(used > 0);
            // Check early, intermediate and final admission failures without
            // pinning the number or ordering of internal construction charges.
            for limit in [0, used / 2, used - 1] {
                let short = request(resource, limit);
                for _ in 0..2 {
                    let error = project(&query, &short).unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                    assert_eq!(query, before);
                    assert_eq!(query.continuation_signature("tests::Entity"), signature);
                    assert_eq!(short.observed(Resource::RowsVisited), 0);
                    assert_eq!(short.observed(Resource::PlanCompilations), 0);
                }
            }
            let exact = request(resource, 2 * used);
            for _ in 0..2 {
                assert_eq!(project(&query, &exact).unwrap(), expected);
            }
            assert_eq!(exact.observed(resource), 2 * used);
            let error = project(&query, &exact).unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
            );
            assert_eq!(query, before);
            assert_eq!(query.continuation_signature("tests::Entity"), signature);
            assert_eq!(exact.observed(Resource::RowsVisited), 0);

            let exhausted = exact.observed(resource);
            assert!(expected.render_text_canonical().is_ok());
            assert!(expected.render_json_canonical().is_ok());
            assert_eq!(exact.observed(resource), exhausted);
        }
        // A fresh request projects the complete same DTO after a rejected diagnostic.
        let next = project(&query, &request(Resource::TemporaryBytes, 16_000_000)).unwrap();
        assert_eq!(next, expected);
    }
}

#[test]
fn explain_access_depth_bounds_detached_rendering_without_changing_query_identity() {
    use crate::db::query::explain::access_projection::MAX_EXPLAIN_ACCESS_DEPTH;

    let mut query = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    for level in 1..MAX_EXPLAIN_ACCESS_DEPTH {
        query.access = if level % 2 == 0 {
            AccessPlan::Union(vec![query.access])
        } else {
            AccessPlan::Intersection(vec![query.access])
        };
    }
    let root = request(Resource::TemporaryBytes, 16_000_000);
    let admitted = project(&query, &root).unwrap();
    let observed = root.observed(Resource::PredicateExpressionSteps);
    for render in [
        ExplainPlan::render_text_canonical,
        ExplainPlan::render_json_canonical,
    ] {
        assert!(render(&admitted).is_ok());
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), observed);
    }

    query.access = AccessPlan::Union(vec![query.access]);
    let before = query.clone();
    let identity = query.continuation_signature("tests::Entity");
    let error = project(&query, &root).unwrap_err();
    assert_eq!(
        error.diagnostic(),
        crate::error::InternalError::query_explain_depth_exceeded(128, 129).diagnostic(),
    );
    assert_eq!(query, before);
    assert_eq!(query.continuation_signature("tests::Entity"), identity);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert!(admitted.render_json_canonical().is_ok());
}

#[test]
fn explain_projection_rejects_order_backing_before_rendering_operands() {
    let query = scalar_query();
    let order = query.scalar_plan().order.as_ref();
    let root = request(
        Resource::TemporaryBytes,
        size_of::<ExplainOrder>() as u64 - 1,
    );
    let result = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        explain_order(order, work)
    });
    assert!(result.is_err());
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
}

#[test]
fn decision_projection_counts_composite_constraints_with_cumulative_visits() {
    let access = ExplainAccessPath::Union(vec![
        ExplainAccessPath::Intersection(vec![
            ExplainAccessPath::ByKeys { keys: vec![] },
            ExplainAccessPath::KeyRange {
                start: Value::Nat64(1),
                end: Value::Nat64(3),
            },
        ]),
        ExplainAccessPath::IndexRange {
            name: "i".into(),
            fields: vec![],
            prefix_len: 2,
            prefix: vec![],
            lower: Bound::Included(Value::Nat64(1)),
            upper: Bound::Unbounded,
        },
    ]);
    let exact = request(Resource::PredicateExpressionSteps, 10);
    let count = || {
        PreparationWork::run(&exact.scope(), Lane::Diagnostic, |work| {
            access_bound_predicate_count(&access, work)
        })
    };
    assert_eq!(count().unwrap(), 6);
    assert_eq!(count().unwrap(), 6);
    assert!(count().is_err());
    assert_eq!(exact.observed(Resource::TemporaryBytes), 0);

    let snapshot = AccessChoiceExplainSnapshot::from_planned_non_index_reason(
        crate::db::query::plan::PlannedNonIndexAccessReason::PlannerExactIndexIntersection,
    );
    let decision = crate::db::query::preparation::with_preparation_work(|work| {
        ExplainAccessDecision::from_snapshot(&access, &snapshot, work)
    })
    .unwrap();
    assert_eq!(decision.residual.access_bound_predicate_count, 6);
    assert_eq!(decision.residual.residual_predicate_count, 6);
    assert_eq!(decision.residual.burden_class, "predicate_only");
}

#[test]
fn decision_projection_rejects_selected_label_before_copying_index_payload() {
    let access = ExplainAccessPath::IndexPrefix {
        name: "large-name".repeat(1000),
        fields: vec![],
        prefix_len: 0,
        values: vec![],
    };
    let short = request(Resource::TemporaryBytes, 0);
    let error = PreparationWork::run(&short.scope(), Lane::Diagnostic, |work| {
        ExplainAccessDecision::from_snapshot(
            &access,
            &AccessChoiceExplainSnapshot::selected_index_not_projected(),
            work,
        )
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    // Only the decision visit and fixed label prefix ran, not the large name.
    assert_eq!(
        short.observed(Resource::PredicateExpressionSteps),
        1 + "IndexPrefix(".len() as u64
    );
}

#[test]
fn explain_projection_grouped_metadata_preserves_paths_aggregates_and_having() {
    let query = grouped_query();
    let root = request(Resource::TemporaryBytes, 16_000_000);
    let projected = project(&query, &root).unwrap();
    let ExplainGrouping::Grouped {
        group_fields,
        aggregates,
        having,
        ..
    } = projected.grouping()
    else {
        panic!("grouped projection required");
    };
    assert_eq!(group_fields[0].field(), "profile.name");
    assert_eq!(group_fields[0].slot_index(), 1);
    assert_eq!(group_fields[0].path.as_ref().unwrap().segments(), &["name"]);
    assert_eq!(aggregates[0].kind(), AggregateKind::Count);
    assert_eq!(aggregates[1].target_field(), Some("amount"));
    assert_eq!(aggregates[1].input_expr(), Some("amount"));
    assert_eq!(aggregates[1].filter_expr(), Some("TRUE"));
    assert!(aggregates[1].distinct());
    assert_eq!(
        &having.as_ref().unwrap().expr,
        &Expr::Literal(Value::Bool(true))
    );
}
