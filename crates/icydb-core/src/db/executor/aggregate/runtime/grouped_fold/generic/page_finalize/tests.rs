use super::{
    GroupedPageCandidate, GroupedPageCandidateRanking, ResolvedGroupedTopKOrderTerm,
    finalize_grouped_page_rows_with_shaper,
};
use crate::{
    db::{
        executor::{
            RuntimeGroupedRow,
            group::GroupKey,
            projection::{CompiledGroupedProjectionPlan, compile_grouped_projection_plan},
        },
        query::{
            builder::aggregate::{count, max_by},
            plan::{
                AggregateKind, FieldSlot, GroupedAggregateExecutionSpec, PlannedProjectionLayout,
                expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
            },
        },
    },
    value::Value,
};

#[test]
fn top_k_compilation_skips_unknown_fields_but_preserves_budget_failure() {
    use crate::db::{
        QueryError,
        direction::Direction,
        executor::pipeline::contracts::GroupedRouteStage,
        query::plan::{GroupedExecutionRoute, LogicalPlan, OrderDirection, OrderSpec, OrderTerm},
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope as Scope,
        DiagnosticExecutionLane as Lane, DiagnosticFactTag,
    };
    let mut route = GroupedRouteStage::new_for_test(Direction::Asc, Some(3));
    route.planner_payload.grouped_execution_route = GroupedExecutionRoute::GenericTopK;
    route.planner_payload.group_fields =
        crate::db::query::plan::GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "age")]);
    let LogicalPlan::Grouped(plan) = &mut std::rc::Rc::get_mut(&mut route.planner_payload.plan)
        .unwrap()
        .logical
    else {
        panic!("test route is grouped")
    };
    plan.scalar.order = Some(OrderSpec {
        fields: vec![
            OrderTerm::field("unknown", OrderDirection::Asc),
            OrderTerm::field("age", OrderDirection::Desc),
        ],
    });
    for (resource, limit) in [
        (Resource::PredicateExpressionSteps, 0),
        (Resource::TemporaryBytes, 0),
        (Resource::PredicateExpressionSteps, 16_000_000),
    ] {
        let result = with_query_execution_budget_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(resource, limit),
            HardExecutionContext::new(Scope::Execution, Lane::PublicRead, 0),
            || super::compile_grouped_top_k_order(&route).map_err(QueryError::execute),
        );
        if limit == 0 {
            assert!(
                result
                    .err()
                    .unwrap()
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
        } else {
            let compiled = result.unwrap().unwrap();
            assert_eq!(compiled.terms.len(), 1);
            assert_eq!(compiled.terms[0].direction, OrderDirection::Desc);
            assert!(matches!(
                compiled.terms[0].expr,
                crate::db::query::plan::expr::CompiledExpr::GroupKey { offset: 0, .. }
            ));
        }
    }
}

#[test]
fn finalize_grouped_page_rows_from_candidates_projects_directly_from_candidates() {
    let projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("age")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Aggregate(max_by("score")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Aggregate(count()),
            alias: None,
        },
    ]);
    let projection_layout = PlannedProjectionLayout {
        group_field_positions: vec![0],
        aggregate_positions: vec![1, 2],
    };
    let group_fields =
        crate::db::query::plan::GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "age")]);
    let aggregate_execution_specs = [
        GroupedAggregateExecutionSpec::from_test_inputs(AggregateKind::Count, None, None, false),
        GroupedAggregateExecutionSpec::from_test_inputs(
            AggregateKind::Max,
            Some(FieldSlot::from_test_slot(1, "score")),
            Some("score"),
            false,
        ),
    ];
    let compiled_projection = crate::db::query::preparation::with_preparation_work(|work| {
        compile_grouped_projection_plan(
            &projection,
            &group_fields,
            aggregate_execution_specs.as_slice(),
            work,
        )
    })
    .expect("grouped projection should compile");
    let grouped_projection =
        CompiledGroupedProjectionPlan::from_test_inputs(compiled_projection, &projection_layout);
    let candidates = vec![GroupedPageCandidate {
        group_key: GroupKey::from_group_values(vec![Value::Nat64(21)])
            .expect("candidate group key"),
        aggregate_values: vec![Value::Nat64(2), Value::Nat64(90)],
        ranking: super::GroupedPageCandidateRanking::Canonical {
            direction: crate::db::direction::Direction::Asc,
        },
    }];

    let (rows, next_cursor_boundary) = finalize_grouped_page_rows_with_shaper(
        candidates.into_iter(),
        None,
        0,
        |_| Ok(true),
        |candidate| {
            crate::db::executor::aggregate::runtime::grouped_output::project_grouped_values_from_compiled_projection(
                &grouped_projection,
                candidate.group_key_values()?,
                candidate.aggregate_values.as_slice(),
            )
        },
    )
    .expect("candidate projection should succeed");

    assert_eq!(
        rows,
        vec![RuntimeGroupedRow::new(
            vec![Value::Nat64(21)],
            vec![Value::Nat64(90), Value::Nat64(2)],
        )]
    );
    assert_eq!(next_cursor_boundary, None);
}

#[test]
fn grouped_top_k_candidate_counts_owned_order_value_backing() {
    let candidate = GroupedPageCandidate {
        group_key: GroupKey::from_group_values(vec![Value::Text("group".repeat(64))])
            .expect("candidate group key"),
        aggregate_values: vec![Value::Text("aggregate".repeat(64))],
        ranking: GroupedPageCandidateRanking::TopK {
            terms: vec![ResolvedGroupedTopKOrderTerm {
                value: Value::Text("order".repeat(64)),
                direction: crate::db::query::plan::OrderDirection::Asc,
            }],
        },
    };

    assert!(candidate.estimated_backing_bytes() > 1_000);
}

fn aggregate_only_projection(
    layout: &PlannedProjectionLayout,
) -> CompiledGroupedProjectionPlan<'_> {
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(count()),
        alias: None,
    }]);
    let specs = [GroupedAggregateExecutionSpec::from_test_inputs(
        AggregateKind::Count,
        None,
        None,
        false,
    )];
    let compiled = crate::db::query::preparation::with_preparation_work(|work| {
        compile_grouped_projection_plan(
            &projection,
            &crate::db::query::plan::GroupFieldSet::empty(),
            &specs,
            work,
        )
    })
    .unwrap();
    CompiledGroupedProjectionPlan::from_test_inputs(compiled, layout)
}

fn select_projected_page(
    route: &super::GroupedRouteStage,
    keys: &[GroupKey],
) -> (Vec<RuntimeGroupedRow>, Option<Vec<Value>>) {
    let layout = PlannedProjectionLayout {
        group_field_positions: vec![],
        aggregate_positions: vec![0],
    };
    let projection = ProjectionSpec::default();
    let mut ordered = super::OrderedGroupedPageSelection::new(route, &projection, 1).unwrap();
    ordered.selection.compiled_projection = Some(aggregate_only_projection(&layout));
    let candidates = keys
        .iter()
        .map(|key| GroupedPageCandidate {
            group_key: key.clone(),
            aggregate_values: vec![Value::Nat64(1)],
            ranking: GroupedPageCandidateRanking::Canonical {
                direction: route.direction(),
            },
        })
        .collect::<Vec<_>>();
    for candidate in &candidates {
        if ordered
            .push_finalized_values(
                candidate.group_key.clone(),
                candidate.aggregate_values.clone(),
            )
            .unwrap()
        {
            break;
        }
    }
    let (ordered_rows, cursor) = ordered.finish(route).unwrap();
    let (rows, next) = finalize_grouped_page_rows_with_shaper(
        candidates.into_iter(),
        route.grouped_pagination_window().limit(),
        route.grouped_pagination_window().initial_offset_for_page(),
        |candidate| candidate.matches_window(None, route.grouped_resume_boundary()),
        |candidate| Ok(RuntimeGroupedRow::new(vec![], candidate.aggregate_values)),
    )
    .unwrap();
    assert_eq!(ordered_rows, rows);
    assert_eq!(
        cursor
            .as_ref()
            .map(super::GroupedContinuationToken::last_group_key),
        next.as_deref()
    );
    (rows, next)
}

#[test]
fn projected_pages_resume_from_canonical_keys_in_both_selectors() {
    use crate::db::{
        cursor::ContinuationSignature,
        direction::Direction,
        executor::{
            GroupedContinuationContext, GroupedPaginationWindow,
            pipeline::contracts::GroupedRouteStage,
        },
    };
    for direction in [Direction::Asc, Direction::Desc] {
        for compound in [false, true] {
            let mut keys = vec![
                Value::Null,
                Value::Int64(-1),
                Value::Nat64(2),
                Value::Text("界".into()),
            ]
            .into_iter()
            .map(|value| {
                let mut key = vec![value];
                if compound {
                    key.push(Value::Text("e\u{301}".into()));
                }
                GroupKey::from_group_values(key).unwrap()
            })
            .collect::<Vec<_>>();
            keys.sort_by(|left, right| {
                super::compare_grouped_boundary_values(
                    direction,
                    left.canonical_value(),
                    right.canonical_value(),
                )
            });
            for limit in [None, Some(0), Some(1), Some(2), Some(4), Some(20)] {
                for offset in [0, 1, 9] {
                    let mut boundary = None;
                    let mut observed = Vec::new();
                    loop {
                        let window = GroupedPaginationWindow::new(
                            limit,
                            if boundary.is_none() { offset } else { 0 },
                            None,
                            0,
                            boundary.clone().map(Value::List),
                        );
                        let mut route = GroupedRouteStage::new_for_test(direction, None);
                        route.continuation = GroupedContinuationContext::new(
                            ContinuationSignature::from_bytes([0; 32]),
                            if compound { 2 } else { 1 },
                            window,
                            direction,
                        );
                        let (rows, next) = select_projected_page(&route, &keys);
                        // Expected progress follows the original ordered keys,
                        // independently of the boundary emitted by either selector.
                        let remaining = &keys[offset.min(keys.len()) + observed.len()..];
                        let emitted = remaining
                            .iter()
                            .take(limit.unwrap_or(usize::MAX))
                            .collect::<Vec<_>>();
                        assert_eq!(rows.len(), emitted.len());
                        observed.extend(emitted.iter().map(|key| key.canonical_value().clone()));
                        let expected_boundary = if emitted.len() < remaining.len() {
                            emitted.last().map(|key| match key.canonical_value() {
                                Value::List(values) => values.clone(),
                                _ => panic!("group key is a list"),
                            })
                        } else {
                            None
                        };
                        assert_eq!(next, expected_boundary);
                        let Some(next) = next else {
                            break;
                        };
                        assert!(observed.len() <= keys.len(), "continuation must advance");
                        boundary = Some(next);
                    }
                    let expected = if limit == Some(0) {
                        vec![]
                    } else {
                        keys.iter()
                            .skip(offset)
                            .map(|key| key.canonical_value().clone())
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(observed, expected);
                }
            }
        }
    }
}
