//! Required diagnostic observations preserve the shared strategy and stop on failure.

use super::*;
use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{MissingRowPolicy, Predicate},
        query::{
            admission::input::MAX_QUERY_INPUT_DEPTH,
            builder::aggregate::count,
            plan::{
                FieldSlot, GroupField, GroupSpec, GroupedExecutionConfig, LogicalPlan,
                OrderDirection, OrderTerm,
                expr::{BinaryOp, CaseWhenArm, Expr, FieldPath},
            },
        },
        schema::AcceptedFieldKind,
    },
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticExecutionLane as Lane, DiagnosticFactTag};

fn request(limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::PredicateExpressionSteps, limit),
    )
}

fn query() -> AccessPlannedQuery {
    let mut query = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    query.logical = LogicalPlan::Grouped(GroupPlan {
        scalar: query.scalar_plan().clone(),
        group: GroupSpec {
            group_fields: GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
                0,
                "key",
                AcceptedFieldKind::Int32,
            )]),
            aggregates: vec![GroupAggregateSpec::from_aggregate_expr(count())],
            execution: GroupedExecutionConfig::planner_default_bounded(),
        },
        having_expr: None,
    });
    query
}

fn cases() -> Vec<AccessPlannedQuery> {
    let mut cases = vec![];
    for case in 0..9 {
        let mut query = query();
        let LogicalPlan::Grouped(grouped) = &mut query.logical else {
            unreachable!()
        };
        grouped.scalar.order = Some(OrderSpec {
            fields: vec![OrderTerm::field("key", OrderDirection::Asc)],
        });
        match case {
            0 => {}
            1 => {
                grouped.scalar.order = Some(OrderSpec {
                    fields: vec![OrderTerm::new(
                        Expr::Aggregate(count()),
                        OrderDirection::Desc,
                    )],
                });
            }
            2 => grouped.scalar.distinct = true,
            3 => grouped.scalar.predicate = Some(Predicate::eq("other".into(), Value::Nat64(1))),
            4 => {
                grouped.group.aggregates =
                    vec![GroupAggregateSpec::from_aggregate_expr(count().distinct())];
            }
            5 => {
                grouped.having_expr = Some(Expr::Binary {
                    op: BinaryOp::Or,
                    left: Box::new(Expr::Literal(Value::Bool(true))),
                    right: Box::new(Expr::Literal(Value::Bool(false))),
                });
            }
            6 => {
                grouped.scalar.order = Some(OrderSpec {
                    fields: vec![OrderTerm::field("other", OrderDirection::Asc)],
                });
            }
            7 => {
                let mut having = Expr::Literal(Value::Bool(true));
                for _ in 1..MAX_QUERY_INPUT_DEPTH {
                    having = Expr::Binary {
                        op: BinaryOp::And,
                        left: Box::new(having),
                        right: Box::new(Expr::Literal(Value::Bool(true))),
                    };
                }
                grouped.having_expr = Some(having);
            }
            8 => {
                grouped.group.group_fields =
                    GroupFieldSet::PathAware(vec![GroupField::scalar_path_for_test(
                        "profile.key",
                        "profile",
                        vec!["key".into()],
                        0,
                        AcceptedFieldKind::Int32,
                    )]);
                grouped.scalar.order = Some(OrderSpec {
                    fields: vec![OrderTerm::new(
                        Expr::FieldPath(FieldPath::new("profile", vec!["key".into()])),
                        OrderDirection::Asc,
                    )],
                });
            }
            _ => unreachable!(),
        }
        cases.push(query);
    }
    cases
}

fn project(
    query: &AccessPlannedQuery,
    root: &RequestExecutionRoot,
) -> Result<GroupedPlanStrategy, QueryError> {
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        grouped_plan_strategy_for_explain(query, query.grouped_plan().unwrap(), work)
    })
}

fn assert_budget_error(error: &QueryError) {
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw(),
    )));
}

#[test]
fn grouped_strategy_diagnostic_work_is_cumulative_and_identity_independent() {
    for query in cases() {
        let before = query.clone();
        let identity = query.continuation_signature("test::Entity");
        let expected = grouped_plan_strategy(&query).unwrap();
        let generous = request(16_000_000);
        assert_eq!(project(&query, &generous).unwrap(), expected);
        let used = generous.observed(Resource::PredicateExpressionSteps);
        assert!(used > 0);
        assert_eq!(generous.observed(Resource::TemporaryBytes), 0);
        assert_eq!(generous.observed(Resource::RowsVisited), 0);

        let short = request(used - 1);
        assert_budget_error(&project(&query, &short).unwrap_err());
        let exact = request(2 * used);
        for _ in 0..2 {
            assert_eq!(project(&query, &exact).unwrap(), expected);
        }
        assert_eq!(exact.observed(Resource::PredicateExpressionSteps), 2 * used);
        assert_budget_error(&project(&query, &exact).unwrap_err());
        assert_eq!(grouped_plan_strategy(&query), Some(expected));
        assert_eq!(query.continuation_signature("test::Entity"), identity);
        assert_eq!(query, before);
    }
}

#[test]
fn grouped_strategy_observer_failure_stops_at_every_boundary() {
    for query in cases() {
        let grouped = query.grouped_plan().unwrap();
        let mut trace = vec![];
        let expected = derive_grouped_plan_strategy(&query, grouped, &mut |steps| {
            trace.push(steps);
            Ok::<_, usize>(())
        })
        .unwrap();
        assert_eq!(Some(expected), grouped_plan_strategy(&query));
        for stop in 0..trace.len() {
            let mut visits = 0;
            let result = derive_grouped_plan_strategy(&query, grouped, &mut |steps| {
                assert_eq!(steps, trace[visits]);
                let current = visits;
                visits += 1;
                if current == stop { Err(stop) } else { Ok(()) }
            });
            assert_eq!(result, Err(stop));
            assert_eq!(visits, stop + 1);
        }
    }
}

#[test]
fn grouped_explain_rejects_strategy_before_allocating_a_dto() {
    let query = query();
    let before = query.continuation_signature("test::Entity");
    let root = request(1); // The outer explain visit consumes the allowance.
    let error = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        query.project_explain(work)
    })
    .unwrap_err();
    assert_budget_error(&error);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(query.continuation_signature("test::Entity"), before);
}

#[test]
fn grouped_strategy_field_comparisons_admit_bytes_before_matching() {
    let name = "k".repeat(32_768);
    let fields = GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
        0,
        &name,
        AcceptedFieldKind::Int32,
    )]);
    let expr = Expr::Field(name.clone().into());
    let mut trace = vec![];
    let result = try_classify_grouped_top_k_order_term(&expr, &fields, &mut |steps| {
        trace.push(steps);
        if steps > 1 { Err(()) } else { Ok(()) }
    });
    assert_eq!(result, Err(()));
    assert_eq!(trace, [1, 1 + name.len() as u64]);
}

#[test]
fn grouped_strategy_expression_walk_stops_on_false_or_error_in_case_order() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Field("condition".into()),
            Expr::Field("result".into()),
        )],
        else_expr: Box::new(Expr::Field("else".into())),
    };
    for fail in [false, true] {
        let mut visited = vec![];
        let result = expr.try_all_tree_expr(&mut |node| {
            if let Expr::Field(field) = node {
                visited.push(field.as_str().to_string());
                if field.as_str() == "result" {
                    return if fail { Err(()) } else { Ok(false) };
                }
            }
            Ok(true)
        });
        assert_eq!(visited, ["condition", "result"]);
        assert_eq!(result, if fail { Err(()) } else { Ok(false) });
    }
    let mut visited = vec![];
    expr.try_for_each_tree_expr(&mut |node| {
        if let Expr::Field(field) = node {
            visited.push(field.as_str().to_string());
        }
        Ok::<_, ()>(())
    })
    .unwrap();
    assert_eq!(visited, ["condition", "result", "else"]);
}
