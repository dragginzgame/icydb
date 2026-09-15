//! Route and retained order contracts use the same borrowed direction policy.

use super::derive_load_route_direction;
use crate::db::{
    direction::Direction,
    predicate::MissingRowPolicy,
    query::plan::{
        AccessPlannedQuery, ExecutionOrderContract, GroupFieldSet, GroupPlan, GroupSpec,
        GroupedExecutionConfig, LogicalPlan, OrderDirection, OrderSpec, OrderTerm,
        expr::{Expr, FieldPath},
    },
};

fn plan(order: Option<OrderSpec>, grouped: bool) -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.order = order;
    if grouped {
        plan.logical = LogicalPlan::Grouped(GroupPlan {
            scalar: scalar.clone(),
            group: GroupSpec {
                group_fields: GroupFieldSet::Direct(vec![]),
                aggregates: vec![],
                execution: GroupedExecutionConfig::planner_default_bounded(),
            },
            having_expr: None,
        });
    }
    plan
}

#[test]
fn route_direction_preserves_defaults_and_leading_term_policy() {
    let cases = [
        (None, Direction::Asc),
        (Some(OrderSpec { fields: vec![] }), Direction::Asc),
        (
            Some(OrderSpec {
                fields: vec![OrderTerm::field("id", OrderDirection::Asc)],
            }),
            Direction::Asc,
        ),
        (
            Some(OrderSpec {
                fields: vec![
                    OrderTerm::field("id", OrderDirection::Desc),
                    OrderTerm::field("other", OrderDirection::Asc),
                ],
            }),
            Direction::Desc,
        ),
        (
            Some(OrderSpec {
                fields: vec![
                    OrderTerm::new(
                        Expr::FieldPath(FieldPath::new("profile", vec!["rank".into()])),
                        OrderDirection::Asc,
                    ),
                    OrderTerm::field("id", OrderDirection::Desc),
                ],
            }),
            Direction::Asc,
        ),
    ];
    for (order, expected) in cases {
        for grouped in [false, true] {
            let plan = plan(order.clone(), grouped);
            assert_eq!(derive_load_route_direction(&plan), expected);
            assert_eq!(
                crate::db::query::preparation::with_preparation_work(|work| {
                    ExecutionOrderContract::from_plan(
                        grouped,
                        plan.scalar_plan().order.as_ref(),
                        work,
                    )
                })
                .unwrap()
                .direction(),
                expected,
            );
        }
    }
}

#[test]
#[cfg(feature = "sql")]
fn aggregate_direction_preserves_extrema_overrides_and_order_fallback() {
    use super::derive_aggregate_route_direction;
    use crate::db::{executor::planning::route::AggregateRouteShape, query::plan::AggregateKind};

    for direction in [OrderDirection::Asc, OrderDirection::Desc] {
        let plan = plan(
            Some(OrderSpec {
                fields: vec![OrderTerm::field("id", direction)],
            }),
            false,
        );
        let fallback = derive_load_route_direction(&plan);
        for kind in [
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::Count,
            AggregateKind::Sum,
        ] {
            for target in [None, Some("amount")] {
                let aggregate = AggregateRouteShape::new_resolved(kind, target, true, true, false);
                let expected = match (target, kind) {
                    (Some(_), AggregateKind::Min) => Direction::Asc,
                    (Some(_), AggregateKind::Max) => Direction::Desc,
                    _ => fallback,
                };
                assert_eq!(derive_aggregate_route_direction(&plan, aggregate), expected);
            }
        }
    }
}
