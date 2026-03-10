//! Module: db::query::plan::validate::grouped::cursor::tests
//! Responsibility: module-local ownership and contracts for db::query::plan::validate::grouped::cursor::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::db::{
    predicate::MissingRowPolicy,
    query::plan::{
        AggregateKind, GroupAggregateSpec, GroupedExecutionConfig, LoadSpec, OrderDirection,
        PageSpec, QueryMode,
    },
};

fn grouped_spec() -> GroupSpec {
    GroupSpec {
        group_fields: vec![FieldSlot {
            index: 0,
            field: "team".to_string(),
        }],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig {
            max_groups: 128,
            max_group_bytes: 8 * 1024,
        },
    }
}

fn scalar_with_group_order(order_fields: Vec<(String, OrderDirection)>) -> ScalarPlan {
    ScalarPlan {
        mode: QueryMode::Load(LoadSpec {
            limit: Some(10),
            offset: 0,
        }),
        predicate: None,
        order: Some(OrderSpec {
            fields: order_fields,
        }),
        distinct: false,
        delete_limit: None,
        page: None,
        consistency: MissingRowPolicy::Ignore,
    }
}

#[test]
fn grouped_order_requires_limit_in_planner_cursor_policy() {
    let logical = scalar_with_group_order(vec![("team".to_string(), OrderDirection::Asc)]);
    let group = grouped_spec();

    let err = validate_group_cursor_constraints(&logical, &group)
        .expect_err("grouped ORDER BY without LIMIT must fail in planner cursor policy");

    assert!(matches!(
        err,
        PlanError::Policy(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanPolicyError::Group(group)
                    if matches!(group.as_ref(), GroupPlanError::OrderRequiresLimit)
            )
    ));
}

#[test]
fn grouped_order_prefix_must_align_with_group_keys_in_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![("id".to_string(), OrderDirection::Asc)]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let group = grouped_spec();

    let err = validate_group_cursor_constraints(&logical, &group)
        .expect_err("grouped ORDER BY not prefixed by GROUP BY keys must fail in planner");

    assert!(matches!(
        err,
        PlanError::Policy(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanPolicyError::Group(group)
                    if matches!(group.as_ref(), GroupPlanError::OrderPrefixNotAlignedWithGroupKeys)
            )
    ));
}

#[test]
fn grouped_order_prefix_alignment_with_limit_passes_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![
        ("team".to_string(), OrderDirection::Asc),
        ("id".to_string(), OrderDirection::Asc),
    ]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let group = grouped_spec();

    validate_group_cursor_constraints(&logical, &group).expect(
        "grouped ORDER BY with LIMIT and group-key-aligned prefix should pass planner policy",
    );
}
