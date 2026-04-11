//! Module: db::query::plan::validate::grouped::policy::tests
//! Covers grouped policy validation rules and rejection behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::db::{
    predicate::CompareOp,
    predicate::MissingRowPolicy,
    query::plan::{
        DeleteSpec, FieldSlot, GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, LoadSpec,
        LogicalPlan, OrderDirection, OrderSpec, QueryMode,
    },
};
use crate::value::Value;

fn scalar_plan(distinct: bool) -> ScalarPlan {
    ScalarPlan {
        mode: QueryMode::Load(LoadSpec {
            limit: None,
            offset: 0,
        }),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        distinct,
        delete_limit: None,
        page: None,
        consistency: MissingRowPolicy::Ignore,
    }
}

#[test]
fn grouped_distinct_without_adjacency_proof_fails_in_planner_policy() {
    let err = validate_grouped_distinct_policy(&scalar_plan(true), false)
        .expect_err("grouped DISTINCT without adjacency proof must fail in planner policy");

    assert!(matches!(
        err,
        PlanError::Policy(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanPolicyError::Group(group)
                    if matches!(
                        group.as_ref(),
                        GroupPlanError::DistinctAdjacencyEligibilityRequired
                    )
            )
    ));
}

#[test]
fn grouped_distinct_with_having_fails_in_planner_policy() {
    let err = validate_grouped_distinct_policy(&scalar_plan(true), true)
        .expect_err("grouped DISTINCT + HAVING must fail in planner policy");

    assert!(matches!(
        err,
        PlanError::Policy(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanPolicyError::Group(group)
                    if matches!(group.as_ref(), GroupPlanError::DistinctHavingUnsupported)
            )
    ));
}

#[test]
fn grouped_non_distinct_shape_passes_planner_distinct_policy_gate() {
    validate_grouped_distinct_policy(&scalar_plan(false), false)
        .expect("non-distinct grouped shapes should pass planner distinct policy gate");
}

#[test]
fn grouped_having_contains_operator_fails_in_planner_policy() {
    let having = GroupHavingSpec {
        clauses: vec![GroupHavingClause {
            symbol: GroupHavingSymbol::GroupField(FieldSlot {
                index: 0,
                field: "team".to_string(),
                kind: None,
            }),
            op: CompareOp::Contains,
            value: Value::Text("A".to_string()),
        }],
    };

    let err = validate_grouped_having_policy(Some(&having))
        .expect_err("grouped HAVING with unsupported compare operator must fail in planner");

    assert!(matches!(
        err,
        PlanError::Policy(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanPolicyError::Group(group)
                    if matches!(
                        group.as_ref(),
                        GroupPlanError::HavingUnsupportedCompareOp { index: 0, .. }
                    )
            )
    ));
}

#[test]
fn grouped_policy_tests_track_planner_logical_mode_contract() {
    // Keep grouped-policy tests compile-time linked to logical mode contracts.
    let _ = LogicalPlan::Scalar(ScalarPlan {
        mode: QueryMode::Delete(DeleteSpec { limit: Some(1) }),
        predicate: None,
        order: None,
        distinct: false,
        delete_limit: None,
        page: None,
        consistency: MissingRowPolicy::Ignore,
    });
}
