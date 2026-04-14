//! Module: db::query::plan::validate::tests::intent_policy
//! Covers owner-level planner intent-policy checks for delete windows,
//! grouping, and key-access constraints.
//! Does not own: leaf-local intent policy rule wiring details.
//! Boundary: keeps planner intent-policy regressions in the validate
//! subsystem `tests/` boundary.

use crate::db::query::plan::{
    DeleteSpec, LoadSpec, OrderDirection, OrderSpec, QueryMode,
    validate::{
        IntentKeyAccessKind, IntentKeyAccessPolicyViolation, PolicyPlanError,
        validate_intent_key_access_policy, validate_intent_plan_shape,
    },
};

#[test]
fn delete_limit_without_order_fails_during_planning_policy_validation() {
    let mode = QueryMode::Delete(DeleteSpec {
        limit: Some(10),
        offset: 0,
    });

    assert_eq!(
        validate_intent_plan_shape(mode, None, false),
        Err(PolicyPlanError::DeleteWindowRequiresOrder),
        "delete LIMIT without ORDER BY must fail in intent/planning validation",
    );
}

#[test]
fn delete_offset_without_order_fails_during_planning_policy_validation() {
    let mode = QueryMode::Delete(DeleteSpec {
        limit: None,
        offset: 1,
    });
    let order = OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    };

    assert_eq!(
        validate_intent_plan_shape(mode, None, false),
        Err(PolicyPlanError::DeleteWindowRequiresOrder),
        "delete OFFSET without ORDER BY must fail in intent/planning validation",
    );

    validate_intent_plan_shape(mode, Some(&order), false)
        .expect("ordered delete OFFSET should pass intent/planning validation");
}

#[test]
fn delete_limit_and_offset_with_order_passes_planning_policy_validation() {
    let mode = QueryMode::Delete(DeleteSpec {
        limit: Some(2),
        offset: 1,
    });
    let order = OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    };

    validate_intent_plan_shape(mode, Some(&order), false)
        .expect("ordered delete LIMIT/OFFSET should pass intent/planning validation");
}

#[test]
fn delete_offset_with_order_does_not_fail_planning_policy_validation() {
    let mode = QueryMode::Delete(DeleteSpec {
        limit: None,
        offset: 1,
    });
    let order = OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    };

    validate_intent_plan_shape(mode, Some(&order), false)
        .expect("ordered delete OFFSET should pass intent/planning validation");
}

#[test]
fn delete_grouping_shape_fails_during_planning_policy_validation() {
    let mode = QueryMode::Delete(DeleteSpec {
        limit: None,
        offset: 0,
    });
    let order = OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    };

    assert_eq!(
        validate_intent_plan_shape(mode, Some(&order), true),
        Err(PolicyPlanError::DeletePlanWithGrouping),
        "delete GROUP BY/HAVING shape must fail in intent/planning validation",
    );
}

#[test]
fn load_mode_allows_ordered_shape_in_intent_policy() {
    let mode = QueryMode::Load(LoadSpec {
        limit: Some(5),
        offset: 0,
    });
    let order = OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    };

    validate_intent_plan_shape(mode, Some(&order), false)
        .expect("ordered load shape should pass intent/planning policy validation");
}

#[test]
fn by_ids_with_predicate_fails_during_planning_policy_validation() {
    assert_eq!(
        validate_intent_key_access_policy(true, None, false),
        Err(IntentKeyAccessPolicyViolation::KeyAccessConflict),
        "conflicting key-access selectors must fail in planner key-access policy",
    );

    assert_eq!(
        validate_intent_key_access_policy(false, Some(IntentKeyAccessKind::Many), true),
        Err(IntentKeyAccessPolicyViolation::ByIdsWithPredicate),
        "by_ids + predicate must fail in planner key-access policy",
    );
}

#[test]
fn only_with_predicate_fails_during_planning_policy_validation() {
    assert_eq!(
        validate_intent_key_access_policy(false, Some(IntentKeyAccessKind::Only), true),
        Err(IntentKeyAccessPolicyViolation::OnlyWithPredicate),
        "only + predicate must fail in planner key-access policy",
    );

    validate_intent_key_access_policy(false, Some(IntentKeyAccessKind::Single), true)
        .expect("single key + predicate remains a valid planner key-access shape");
}
