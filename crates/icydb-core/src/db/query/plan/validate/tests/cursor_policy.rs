//! Module: db::query::plan::validate::tests::cursor_policy
//! Covers owner-level planner cursor policy checks for paging and order shape.
//! Does not own: leaf-local cursor-policy helper implementation details.
//! Boundary: keeps planner cursor policy regressions in the validate
//! subsystem `tests/` boundary.

use crate::db::query::plan::{
    DeleteSpec, LoadSpec, OrderDirection, OrderSpec, QueryMode,
    validate::{
        CursorOrderPlanShapeError, CursorPagingPolicyError, validate_cursor_order_plan_shape,
        validate_cursor_paging_requirements,
    },
};

#[test]
fn cursor_paging_requires_order() {
    let spec = LoadSpec {
        limit: Some(10),
        offset: 0,
    };

    assert_eq!(
        validate_cursor_paging_requirements(false, spec),
        Err(CursorPagingPolicyError::CursorRequiresOrder),
        "cursor paging must require explicit ordering",
    );
}

#[test]
fn cursor_paging_requires_limit() {
    let spec = LoadSpec {
        limit: None,
        offset: 0,
    };

    assert_eq!(
        validate_cursor_paging_requirements(true, spec),
        Err(CursorPagingPolicyError::CursorRequiresLimit),
        "cursor paging must require explicit LIMIT",
    );
}

#[test]
fn cursor_order_shape_requires_explicit_order_when_requested() {
    let missing = validate_cursor_order_plan_shape(None, true);
    assert_eq!(
        missing,
        Err(CursorOrderPlanShapeError::MissingExplicitOrder),
        "missing explicit ORDER BY should fail shape validation",
    );

    let empty_order = OrderSpec { fields: Vec::new() };
    let empty = validate_cursor_order_plan_shape(Some(&empty_order), true);
    assert_eq!(
        empty,
        Err(CursorOrderPlanShapeError::EmptyOrderSpec),
        "empty ORDER BY should fail shape validation",
    );
}

#[test]
fn cursor_order_shape_accepts_valid_explicit_order() {
    let order = OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    };

    let validated = validate_cursor_order_plan_shape(Some(&order), true)
        .expect("valid explicit order should pass cursor order-shape validation")
        .expect("validated order should be present");
    assert_eq!(validated, &order);
}

#[test]
fn cursor_policy_tests_exercise_planner_mode_types() {
    let _ = QueryMode::Load(LoadSpec {
        limit: Some(1),
        offset: 0,
    });
    let _ = QueryMode::Delete(DeleteSpec {
        limit: Some(1),
        offset: 0,
    });
}
