//! Module: db::query::plan::tests::continuation
//! Covers planner continuation-window contracts and grouped cursor-policy
//! projections.
//! Does not own: executor-side continuation behavior outside the plan
//! boundary.
//! Boundary: exercises continuation semantics from the owner `tests/`
//! boundary because the assertions span access, cursor, and plan contracts.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::{
            ContinuationSignature, CursorPlanError, GroupedContinuationToken, GroupedPlannedCursor,
        },
        direction::Direction,
        query::plan::{
            ExecutionOrderContract, ExecutionShapeSignature, GroupedCursorPolicyViolation,
            PlannedContinuationContract, ScalarAccessWindowPlan,
        },
    },
    value::Value,
};

fn continuation_signature_fixture() -> ContinuationSignature {
    ContinuationSignature::from_bytes([0x11; 32])
}

fn grouped_contract(
    violation: Option<GroupedCursorPolicyViolation>,
) -> PlannedContinuationContract {
    PlannedContinuationContract::new(
        ExecutionShapeSignature::new(continuation_signature_fixture()),
        1,
        4,
        ExecutionOrderContract::from_plan(true, None),
        Some(2),
        AccessPlan::path(AccessPath::FullScan),
        violation,
    )
}

fn applied_grouped_cursor(contract: &PlannedContinuationContract) -> GroupedPlannedCursor {
    GroupedPlannedCursor::new(vec![Value::Uint(7)], contract.expected_initial_offset())
}

#[test]
fn scalar_access_window_fetch_count_unbounded_remains_unbounded() {
    let window = ScalarAccessWindowPlan::new(3, None);

    assert_eq!(window.fetch_count(), None);
}

#[test]
fn scalar_access_window_fetch_count_bounded_adds_lookahead_row() {
    let window = ScalarAccessWindowPlan::new(3, Some(2));

    assert_eq!(window.keep_count(), Some(5));
    assert_eq!(window.fetch_count(), Some(6));
}

#[test]
fn scalar_access_window_fetch_count_limit_zero_projects_zero_lookahead() {
    let window = ScalarAccessWindowPlan::new(4, Some(0));

    assert_eq!(window.keep_count(), Some(4));
    assert_eq!(window.fetch_count(), Some(0));
}

#[test]
fn grouped_cursor_contract_shares_policy_gate_for_token_and_window_paths() {
    let contract = grouped_contract(Some(
        GroupedCursorPolicyViolation::ContinuationRequiresLimit,
    ));
    let continuation_token = GroupedContinuationToken::new_with_direction(
        continuation_signature_fixture(),
        vec![Value::Uint(7)],
        Direction::Asc,
        contract.expected_initial_offset(),
    );

    let token_err = contract
        .prepare_grouped_cursor_token("PlanEntity", Some(continuation_token))
        .expect_err("grouped cursor token reuse should honor grouped cursor policy");
    let window_err = contract
        .project_grouped_paging_window(&applied_grouped_cursor(&contract))
        .expect_err("grouped paging window should honor grouped cursor policy");

    assert!(matches!(
        &token_err,
        CursorPlanError::ContinuationCursorInvariantViolation { reason }
            if reason == "grouped continuation cursors require an explicit LIMIT"
    ));
    assert_eq!(
        token_err.to_string(),
        window_err.to_string(),
        "grouped token preparation and grouped paging window must project the same grouped cursor policy error",
    );
}

#[test]
fn grouped_cursor_contract_skips_policy_gate_for_initial_grouped_page() {
    let contract = grouped_contract(Some(
        GroupedCursorPolicyViolation::ContinuationRequiresLimit,
    ));

    let prepared = contract
        .prepare_grouped_cursor_token("PlanEntity", None)
        .expect("initial grouped page should not be blocked by continuation-only policy");
    let window = contract
        .project_grouped_paging_window(&GroupedPlannedCursor::none())
        .expect("initial grouped page window should not be blocked by continuation-only policy");
    let (limit, initial_offset_for_page, selection_bound, resume_initial_offset, resume_boundary) =
        window.into_parts();

    assert!(prepared.is_empty());
    assert_eq!(limit, Some(2));
    assert_eq!(initial_offset_for_page, 4);
    assert_eq!(selection_bound, Some(7));
    assert_eq!(resume_initial_offset, 4);
    assert_eq!(resume_boundary, None);
}
