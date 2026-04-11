//! Module: db::executor::tests::cursor_validation
//! Responsibility: module-local ownership and contracts for db::executor::tests::cursor_validation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;

use crate::{
    db::{
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, CursorBoundarySlot,
            CursorPlanError, GroupedContinuationToken,
        },
        direction::Direction,
        executor::{ExecutablePlan, ExecutorPlanError},
    },
    types::Ulid,
    value::Value,
};

// Extract the cursor-domain taxonomy from executor plan-surface failures.
fn unwrap_cursor_plan_error(err: ExecutorPlanError) -> CursorPlanError {
    match err {
        ExecutorPlanError::Cursor(inner) => *inner,
    }
}

// Build the current scalar continuation contract used by the executor cursor boundary.
fn scalar_phase_plan() -> (ExecutablePlan<PhaseEntity>, ContinuationSignature, u32) {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("scalar phase query should plan")
        .into_inner();
    let continuation = plan
        .continuation_contract(<PhaseEntity as crate::traits::Path>::PATH)
        .expect("scalar phase load plan should project continuation contract");
    let signature = continuation.continuation_signature();
    let initial_offset = continuation.expected_initial_offset();

    (ExecutablePlan::new(plan), signature, initial_offset)
}

// Build the grouped continuation contract used by grouped executor cursor validation.
fn grouped_pushdown_plan() -> (
    ExecutablePlan<PushdownParityEntity>,
    ContinuationSignature,
    u32,
) {
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped query should plan")
        .into_inner();
    let continuation = plan
        .continuation_contract(<PushdownParityEntity as crate::traits::Path>::PATH)
        .expect("grouped load plan should project continuation contract");
    let signature = continuation.continuation_signature();
    let initial_offset = continuation.expected_initial_offset();

    (ExecutablePlan::new(plan), signature, initial_offset)
}

#[test]
fn load_cursor_rejects_boundary_value_type_mismatch_at_plan_time() {
    let (plan, signature, initial_offset) = scalar_phase_plan();
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Text("not-a-rank".to_string())),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(2002))),
        ],
    };
    let cursor =
        ContinuationToken::new_with_direction(signature, boundary, Direction::Asc, initial_offset)
            .encode()
            .expect("boundary-type cursor should encode");

    let err = unwrap_cursor_plan_error(
        plan.prepare_cursor(Some(cursor.as_slice()))
            .expect_err("boundary field type mismatch should be rejected during planning"),
    );

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorBoundaryTypeMismatch { field, .. } if field == "rank"
    ));
}

#[test]
fn load_cursor_rejects_primary_key_type_mismatch_at_plan_time() {
    let (plan, signature, initial_offset) = scalar_phase_plan();
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Text("not-a-ulid".to_string())),
        ],
    };
    let cursor =
        ContinuationToken::new_with_direction(signature, boundary, Direction::Asc, initial_offset)
            .encode()
            .expect("pk-type cursor should encode");

    let err = unwrap_cursor_plan_error(
        plan.prepare_cursor(Some(cursor.as_slice()))
            .expect_err("pk type mismatch should be rejected during planning"),
    );

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { field, .. } if field == "id"
    ));
}

#[test]
fn load_cursor_rejects_boundary_arity_mismatch_at_plan_time() {
    let (plan, signature, initial_offset) = scalar_phase_plan();
    let boundary = CursorBoundary {
        // Ordered scalar continuation uses `(rank ASC, id ASC)`.
        slots: vec![CursorBoundarySlot::Present(Value::Uint(10))],
    };
    let cursor =
        ContinuationToken::new_with_direction(signature, boundary, Direction::Asc, initial_offset)
            .encode()
            .expect("boundary-arity cursor should encode");

    let err = unwrap_cursor_plan_error(
        plan.prepare_cursor(Some(cursor.as_slice()))
            .expect_err("boundary arity mismatch should be rejected during planning"),
    );

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorBoundaryArityMismatch {
            expected: 2,
            found: 1,
        }
    ));
}

#[test]
fn load_cursor_rejects_wrong_entity_path_at_plan_time() {
    let foreign_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("foreign entity plan should build")
        .into_inner();
    let foreign_contract = foreign_plan
        .continuation_contract(<SimpleEntity as crate::traits::Path>::PATH)
        .expect("foreign entity load plan should project continuation contract");
    let foreign_cursor = ContinuationToken::new_with_direction(
        foreign_contract.continuation_signature(),
        CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
                3001,
            )))],
        },
        Direction::Asc,
        foreign_contract.expected_initial_offset(),
    )
    .encode()
    .expect("foreign entity cursor should encode");

    let local_plan: ExecutablePlan<PhaseEntity> = {
        let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .limit(1)
            .plan()
            .expect("local entity plan should build")
            .into_inner();

        ExecutablePlan::new(plan)
    };
    let err = unwrap_cursor_plan_error(
        local_plan
            .prepare_cursor(Some(foreign_cursor.as_slice()))
            .expect_err("cursor from a different entity path should be rejected during planning"),
    );

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorSignatureMismatch { .. }
    ));
}

#[test]
fn load_cursor_rejects_offset_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .expect("offset plan should build")
        .into_inner();
    let continuation = plan
        .continuation_contract(<PhaseEntity as crate::traits::Path>::PATH)
        .expect("offset load plan should project continuation contract");
    let signature = continuation.continuation_signature();
    let plan: ExecutablePlan<PhaseEntity> = ExecutablePlan::new(plan);
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(4001))),
        ],
    };
    let cursor = ContinuationToken::new_with_direction(signature, boundary, Direction::Asc, 0)
        .encode()
        .expect("offset-mismatch cursor should encode");

    let err = unwrap_cursor_plan_error(
        plan.prepare_cursor(Some(cursor.as_slice()))
            .expect_err("offset mismatch should be rejected during planning"),
    );

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorWindowMismatch {
            expected_offset: 2,
            actual_offset: 0,
        }
    ));
}

#[test]
fn grouped_cursor_rejects_cross_shape_resume_token_at_plan_time() {
    let source_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped source query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped source plan should build")
        .into_inner();
    let source_contract = source_plan
        .continuation_contract(<PushdownParityEntity as crate::traits::Path>::PATH)
        .expect("grouped source plan should project continuation contract");
    let cursor = GroupedContinuationToken::new_with_direction(
        source_contract.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        source_contract.expected_initial_offset(),
    )
    .encode()
    .expect("grouped source cursor should encode");

    let target_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("grouped target query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped target plan should build")
        .into_inner();
    let target_plan: ExecutablePlan<PushdownParityEntity> = ExecutablePlan::new(target_plan);
    let err = unwrap_cursor_plan_error(
        target_plan
            .prepare_grouped_cursor(Some(cursor.as_slice()))
            .expect_err("cross-shape grouped token must be rejected"),
    );

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorSignatureMismatch { .. }
    ));
}

#[test]
fn grouped_cursor_rejects_descending_direction_at_plan_time() {
    let (plan, signature, initial_offset) = grouped_pushdown_plan();
    let cursor = GroupedContinuationToken::new_with_direction(
        signature,
        vec![Value::Uint(7)],
        Direction::Desc,
        initial_offset,
    )
    .encode()
    .expect("descending-direction grouped cursor should encode");

    let err = unwrap_cursor_plan_error(
        plan.prepare_grouped_cursor(Some(cursor.as_slice()))
            .expect_err("grouped cursor with descending direction must be rejected"),
    );

    assert!(matches!(
        err,
        CursorPlanError::InvalidContinuationCursorPayload { reason }
            if reason.contains("direction must be ascending")
    ));
}
