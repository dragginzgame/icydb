use super::*;
use crate::db::direction::Direction;

#[test]
fn load_cursor_rejects_version_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("version-mismatch plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(2001))),
        ],
    };
    let token = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Asc,
        0,
    );
    let version_mismatch_cursor = token
        .encode_with_version_for_test(99)
        .expect("version-mismatch cursor should encode");

    let err = plan
        .prepare_cursor(Some(version_mismatch_cursor.as_slice()))
        .expect_err("unsupported cursor version should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorVersionMismatch {
                    version: 99
                    }
                )
        ),
        "planning should reject unsupported cursor versions"
    );
}

#[test]
fn load_cursor_rejects_boundary_value_type_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("boundary-type plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Text("not-a-rank".to_string())),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(2002))),
        ],
    };
    let cursor = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Asc,
        0,
    )
    .encode()
    .expect("boundary-type cursor should encode");

    let err = plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect_err("boundary field type mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorBoundaryTypeMismatch {
                    field,
                    ..
                    } if field == "rank"
                )
        ),
        "planning should reject non-PK boundary type mismatches"
    );
}

#[test]
fn load_cursor_rejects_primary_key_type_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("pk-type plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Text("not-a-ulid".to_string())),
        ],
    };
    let cursor = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Asc,
        0,
    )
    .encode()
    .expect("pk-type cursor should encode");

    let err = plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect_err("pk type mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    field,
                    ..
                    } if field == "id"
                )
        ),
        "planning should reject primary-key boundary type mismatches"
    );
}

#[test]
fn load_cursor_rejects_wrong_entity_path_at_plan_time() {
    let foreign_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("foreign entity plan should build");
    let foreign_cursor = ContinuationToken::new_with_direction(
        foreign_plan.continuation_signature(),
        CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
                3001,
            )))],
        },
        Direction::Asc,
        0,
    )
    .encode()
    .expect("foreign entity cursor should encode");

    let local_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("local entity plan should build");
    let err = local_plan
        .prepare_cursor(Some(foreign_cursor.as_slice()))
        .expect_err("cursor from a different entity path should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "planning should reject wrong-entity cursors via plan-signature mismatch"
    );
}

#[test]
fn load_cursor_rejects_offset_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .expect("offset plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(4001))),
        ],
    };
    let cursor = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Asc,
        0,
    )
    .encode()
    .expect("offset-mismatch cursor should encode");

    let err = plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect_err("offset mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorWindowMismatch {
                        expected_offset: 2,
                        actual_offset: 0
                    }
                )
        ),
        "planning should reject continuation cursors that do not match query offset"
    );
}

#[test]
fn load_cursor_v1_token_rejects_non_zero_offset_plan() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .expect("offset plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(4002))),
        ],
    };
    let token = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Asc,
        2,
    );
    let legacy_cursor = token
        .encode_with_version_for_test(1)
        .expect("legacy v1 cursor should encode");

    let err = plan
        .prepare_cursor(Some(legacy_cursor.as_slice()))
        .expect_err("v1 cursor should be rejected for non-zero offset plans");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorWindowMismatch {
                        expected_offset: 2,
                        actual_offset: 0
                    }
                )
        ),
        "legacy v1 cursors should map to offset mismatch when offset is non-zero"
    );
}

#[test]
fn load_cursor_rejects_order_field_signature_mismatch_at_plan_time() {
    let source_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("source plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(5001))),
        ],
    };
    let cursor = ContinuationToken::new_with_direction(
        source_plan.continuation_signature(),
        boundary,
        Direction::Asc,
        0,
    )
    .encode()
    .expect("order-field mismatch cursor should encode");

    let target_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("label")
        .limit(1)
        .plan()
        .expect("target plan should build");
    let err = target_plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect_err("cursor from a different order spec must be rejected");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "planning should reject order-spec signature mismatch"
    );
}

#[test]
fn load_cursor_rejects_direction_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("direction-mismatch plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(5002))),
        ],
    };
    let cursor = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Desc,
        0,
    )
    .encode()
    .expect("direction-mismatch cursor should encode");

    let err = plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect_err("cursor with mismatched direction must be rejected");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::InvalidContinuationCursorPayload {
                        reason
                    } if reason.contains("direction does not match executable plan direction")
                )
        ),
        "planning should reject continuation cursor direction mismatches"
    );
}

#[test]
fn load_cursor_accepts_matching_offset_window_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .expect("offset plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(5003))),
        ],
    };
    let cursor = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Asc,
        2,
    )
    .encode()
    .expect("matching-offset cursor should encode");

    let planned = plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect("cursor with matching offset should pass plan-time validation");
    assert!(
        !planned.is_empty(),
        "matching-offset cursor should produce planned cursor state"
    );
    assert_eq!(
        planned.initial_offset(),
        2,
        "planned cursor should preserve the validated initial offset"
    );
}
