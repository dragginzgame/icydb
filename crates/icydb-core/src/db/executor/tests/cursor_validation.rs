use super::*;
use crate::db::{
    cursor::{CursorPlanError, GroupedContinuationToken},
    direction::Direction,
};

#[test]
fn load_cursor_rejects_version_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorVersionMismatch {
                    version: 99
                    }
                )
        ),
        "planning should reject unsupported cursor versions"
    );
}

#[test]
fn load_cursor_rejects_boundary_value_type_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorBoundaryTypeMismatch {
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
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
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
    let foreign_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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

    let local_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("local entity plan should build");
    let err = local_plan
        .prepare_cursor(Some(foreign_cursor.as_slice()))
        .expect_err("cursor from a different entity path should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "planning should reject wrong-entity cursors via plan-signature mismatch"
    );
}

#[test]
fn load_cursor_rejects_offset_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorWindowMismatch {
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
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorWindowMismatch {
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
    let source_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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

    let target_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("label")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target plan should build");
    let err = target_plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect_err("cursor from a different order spec must be rejected");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "planning should reject order-spec signature mismatch"
    );
}

#[test]
fn load_cursor_rejects_direction_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::InvalidContinuationCursorPayload {
                        reason
                    } if reason.contains("direction does not match executable plan direction")
                )
        ),
        "planning should reject continuation cursor direction mismatches"
    );
}

#[test]
fn load_cursor_accepts_matching_offset_window_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
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

#[test]
fn grouped_cursor_rejects_cross_shape_resume_token_and_encoded_bytes_differ() {
    let grouped_by_group = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped-by-group query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped-by-group plan should build");
    let grouped_by_rank = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("grouped-by-rank query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped-by-rank plan should build");

    let signature_group = grouped_by_group.continuation_signature();
    let signature_rank = grouped_by_rank.continuation_signature();
    assert_ne!(
        signature_group, signature_rank,
        "grouped continuation signatures must change when group key shape changes"
    );

    let token_group = GroupedContinuationToken::new_with_direction(
        signature_group,
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    );
    let token_rank = GroupedContinuationToken::new_with_direction(
        signature_rank,
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    );
    let bytes_group = token_group
        .encode()
        .expect("grouped-by-group token should encode");
    let bytes_rank = token_rank
        .encode()
        .expect("grouped-by-rank token should encode");
    assert_ne!(
        bytes_group, bytes_rank,
        "grouped continuation token bytes must differ across grouped query shapes"
    );

    let prepared = grouped_by_group
        .prepare_grouped_cursor(Some(bytes_group.as_slice()))
        .expect("matching grouped token should validate");
    assert!(
        !prepared.is_empty(),
        "matching grouped token should produce grouped cursor state"
    );

    let err = grouped_by_rank
        .prepare_grouped_cursor(Some(bytes_group.as_slice()))
        .expect_err("cross-shape grouped token must be rejected");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "grouped resume must fail fast on continuation signature mismatch"
    );
}

#[test]
fn grouped_cursor_rejects_unsupported_version_at_plan_time() {
    let grouped_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped plan should build");
    let token = GroupedContinuationToken::new_with_direction(
        grouped_plan.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    );
    let cursor = token
        .encode_with_version_for_test(9)
        .expect("unsupported-version grouped cursor should encode");

    let err = grouped_plan
        .prepare_grouped_cursor(Some(cursor.as_slice()))
        .expect_err("unsupported grouped cursor version must be rejected");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    CursorPlanError::ContinuationCursorVersionMismatch { version } if *version == 9
                )
        ),
        "grouped planning should reject unsupported grouped cursor versions"
    );
}

#[test]
fn grouped_cursor_rejects_cross_shape_resume_token_when_having_changes() {
    let grouped_having_gt = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped having source query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .having_aggregate(0, CompareOp::Gt, Value::Uint(1))
        .expect("grouped having source clause should build")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped having source plan should build");
    let grouped_having_gte = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped having target query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .having_aggregate(0, CompareOp::Gte, Value::Uint(1))
        .expect("grouped having target clause should build")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped having target plan should build");

    assert_ne!(
        grouped_having_gt.continuation_signature(),
        grouped_having_gte.continuation_signature(),
        "grouped continuation signatures must change when HAVING shape changes"
    );

    let cursor = GroupedContinuationToken::new_with_direction(
        grouped_having_gt.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("grouped having source cursor should encode");
    let err = grouped_having_gte
        .prepare_grouped_cursor(Some(cursor.as_slice()))
        .expect_err("grouped cursor must fail when HAVING shape changes");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "grouped resume must fail when HAVING shape differs"
    );
}

#[test]
fn grouped_cursor_rejects_cross_shape_resume_token_when_distinct_aggregate_changes() {
    let grouped_count = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped count source query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped count source plan should build");
    let grouped_count_distinct = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped count distinct target query should build")
        .aggregate(crate::db::count().distinct())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped count distinct target plan should build");

    assert_ne!(
        grouped_count.continuation_signature(),
        grouped_count_distinct.continuation_signature(),
        "grouped continuation signatures must change when aggregate DISTINCT changes"
    );

    let cursor = GroupedContinuationToken::new_with_direction(
        grouped_count.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("grouped count source cursor should encode");
    let err = grouped_count_distinct
        .prepare_grouped_cursor(Some(cursor.as_slice()))
        .expect_err("grouped cursor must fail when aggregate distinct shape changes");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "grouped resume must fail when aggregate DISTINCT shape differs"
    );
}
