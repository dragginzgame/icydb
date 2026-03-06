use super::*;
use crate::db::{
    cursor::{CursorPlanError, GroupedContinuationToken},
    direction::Direction,
};

// Extract the cursor-domain taxonomy from executor plan-surface failures.
fn unwrap_cursor_plan_error(err: crate::db::executor::ExecutorPlanError) -> CursorPlanError {
    match err {
        crate::db::executor::ExecutorPlanError::Cursor(inner) => *inner,
    }
}

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
fn load_cursor_rejects_boundary_arity_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("boundary-arity plan should build");
    let boundary = CursorBoundary {
        // The plan order is `(rank ASC, id ASC)`, so one slot is arity-invalid.
        slots: vec![CursorBoundarySlot::Present(Value::Uint(10))],
    };
    let cursor = ContinuationToken::new_with_direction(
        plan.continuation_signature(),
        boundary,
        Direction::Asc,
        0,
    )
    .encode()
    .expect("boundary-arity cursor should encode");

    let err = plan
        .prepare_cursor(Some(cursor.as_slice()))
        .expect_err("boundary arity mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                        expected: 2,
                        found: 1
                    }
                )
        ),
        "planning should reject continuation cursor boundary arity mismatches"
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
fn load_cursor_v2_token_rejects_version_at_plan_time() {
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
    let v2_cursor = token
        .encode_with_version_for_test(2)
        .expect("v2 cursor should encode");

    let err = plan
        .prepare_cursor(Some(v2_cursor.as_slice()))
        .expect_err("v2 cursor should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorVersionMismatch {
                        version: 2
                    }
                )
        ),
        "v2 cursor versions must be rejected as unsupported"
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
fn executable_plan_continuation_signature_is_stable_and_shape_sensitive() {
    // Phase 1: derive one executable plan continuation signature from canonical planner semantics.
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("plan should build");
    let signature = plan.continuation_signature();

    assert_eq!(
        plan.continuation_signature(),
        signature,
        "executable continuation signatures must be stable across repeated reads",
    );

    // Phase 2: semantic shape drift must produce a distinct continuation signature.
    let drifted_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("label")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("drifted plan should build");

    assert_ne!(
        signature,
        drifted_plan.continuation_signature(),
        "continuation signature must invalidate when planner shape changes",
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

#[test]
#[expect(clippy::too_many_lines)]
fn grouped_and_scalar_cursor_mismatch_matrix_preserves_contract_parity() {
    // Phase 1: build one scalar plan and one grouped plan with identical pagination windows.
    let scalar_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .offset(2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("scalar plan should build");
    let grouped_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped plan should build")
        .aggregate(crate::db::count())
        .limit(1)
        .offset(2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped plan should build");

    // Phase 2: assert unsupported-version parity across scalar and grouped cursors.
    let scalar_boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(9001))),
        ],
    };
    let scalar_version_cursor = ContinuationToken::new_with_direction(
        scalar_plan.continuation_signature(),
        scalar_boundary.clone(),
        Direction::Asc,
        2,
    )
    .encode_with_version_for_test(9)
    .expect("scalar unsupported-version cursor should encode");
    let grouped_version_cursor = GroupedContinuationToken::new_with_direction(
        grouped_plan.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        2,
    )
    .encode_with_version_for_test(9)
    .expect("grouped unsupported-version cursor should encode");
    let scalar_version_err = unwrap_cursor_plan_error(
        scalar_plan
            .prepare_cursor(Some(scalar_version_cursor.as_slice()))
            .expect_err("scalar unsupported version must fail"),
    );
    let grouped_version_err = unwrap_cursor_plan_error(
        grouped_plan
            .prepare_grouped_cursor(Some(grouped_version_cursor.as_slice()))
            .expect_err("grouped unsupported version must fail"),
    );
    assert!(
        matches!(
            scalar_version_err,
            CursorPlanError::ContinuationCursorVersionMismatch { version: 9 }
        ),
        "scalar mismatch matrix must keep unsupported-version mapping explicit"
    );
    assert!(
        matches!(
            grouped_version_err,
            CursorPlanError::ContinuationCursorVersionMismatch { version: 9 }
        ),
        "grouped mismatch matrix must keep unsupported-version mapping explicit"
    );

    // Phase 3: assert signature-mismatch parity across scalar and grouped plan-shape drift.
    let scalar_drift_target = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("label")
        .limit(1)
        .offset(2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("scalar drift target should build");
    let scalar_signature_cursor = ContinuationToken::new_with_direction(
        scalar_plan.continuation_signature(),
        scalar_boundary,
        Direction::Asc,
        2,
    )
    .encode()
    .expect("scalar signature cursor should encode");
    let scalar_signature_err = unwrap_cursor_plan_error(
        scalar_drift_target
            .prepare_cursor(Some(scalar_signature_cursor.as_slice()))
            .expect_err("scalar plan-shape drift must fail"),
    );

    let grouped_drift_target = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("grouped drift target should build")
        .aggregate(crate::db::count())
        .limit(1)
        .offset(2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped drift target should build");
    let grouped_signature_cursor = GroupedContinuationToken::new_with_direction(
        grouped_plan.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        2,
    )
    .encode()
    .expect("grouped signature cursor should encode");
    let grouped_signature_err = unwrap_cursor_plan_error(
        grouped_drift_target
            .prepare_grouped_cursor(Some(grouped_signature_cursor.as_slice()))
            .expect_err("grouped plan-shape drift must fail"),
    );
    assert!(
        matches!(
            scalar_signature_err,
            CursorPlanError::ContinuationCursorSignatureMismatch { .. }
        ),
        "scalar mismatch matrix must keep signature-mismatch mapping explicit"
    );
    assert!(
        matches!(
            grouped_signature_err,
            CursorPlanError::ContinuationCursorSignatureMismatch { .. }
        ),
        "grouped mismatch matrix must keep signature-mismatch mapping explicit"
    );

    // Phase 4: assert offset-window mismatch parity across scalar and grouped cursors.
    let scalar_window_cursor = ContinuationToken::new_with_direction(
        scalar_plan.continuation_signature(),
        CursorBoundary {
            slots: vec![
                CursorBoundarySlot::Present(Value::Uint(10)),
                CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(9002))),
            ],
        },
        Direction::Asc,
        0,
    )
    .encode()
    .expect("scalar window-mismatch cursor should encode");
    let grouped_window_cursor = GroupedContinuationToken::new_with_direction(
        grouped_plan.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("grouped window-mismatch cursor should encode");
    let scalar_window_err = unwrap_cursor_plan_error(
        scalar_plan
            .prepare_cursor(Some(scalar_window_cursor.as_slice()))
            .expect_err("scalar window mismatch must fail"),
    );
    let grouped_window_err = unwrap_cursor_plan_error(
        grouped_plan
            .prepare_grouped_cursor(Some(grouped_window_cursor.as_slice()))
            .expect_err("grouped window mismatch must fail"),
    );
    assert!(
        matches!(
            scalar_window_err,
            CursorPlanError::ContinuationCursorWindowMismatch {
                expected_offset: 2,
                actual_offset: 0
            }
        ),
        "scalar mismatch matrix must keep window-mismatch mapping explicit"
    );
    assert!(
        matches!(
            grouped_window_err,
            CursorPlanError::ContinuationCursorWindowMismatch {
                expected_offset: 2,
                actual_offset: 0
            }
        ),
        "grouped mismatch matrix must keep window-mismatch mapping explicit"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn continuation_resume_tokens_fail_closed_on_scalar_and_grouped_shape_drift_matrix() {
    // Phase 1: mint one scalar resume token from the source semantic shape.
    let scalar_source = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("scalar source plan should build");
    let scalar_cursor = ContinuationToken::new_with_direction(
        scalar_source.continuation_signature(),
        CursorBoundary {
            slots: vec![
                CursorBoundarySlot::Present(Value::Uint(10)),
                CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(9101))),
            ],
        },
        Direction::Asc,
        0,
    )
    .encode()
    .expect("scalar source cursor should encode");

    // Phase 2: assert scalar resume invalidation across independent shape drifts.
    for (case_name, scalar_target) in [
        (
            "scalar_order_field_drift",
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("label")
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("scalar order-drift target plan should build"),
        ),
        (
            "scalar_distinct_flag_drift",
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("rank")
                .distinct()
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("scalar distinct-drift target plan should build"),
        ),
    ] {
        let err = unwrap_cursor_plan_error(
            scalar_target
                .prepare_cursor(Some(scalar_cursor.as_slice()))
                .expect_err("scalar shape drift must reject stale continuation token"),
        );
        assert!(
            matches!(
                err,
                CursorPlanError::ContinuationCursorSignatureMismatch { .. }
            ),
            "scalar continuation invalidation must fail closed on shape drift ({case_name})"
        );
    }

    // Phase 3: mint one grouped resume token from the source grouped shape.
    let grouped_source = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .group_by("group")
        .expect("grouped source query should build")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped source plan should build");
    let grouped_cursor = GroupedContinuationToken::new_with_direction(
        grouped_source.continuation_signature(),
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("grouped source cursor should encode");

    // Phase 4: assert grouped resume invalidation across independent shape drifts.
    for (case_name, grouped_target) in [
        (
            "grouped_having_clause_drift",
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .group_by("group")
                .expect("grouped having-drift target query should build")
                .aggregate(crate::db::count())
                .limit(1)
                .having_aggregate(0, CompareOp::Gt, Value::Uint(1))
                .expect("grouped having-drift target clause should build")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("grouped having-drift target plan should build"),
        ),
        (
            "grouped_distinct_aggregate_drift",
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .group_by("group")
                .expect("grouped distinct-drift target query should build")
                .aggregate(crate::db::count().distinct())
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("grouped distinct-drift target plan should build"),
        ),
    ] {
        let err = unwrap_cursor_plan_error(
            grouped_target
                .prepare_grouped_cursor(Some(grouped_cursor.as_slice()))
                .expect_err("grouped shape drift must reject stale continuation token"),
        );
        assert!(
            matches!(
                err,
                CursorPlanError::ContinuationCursorSignatureMismatch { .. }
            ),
            "grouped continuation invalidation must fail closed on shape drift ({case_name})"
        );
    }
}
