use super::*;

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
    let token = ContinuationToken::new(plan.continuation_signature(), boundary);
    let version_mismatch_cursor = token
        .encode_with_version_for_test(99)
        .expect("version-mismatch cursor should encode");

    let err = plan
        .plan_cursor(Some(version_mismatch_cursor.as_slice()))
        .expect_err("unsupported cursor version should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorVersionMismatch { version: 99 }
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
    let cursor = ContinuationToken::new(plan.continuation_signature(), boundary)
        .encode()
        .expect("boundary-type cursor should encode");

    let err = plan
        .plan_cursor(Some(cursor.as_slice()))
        .expect_err("boundary field type mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorBoundaryTypeMismatch { field, .. }
            if field == "rank"
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
    let cursor = ContinuationToken::new(plan.continuation_signature(), boundary)
        .encode()
        .expect("pk-type cursor should encode");

    let err = plan
        .plan_cursor(Some(cursor.as_slice()))
        .expect_err("pk type mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorPrimaryKeyTypeMismatch { field, .. }
            if field == "id"
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
    let foreign_cursor = ContinuationToken::new(
        foreign_plan.continuation_signature(),
        CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
                3001,
            )))],
        },
    )
    .encode()
    .expect("foreign entity cursor should encode");

    let local_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("local entity plan should build");
    let err = local_plan
        .plan_cursor(Some(foreign_cursor.as_slice()))
        .expect_err("cursor from a different entity path should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorSignatureMismatch { .. }
        ),
        "planning should reject wrong-entity cursors via plan-signature mismatch"
    );
}
