use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorPlanError, GroupedContinuationToken,
            prepare_grouped_cursor, revalidate_grouped_cursor, validate_grouped_cursor_order_plan,
        },
        direction::Direction,
        query::plan::{OrderDirection, OrderSpec},
    },
    value::Value,
};

fn grouped_token_fixture(direction: Direction) -> GroupedContinuationToken {
    GroupedContinuationToken::new_with_direction(
        ContinuationSignature::from_bytes([0x42; 32]),
        vec![
            Value::Text("tenant-a".to_string()),
            Value::Uint(7),
            Value::Bool(true),
        ],
        direction,
        4,
    )
}

#[test]
fn prepare_grouped_cursor_rejects_descending_cursor_direction() {
    let token = grouped_token_fixture(Direction::Desc);
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");
    let err = prepare_grouped_cursor(
        "grouped::test_entity",
        None::<&OrderSpec>,
        token.signature(),
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect_err("grouped cursor direction must remain ascending");

    assert!(matches!(
        err,
        CursorPlanError::InvalidContinuationCursorPayload { reason }
            if reason == "grouped continuation cursor direction must be ascending"
    ));
}

#[test]
fn prepare_grouped_cursor_rejects_signature_mismatch() {
    let token = grouped_token_fixture(Direction::Asc);
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");
    let expected_signature = ContinuationSignature::from_bytes([0x24; 32]);
    let err = prepare_grouped_cursor(
        "grouped::test_entity",
        None::<&OrderSpec>,
        expected_signature,
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect_err("grouped cursor signature mismatch must fail");

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorSignatureMismatch {
            entity_path,
            expected: _,
            actual: _,
        } if entity_path == "grouped::test_entity"
    ));
}

#[test]
fn prepare_grouped_cursor_rejects_offset_mismatch() {
    let token = grouped_token_fixture(Direction::Asc);
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");
    let err = prepare_grouped_cursor(
        "grouped::test_entity",
        None::<&OrderSpec>,
        token.signature(),
        token.initial_offset() + 1,
        Some(encoded.as_slice()),
    )
    .expect_err("grouped cursor initial offset mismatch must fail");

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorWindowMismatch {
            expected_offset,
            actual_offset,
        } if expected_offset == token.initial_offset() + 1 && actual_offset == token.initial_offset()
    ));
}

#[test]
fn validate_grouped_cursor_order_plan_rejects_empty_order_spec() {
    let empty_order = OrderSpec { fields: vec![] };
    let err = validate_grouped_cursor_order_plan(Some(&empty_order))
        .expect_err("grouped cursor order plan must reject empty order specs");

    assert!(matches!(
        err,
        CursorPlanError::InvalidContinuationCursorPayload { reason }
            if reason.contains("cursor pagination requires non-empty ordering")
    ));
}

#[test]
fn validate_grouped_cursor_order_plan_accepts_missing_or_non_empty_order() {
    validate_grouped_cursor_order_plan(None::<&OrderSpec>)
        .expect("grouped cursor order plan should allow omitted order");
    let order = OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    };
    validate_grouped_cursor_order_plan(Some(&order))
        .expect("grouped cursor order plan should allow non-empty order");
}

#[test]
fn revalidate_grouped_cursor_round_trip_preserves_resume_boundary_when_offset_matches() {
    let token = grouped_token_fixture(Direction::Asc);
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");
    let prepared = prepare_grouped_cursor(
        "grouped::test_entity",
        None::<&OrderSpec>,
        token.signature(),
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect("grouped cursor should prepare");

    let revalidated = revalidate_grouped_cursor(token.initial_offset(), prepared.clone())
        .expect("grouped cursor revalidate should preserve valid resume cursor");

    assert_eq!(revalidated, prepared);
}

#[test]
fn revalidate_grouped_cursor_rejects_offset_mismatch() {
    let token = grouped_token_fixture(Direction::Asc);
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");
    let prepared = prepare_grouped_cursor(
        "grouped::test_entity",
        None::<&OrderSpec>,
        token.signature(),
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect("grouped cursor should prepare");
    let err = revalidate_grouped_cursor(token.initial_offset() + 1, prepared)
        .expect_err("grouped cursor revalidate must enforce offset compatibility");

    assert!(matches!(
        err,
        CursorPlanError::ContinuationCursorWindowMismatch {
            expected_offset,
            actual_offset,
        } if expected_offset == token.initial_offset() + 1 && actual_offset == token.initial_offset()
    ));
}
