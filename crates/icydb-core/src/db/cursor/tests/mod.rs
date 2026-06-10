//! Module: db::cursor::tests
//! Covers grouped cursor token validation, revalidation, and order-contract
//! invariants.

use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorDecodeError, CursorPayloadErrorCode, CursorPlanError,
            GroupedContinuationToken, prepare_grouped_cursor, revalidate_grouped_cursor,
            validate_grouped_cursor_order_plan,
        },
        direction::Direction,
        query::plan::{OrderDirection, OrderSpec},
    },
    error::{ErrorClass, ErrorOrigin},
    value::Value,
};

fn grouped_token_fixture(direction: Direction) -> GroupedContinuationToken {
    GroupedContinuationToken::new_with_direction(
        ContinuationSignature::from_bytes([0x42; 32]),
        vec![
            Value::Text("tenant-a".to_string()),
            Value::Nat64(7),
            Value::Bool(true),
        ],
        direction,
        4,
    )
}

#[test]
fn prepare_grouped_cursor_rejects_direction_mismatch() {
    let token = grouped_token_fixture(Direction::Desc);
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");
    let err = prepare_grouped_cursor(
        "grouped::test_entity",
        None::<&OrderSpec>,
        Direction::Asc,
        token.signature(),
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect_err("grouped cursor direction must match grouped execution direction");

    std::assert_matches!(
        err,
        CursorPlanError::InvalidContinuationCursorPayload { .. }
    );
}

#[test]
fn prepare_grouped_cursor_accepts_matching_descending_direction() {
    let token = grouped_token_fixture(Direction::Desc);
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");

    let prepared = prepare_grouped_cursor(
        "grouped::test_entity",
        None::<&OrderSpec>,
        Direction::Desc,
        token.signature(),
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect("grouped cursor direction should match descending grouped execution");

    assert_eq!(prepared.last_group_key(), Some(token.last_group_key()));
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
        Direction::Asc,
        expected_signature,
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect_err("grouped cursor signature mismatch must fail");

    std::assert_matches!(
        err,
        CursorPlanError::ContinuationCursorSignatureMismatch { .. }
    );
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
        Direction::Asc,
        token.signature(),
        token.initial_offset() + 1,
        Some(encoded.as_slice()),
    )
    .expect_err("grouped cursor initial offset mismatch must fail");

    std::assert_matches!(
        err,
        CursorPlanError::ContinuationCursorWindowMismatch { .. }
    );
}

#[test]
fn validate_grouped_cursor_order_plan_rejects_empty_order_spec() {
    let empty_order = OrderSpec { fields: vec![] };
    let err = validate_grouped_cursor_order_plan(Some(&empty_order))
        .expect_err("grouped cursor order plan must reject empty order specs");

    std::assert_matches!(err, CursorPlanError::ContinuationCursorInvariantViolation);
}

#[test]
fn validate_grouped_cursor_order_plan_accepts_missing_or_non_empty_order() {
    validate_grouped_cursor_order_plan(None::<&OrderSpec>)
        .expect("grouped cursor order plan should allow omitted order");
    let order = OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    };
    validate_grouped_cursor_order_plan(Some(&order))
        .expect("grouped cursor order plan should allow non-empty order");
}

#[test]
fn cursor_order_invariant_constructors_use_compact_variant() {
    let requires_order = CursorPlanError::cursor_requires_order();
    let requires_non_empty_order = CursorPlanError::cursor_requires_non_empty_order();
    let requires_explicit_or_grouped =
        CursorPlanError::cursor_requires_explicit_or_grouped_ordering();

    std::assert_matches!(
        requires_order,
        CursorPlanError::ContinuationCursorInvariantViolation
    );
    std::assert_matches!(
        requires_non_empty_order,
        CursorPlanError::ContinuationCursorInvariantViolation
    );
    std::assert_matches!(
        requires_explicit_or_grouped,
        CursorPlanError::ContinuationCursorInvariantViolation
    );
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
        Direction::Asc,
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
        Direction::Asc,
        token.signature(),
        token.initial_offset(),
        Some(encoded.as_slice()),
    )
    .expect("grouped cursor should prepare");
    let err = revalidate_grouped_cursor(token.initial_offset() + 1, prepared)
        .expect_err("grouped cursor revalidate must enforce offset compatibility");

    std::assert_matches!(
        err,
        CursorPlanError::ContinuationCursorWindowMismatch { .. }
    );
}

#[test]
fn pk_cursor_decode_error_mapping_is_explicit_for_all_cursor_variants() {
    let cases = vec![
        CursorPlanError::InvalidContinuationCursor {
            reason: CursorDecodeError::OddLength,
        },
        CursorPlanError::invalid_continuation_cursor_payload(CursorPayloadErrorCode::UNKNOWN),
        CursorPlanError::continuation_cursor_signature_mismatch(
            "cursor::tests",
            &ContinuationSignature::from_bytes([0x10; 32]),
            &ContinuationSignature::from_bytes([0x11; 32]),
        ),
        CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
        CursorPlanError::continuation_cursor_window_mismatch(8, 3),
        CursorPlanError::continuation_cursor_boundary_type_mismatch_at(0),
        CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(1),
        CursorPlanError::ContinuationCursorInvariantViolation,
    ];

    for err in cases {
        let mapped = err.into_pk_cursor_decode_internal_error();

        assert_eq!(
            mapped.class,
            ErrorClass::InvariantViolation,
            "pk cursor decode mapping must remain invariant-classed",
        );
        assert_eq!(
            mapped.origin,
            ErrorOrigin::Cursor,
            "pk cursor decode mapping must remain cursor-origin",
        );
        assert_eq!(
            mapped.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
            "pk cursor decode mapping must preserve runtime invariant diagnostics",
        );
    }
}
