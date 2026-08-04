//! Module: db::cursor::tests
//! Covers grouped cursor token validation and order-contract invariants.

use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorDecodeError, CursorPlanError, GroupedContinuationToken,
            decode_optional_cursor_token, prepare_grouped_cursor,
            validate_grouped_cursor_order_plan,
        },
        direction::Direction,
        query::plan::{OrderDirection, OrderSpec},
    },
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticDecodeReason, DiagnosticFactTag};

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
        &err,
        CursorPlanError::InvalidContinuationCursorPayload { .. }
    );
    assert_eq!(
        err.diagnostic_facts(),
        vec![(
            DiagnosticFactTag::DecodeReason,
            DiagnosticDecodeReason::CursorGroupedDirectionMismatch.raw(),
        )],
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
        &err,
        CursorPlanError::ContinuationCursorSignatureMismatch { .. }
    );
    assert_eq!(
        err.diagnostic_facts(),
        vec![
            (
                DiagnosticFactTag::ExpectedSignaturePrefix,
                u64::from(u32::from_be_bytes([0x24; 4])),
            ),
            (
                DiagnosticFactTag::ActualSignaturePrefix,
                u64::from(u32::from_be_bytes([0x42; 4])),
            ),
        ],
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
        &err,
        CursorPlanError::ContinuationCursorWindowMismatch { .. }
    );
    assert_eq!(
        err.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ExpectedOffset, 5),
            (DiagnosticFactTag::ActualOffset, 4),
        ],
    );
}

#[test]
fn external_cursor_decode_facts_preserve_bounds_and_zero_based_positions() {
    let err = decode_optional_cursor_token(Some("0x"))
        .expect_err("invalid cursor hex should fail before token decode");
    assert_eq!(
        err.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ComponentIndex, 1),
            (
                DiagnosticFactTag::DecodeReason,
                DiagnosticDecodeReason::CursorInvalidHex.raw(),
            ),
        ],
    );

    let err = CursorPlanError::invalid_continuation_cursor(CursorDecodeError::TooLong {
        len: 16_386,
        max: 16_384,
    });
    assert_eq!(
        err.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ActualLength, 16_386),
            (DiagnosticFactTag::Maximum, 16_384),
            (
                DiagnosticFactTag::DecodeReason,
                DiagnosticDecodeReason::CursorTooLong.raw(),
            ),
        ],
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
