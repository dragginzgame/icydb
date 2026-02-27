pub(in crate::db) use crate::db::cursor::planned::GroupedPlannedCursor;
pub(in crate::db) use crate::db::cursor::token::GroupedContinuationToken;
#[allow(unused_imports)]
pub(crate) use crate::db::cursor::token::GroupedContinuationTokenError;
///
/// GROUPED CURSOR SCAFFOLD
///
/// WIP ownership note:
/// GROUP BY is intentionally isolated behind this module for now.
/// Keep grouped scaffold code behind this boundary for the time being and do not remove it.
///
/// Explicit ownership boundary for grouped cursor token/state scaffold.
/// This module gathers grouped cursor contracts and grouped cursor helpers under
/// one import surface.
///

#[allow(unused_imports)]
pub(in crate::db) use crate::db::cursor::{
    prepare_grouped_cursor, revalidate_grouped_cursor, validate_grouped_cursor_order_plan,
};

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::cursor::{
            grouped::GroupedContinuationToken, prepare_grouped_cursor, revalidate_grouped_cursor,
            validate_grouped_cursor_order_plan,
        },
        db::{
            codec::cursor::encode_cursor,
            cursor::{
                ContinuationSignature, CursorPlanError, grouped::GroupedContinuationTokenError,
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
    fn grouped_continuation_token_round_trip_preserves_fields() {
        let token = grouped_token_fixture(Direction::Asc);

        let encoded = token
            .encode()
            .expect("grouped continuation token should encode");
        let decoded = GroupedContinuationToken::decode(encoded.as_slice())
            .expect("grouped continuation token should decode");

        assert_eq!(decoded.signature(), token.signature());
        assert_eq!(decoded.last_group_key(), token.last_group_key());
        assert_eq!(decoded.direction(), token.direction());
        assert_eq!(decoded.initial_offset(), token.initial_offset());
    }

    #[test]
    fn grouped_continuation_token_v1_wire_vector_is_frozen() {
        let token = grouped_token_fixture(Direction::Asc);

        let encoded = token
            .encode()
            .expect("grouped continuation token should encode");
        let actual_hex = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_hex,
            "a56776657273696f6e01697369676e61747572659820184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218426e6c6173745f67726f75705f6b657983a164546578746874656e616e742d61a16455696e7407a164426f6f6cf569646972656374696f6e634173636e696e697469616c5f6f666673657404"
        );
    }

    #[test]
    fn grouped_continuation_token_decode_rejects_unsupported_version() {
        let token = grouped_token_fixture(Direction::Asc);
        let encoded = token
            .encode_with_version_for_test(9)
            .expect("grouped continuation token test wire should encode");
        let err = GroupedContinuationToken::decode(encoded.as_slice())
            .expect_err("unknown grouped cursor wire version must fail");

        assert_eq!(
            err,
            GroupedContinuationTokenError::UnsupportedVersion { version: 9 }
        );
    }

    #[test]
    fn grouped_continuation_token_decode_rejects_oversized_payload() {
        let oversized = vec![0_u8; 8 * 1024 + 1];
        let err = GroupedContinuationToken::decode(oversized.as_slice())
            .expect_err("oversized grouped cursor payload must fail");

        assert!(matches!(err, GroupedContinuationTokenError::Decode(_)));
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
        .expect_err("grouped cursor direction must remain ascending in 0.32.x");

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
}
