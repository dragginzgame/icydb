mod anchor;
pub(crate) mod boundary;
mod continuation;
mod errors;
mod order;
mod planned;
mod range_token;
mod spine;
pub(crate) mod token;

pub(crate) use boundary::{CursorBoundary, CursorBoundarySlot};
pub(in crate::db) use boundary::{
    apply_order_direction, compare_boundary_slots, decode_pk_cursor_boundary as decode_pk_boundary,
    validate_cursor_boundary_for_order, validate_cursor_direction, validate_cursor_window_offset,
};
pub(in crate::db) use continuation::next_cursor_for_materialized_rows;
pub(crate) use errors::CursorPlanError;
pub(in crate::db) use order::{apply_cursor_boundary, apply_order_spec, apply_order_spec_bounded};
pub(in crate::db) use planned::{GroupedPlannedCursor, PlannedCursor};
pub(in crate::db) use range_token::{
    RangeToken, cursor_anchor_from_index_key, range_token_anchor_key,
    range_token_from_cursor_anchor, range_token_from_lowered_anchor,
};
pub(crate) use token::{ContinuationSignature, ContinuationToken, ContinuationTokenError};
pub(in crate::db) use token::{GroupedContinuationToken, IndexRangeCursorAnchor};

use crate::{
    db::{
        direction::Direction,
        query::plan::{AccessPlannedQuery, OrderSpec},
    },
    error::InternalError,
    traits::{EntityKind, FieldValue},
};

/// Validate and decode a continuation cursor into executor-ready cursor state.
pub(in crate::db) fn prepare_cursor<E: EntityKind>(
    plan: &AccessPlannedQuery<E::Key>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<&[u8]>,
) -> Result<PlannedCursor, CursorPlanError>
where
    E::Key: FieldValue,
{
    let order = validated_cursor_order(plan)?;

    spine::validate_planned_cursor::<E>(
        cursor,
        plan.access.as_path(),
        E::PATH,
        E::MODEL,
        order,
        continuation_signature,
        direction,
        initial_offset,
    )
}

/// Revalidate executor-provided cursor state through the canonical cursor spine.
pub(in crate::db) fn revalidate_cursor<E: EntityKind>(
    plan: &AccessPlannedQuery<E::Key>,
    direction: Direction,
    initial_offset: u32,
    cursor: PlannedCursor,
) -> Result<PlannedCursor, CursorPlanError>
where
    E::Key: FieldValue,
{
    if cursor.is_empty() {
        return Ok(PlannedCursor::none());
    }

    let order = validated_cursor_order(plan)?;

    spine::validate_planned_cursor_state::<E>(
        cursor,
        plan.access.as_path(),
        E::MODEL,
        order,
        direction,
        initial_offset,
    )
}

/// Validate and decode a grouped continuation cursor into grouped cursor state.
pub(in crate::db) fn prepare_grouped_cursor(
    entity_path: &'static str,
    order: Option<&OrderSpec>,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<&[u8]>,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    validate_grouped_cursor_order_plan(order)?;
    let Some(cursor) = cursor else {
        return Ok(GroupedPlannedCursor::none());
    };
    let token = GroupedContinuationToken::decode(cursor).map_err(|err| {
        CursorPlanError::InvalidContinuationCursorPayload {
            reason: err.to_string(),
        }
    })?;
    if token.signature() != continuation_signature {
        return Err(CursorPlanError::ContinuationCursorSignatureMismatch {
            entity_path,
            expected: continuation_signature.to_string(),
            actual: token.signature().to_string(),
        });
    }
    if token.direction() != Direction::Asc {
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: "grouped continuation cursor direction must be ascending".to_string(),
        });
    }
    validate_cursor_window_offset(initial_offset, token.initial_offset())?;

    Ok(GroupedPlannedCursor::new(
        token.last_group_key().to_vec(),
        token.initial_offset(),
    ))
}

/// Revalidate grouped cursor state through grouped cursor invariants.
pub(in crate::db) fn revalidate_grouped_cursor(
    initial_offset: u32,
    cursor: GroupedPlannedCursor,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    if cursor.is_empty() {
        return Ok(GroupedPlannedCursor::none());
    }
    validate_cursor_window_offset(initial_offset, cursor.initial_offset())?;

    Ok(cursor)
}

/// Decode a typed primary-key cursor boundary for PK-ordered executor paths.
pub(in crate::db) fn decode_pk_cursor_boundary<E>(
    boundary: Option<&CursorBoundary>,
) -> Result<Option<E::Key>, InternalError>
where
    E: EntityKind,
{
    decode_pk_boundary::<E>(boundary).map_err(|err| match err {
        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { value: None, .. } => {
            InternalError::query_executor_invariant("pk cursor slot must be present")
        }
        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { value: Some(_), .. } => {
            InternalError::query_executor_invariant("pk cursor slot type mismatch")
        }
        _ => InternalError::query_executor_invariant(err.to_string()),
    })
}

// Resolve cursor ordering for plan-surface decoding and executor revalidation.
fn validated_cursor_order<K>(plan: &AccessPlannedQuery<K>) -> Result<&OrderSpec, CursorPlanError> {
    let order = plan.scalar_plan().order.as_ref();
    let Some(order) = validated_cursor_order_internal(
        order,
        true,
        "cursor pagination requires explicit ordering",
    )?
    else {
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: InternalError::executor_invariant_message(
                "cursor pagination requires explicit ordering",
            ),
        });
    };

    Ok(order)
}

/// Validate grouped cursor ordering plan shape.
///
/// GROUP BY v1 uses canonical lexicographic group-key order by default, so
/// explicit ordering is optional, but empty order specs remain invalid.
pub(in crate::db) fn validate_grouped_cursor_order_plan(
    order: Option<&OrderSpec>,
) -> Result<(), CursorPlanError> {
    let _ = validated_cursor_order_internal(
        order,
        false,
        "grouped cursor pagination uses canonical group-key order when ORDER BY is omitted",
    )?;

    Ok(())
}

fn validated_cursor_order_internal<'a>(
    order: Option<&'a OrderSpec>,
    require_explicit_order: bool,
    missing_order_message: &'static str,
) -> Result<Option<&'a OrderSpec>, CursorPlanError> {
    let Some(order) = order else {
        if require_explicit_order {
            return Err(CursorPlanError::InvalidContinuationCursorPayload {
                reason: InternalError::executor_invariant_message(missing_order_message),
            });
        }

        return Ok(None);
    };
    if order.fields.is_empty() {
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: InternalError::executor_invariant_message(
                "cursor pagination requires non-empty ordering",
            ),
        });
    }

    Ok(Some(order))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            cursor::{
                ContinuationSignature, CursorPlanError, GroupedContinuationToken,
                prepare_grouped_cursor, revalidate_grouped_cursor,
                validate_grouped_cursor_order_plan,
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
}
