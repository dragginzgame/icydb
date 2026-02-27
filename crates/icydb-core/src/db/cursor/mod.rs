mod anchor;
pub(crate) mod boundary;
mod errors;
mod planned;
mod range_token;
mod spine;
pub(crate) mod token;

pub(crate) use boundary::{CursorBoundary, CursorBoundarySlot};
pub(in crate::db) use boundary::{
    apply_order_direction, compare_boundary_slots, decode_pk_cursor_boundary as decode_pk_boundary,
    validate_cursor_boundary_for_order, validate_cursor_direction, validate_cursor_window_offset,
};
pub(crate) use errors::CursorPlanError;
pub(in crate::db) use planned::{GroupedPlannedCursor, PlannedCursor};
pub(in crate::db) use range_token::{
    RangeToken, cursor_anchor_from_index_key, range_token_anchor_key,
    range_token_from_cursor_anchor, range_token_from_lowered_anchor,
};
pub(in crate::db) use token::GroupedContinuationToken;
pub(in crate::db) use token::IndexRangeCursorAnchor;
pub(crate) use token::{ContinuationSignature, ContinuationToken, ContinuationTokenError};

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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
    let Some(order) = validated_cursor_order_internal(
        plan.order.as_ref(),
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
#[allow(dead_code)]
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
