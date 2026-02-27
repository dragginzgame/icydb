mod anchor;
pub(crate) mod boundary;
mod errors;
mod planned;
mod spine;
pub(crate) mod token;

pub(crate) use boundary::{CursorBoundary, CursorBoundarySlot};
pub(in crate::db) use boundary::{
    apply_order_direction, compare_boundary_slots, decode_pk_cursor_boundary as decode_pk_boundary,
    validate_cursor_boundary_for_order, validate_cursor_direction, validate_cursor_window_offset,
};
pub(crate) use errors::CursorPlanError;
pub(in crate::db) use planned::PlannedCursor;
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
    let order = validated_cursor_order_plan(plan)?;

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

    let order = validated_cursor_order_internal(plan)?;

    spine::validate_planned_cursor_state::<E>(
        cursor,
        plan.access.as_path(),
        E::MODEL,
        order,
        direction,
        initial_offset,
    )
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

// Resolve cursor ordering for plan-surface cursor decoding.
fn validated_cursor_order_plan<K>(
    plan: &AccessPlannedQuery<K>,
) -> Result<&OrderSpec, CursorPlanError> {
    let Some(order) = plan.order.as_ref() else {
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: InternalError::executor_invariant_message(
                "cursor pagination requires explicit ordering",
            ),
        });
    };
    if order.fields.is_empty() {
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: InternalError::executor_invariant_message(
                "cursor pagination requires non-empty ordering",
            ),
        });
    }

    Ok(order)
}

// Resolve cursor ordering for executor-provided cursor-state revalidation.
fn validated_cursor_order_internal<K>(
    plan: &AccessPlannedQuery<K>,
) -> Result<&OrderSpec, CursorPlanError> {
    let Some(order) = plan.order.as_ref() else {
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: InternalError::executor_invariant_message(
                "cursor pagination requires explicit ordering",
            ),
        });
    };
    if order.fields.is_empty() {
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: InternalError::executor_invariant_message(
                "cursor pagination requires non-empty ordering",
            ),
        });
    }

    Ok(order)
}
