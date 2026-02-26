use crate::{
    db::{
        cursor::{ContinuationSignature, CursorPlanError},
        direction::Direction,
        executor::{PlannedCursor, cursor::spine},
        plan::{AccessPlannedQuery, OrderSpec},
        query::plan::PlanError,
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
) -> Result<PlannedCursor, PlanError>
where
    E::Key: FieldValue,
{
    let order = validated_cursor_order_plan(plan).map_err(PlanError::from)?;

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
) -> Result<PlannedCursor, InternalError>
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
    .map_err(InternalError::from_cursor_plan_error)
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
) -> Result<&OrderSpec, InternalError> {
    let Some(order) = plan.order.as_ref() else {
        return Err(InternalError::query_executor_invariant(
            "cursor pagination requires explicit ordering",
        ));
    };
    if order.fields.is_empty() {
        return Err(InternalError::query_executor_invariant(
            "cursor pagination requires non-empty ordering",
        ));
    }

    Ok(order)
}
