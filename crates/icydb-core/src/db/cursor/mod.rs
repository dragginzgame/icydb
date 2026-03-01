//! Module: cursor
//! Responsibility: continuation cursor decode/revalidation boundaries for executor routes.
//! Does not own: query planning policy, index lowering, or storage mutation semantics.
//! Boundary: accepts planner/runtime cursor context and produces validated cursor state.

#[cfg(test)]
mod tests;

mod anchor;
pub(crate) mod boundary;
mod continuation;
mod error;
mod grouped_validate;
mod order;
mod planned;
mod range_token;
mod signature;
mod spine;

pub(crate) mod token;

use crate::{
    db::{access::AccessPath, direction::Direction, query::plan::OrderSpec},
    error::InternalError,
    traits::{EntityKind, EntityValue, FieldValue},
};

pub(crate) use boundary::{CursorBoundary, CursorBoundarySlot};
pub(in crate::db) use boundary::{
    apply_order_direction, compare_boundary_slots, decode_pk_cursor_boundary as decode_pk_boundary,
    validate_cursor_boundary_for_order, validate_cursor_direction, validate_cursor_window_offset,
};
pub(in crate::db) use continuation::next_cursor_for_materialized_rows;
pub(crate) use error::CursorPlanError;
pub(in crate::db) use order::{apply_cursor_boundary, apply_order_spec, apply_order_spec_bounded};
pub(in crate::db) use planned::{GroupedPlannedCursor, PlannedCursor};
pub(in crate::db) use range_token::{
    RangeToken, cursor_anchor_from_index_key, range_token_anchor_key,
    range_token_from_cursor_anchor, range_token_from_lowered_anchor,
};
#[allow(unreachable_pub)]
pub use signature::ContinuationSignature;
pub(crate) use token::{ContinuationToken, TokenWireError};
pub(in crate::db) use token::{GroupedContinuationToken, IndexRangeCursorAnchor};

/// Validate and decode a continuation cursor into executor-ready cursor state.
pub(in crate::db) fn prepare_cursor<E: EntityKind>(
    access: Option<&AccessPath<E::Key>>,
    order: Option<&OrderSpec>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<&[u8]>,
) -> Result<PlannedCursor, CursorPlanError>
where
    E::Key: FieldValue,
{
    let order = validated_cursor_order(order)?;

    spine::validate_planned_cursor::<E>(
        cursor,
        access,
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
    access: Option<&AccessPath<E::Key>>,
    order: Option<&OrderSpec>,
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

    let order = validated_cursor_order(order)?;

    spine::validate_planned_cursor_state::<E>(
        cursor,
        access,
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

    grouped_validate::validate_grouped_cursor(
        cursor,
        entity_path,
        continuation_signature,
        initial_offset,
    )
}

/// Revalidate grouped cursor state through grouped cursor invariants.
pub(in crate::db) fn revalidate_grouped_cursor(
    initial_offset: u32,
    cursor: GroupedPlannedCursor,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    grouped_validate::revalidate_grouped_cursor_state(initial_offset, cursor)
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
fn validated_cursor_order(order: Option<&OrderSpec>) -> Result<&OrderSpec, CursorPlanError> {
    let Some(order) = validated_cursor_order_internal(
        order,
        true,
        "cursor pagination requires explicit ordering",
    )?
    else {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            InternalError::executor_invariant_message(
                "cursor pagination requires explicit ordering",
            ),
        ));
    };

    Ok(order)
}

/// Build one cursor boundary from one entity under one canonical order spec.
#[must_use]
pub(in crate::db) fn cursor_boundary_from_entity<E: EntityKind + EntityValue>(
    entity: &E,
    order: &OrderSpec,
) -> CursorBoundary {
    CursorBoundary::from_ordered_entity(entity, order)
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
            return Err(CursorPlanError::invalid_continuation_cursor_payload(
                InternalError::executor_invariant_message(missing_order_message),
            ));
        }

        return Ok(None);
    };
    if order.fields.is_empty() {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            InternalError::executor_invariant_message(
                "cursor pagination requires non-empty ordering",
            ),
        ));
    }

    Ok(Some(order))
}
