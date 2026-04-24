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
mod planned;
mod range_token;
mod runtime;
mod signature;
pub(in crate::db) mod spine;
pub(crate) mod string;

pub(crate) mod token;

use crate::{
    db::{
        direction::Direction,
        executor::ExecutionPathPayload,
        query::plan::{OrderSpec, validate_cursor_order_plan_shape},
    },
    traits::KeyValueCodec,
    types::EntityTag,
};

pub(in crate::db) use anchor::ValidatedInEnvelopeIndexRangeCursorAnchor;
pub(crate) use boundary::{CursorBoundary, CursorBoundarySlot};
pub(in crate::db) use boundary::{
    apply_order_direction, decode_pk_cursor_boundary_storage_key_for_name,
    validate_cursor_boundary_for_order, validate_cursor_direction, validate_cursor_window_offset,
};
pub(in crate::db) use continuation::{
    IndexScanContinuationInput, MaterializedCursorRow, effective_keep_count_for_limit,
    effective_page_offset_for_window, next_cursor_for_materialized_rows,
};
pub(crate) use error::CursorPlanError;
pub(in crate::db) use planned::{GroupedPlannedCursor, PlannedCursor};
pub(in crate::db) use range_token::{
    RangeToken, cursor_anchor_from_raw_index_key, range_token_anchor_key,
    range_token_from_validated_cursor_anchor,
};
pub(in crate::db) use runtime::window_cursor_contract_for_plan;
pub(in crate::db) use runtime::{
    ContinuationKeyRef, ContinuationRuntime, LoopAction, WindowCursorContract,
};
#[expect(unreachable_pub)]
pub use signature::ContinuationSignature;
pub(in crate::db) use string::encode_grouped_cursor_token;
#[expect(unreachable_pub)]
pub use string::{CursorDecodeError, decode_cursor, encode_cursor};
pub(crate) use token::{ContinuationToken, TokenWireError};
pub(in crate::db) use token::{GroupedContinuationToken, IndexRangeCursorAnchor};

/// Decode one optional external continuation token through cursor-runtime authority.
pub(in crate::db) fn decode_optional_cursor_token(
    cursor_token: Option<&str>,
) -> Result<Option<Vec<u8>>, CursorPlanError> {
    cursor_token
        .map(|token| decode_cursor(token).map_err(CursorPlanError::invalid_continuation_cursor))
        .transpose()
}

/// Decode one optional grouped cursor token through the existing external
/// hex-token boundary while preserving grouped-token ownership for downstream
/// validation.
pub(in crate::db) fn decode_optional_grouped_cursor_token(
    cursor_token: Option<&str>,
) -> Result<Option<GroupedContinuationToken>, CursorPlanError> {
    decode_optional_cursor_token(cursor_token)?
        .map(|bytes| {
            GroupedContinuationToken::decode(bytes.as_slice())
                .map_err(CursorPlanError::from_token_wire_error)
        })
        .transpose()
}

/// Validate and decode a continuation cursor into executor-ready cursor state.
#[expect(clippy::too_many_arguments)]
pub(in crate::db) fn prepare_cursor<K: KeyValueCodec>(
    access: Option<ExecutionPathPayload<'_, K>>,
    entity_path: &'static str,
    entity_tag: EntityTag,
    model: &crate::model::entity::EntityModel,
    order: Option<&OrderSpec>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<&[u8]>,
) -> Result<PlannedCursor, CursorPlanError> {
    let order = validated_cursor_order(order)?;

    spine::validate_planned_cursor(
        cursor,
        access,
        entity_path,
        entity_tag,
        model,
        order,
        continuation_signature,
        direction,
        initial_offset,
    )
}

/// Revalidate executor-provided cursor state through the canonical cursor spine.
pub(in crate::db) fn revalidate_cursor<K: KeyValueCodec>(
    access: Option<ExecutionPathPayload<'_, K>>,
    entity_tag: EntityTag,
    model: &crate::model::entity::EntityModel,
    order: Option<&OrderSpec>,
    direction: Direction,
    initial_offset: u32,
    cursor: PlannedCursor,
) -> Result<PlannedCursor, CursorPlanError> {
    if cursor.is_empty() {
        return Ok(PlannedCursor::none());
    }

    let order = validated_cursor_order(order)?;

    spine::validate_planned_cursor_state(
        cursor,
        access,
        entity_tag,
        model,
        order,
        direction,
        initial_offset,
    )
}

/// Validate and decode a grouped continuation cursor into grouped cursor state.
#[cfg(test)]
pub(in crate::db) fn prepare_grouped_cursor(
    entity_path: &'static str,
    order: Option<&OrderSpec>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<&[u8]>,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    validate_grouped_cursor_order_plan(order)?;

    spine::validate_grouped_cursor(
        cursor,
        entity_path,
        continuation_signature,
        direction,
        initial_offset,
    )
}

/// Validate one already-decoded grouped continuation token into grouped
/// executor cursor state.
pub(in crate::db) fn prepare_grouped_cursor_token(
    entity_path: &'static str,
    order: Option<&OrderSpec>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<GroupedContinuationToken>,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    validate_grouped_cursor_order_plan(order)?;

    spine::validate_grouped_cursor_token(
        cursor,
        entity_path,
        continuation_signature,
        direction,
        initial_offset,
    )
}

/// Revalidate grouped cursor state through grouped cursor invariants.
pub(in crate::db) fn revalidate_grouped_cursor(
    initial_offset: u32,
    cursor: GroupedPlannedCursor,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    spine::validate_grouped_cursor_state(initial_offset, cursor)
}

// Resolve cursor ordering for plan-surface decoding and executor revalidation.
fn validated_cursor_order(order: Option<&OrderSpec>) -> Result<&OrderSpec, CursorPlanError> {
    let Some(order) = validated_cursor_order_internal(
        order,
        true,
        CursorPlanError::cursor_requires_order_message(),
    )?
    else {
        return Err(CursorPlanError::cursor_requires_order());
    };

    Ok(order)
}

/// Validate grouped cursor ordering plan shape.
///
/// Grouped pagination uses canonical lexicographic group-key order by default,
/// so explicit ordering is optional, but empty order specs remain invalid.
pub(in crate::db) fn validate_grouped_cursor_order_plan(
    order: Option<&OrderSpec>,
) -> Result<(), CursorPlanError> {
    validated_cursor_order_internal(
        order,
        false,
        "grouped cursor pagination uses canonical group-key order when ORDER BY is omitted",
    )
    .map(|_| ())
}

fn validated_cursor_order_internal<'a>(
    order: Option<&'a OrderSpec>,
    require_explicit_order: bool,
    missing_order_message: &'static str,
) -> Result<Option<&'a OrderSpec>, CursorPlanError> {
    validate_cursor_order_plan_shape(order, require_explicit_order)
        .map_err(|err| err.to_cursor_plan_error(missing_order_message))
}
