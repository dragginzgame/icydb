//! Module: cursor::spine
//! Responsibility: centralized cursor validation spine for continuation resume contracts.
//! Does not own: cursor wire serialization format or planner query-shape semantics.
//! Boundary: validates direction/window/boundary/anchor invariants before resume execution.

use crate::{
    db::KeyValueCodec,
    db::{
        access::ExecutionPathPayload,
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, CursorPlanError,
            GroupedContinuationToken, IndexRangeCursorAnchor, ValidatedCursor,
            ValidatedGroupedCursor, ValidatedInEnvelopeIndexRangeCursorAnchor,
            anchor::{
                validate_index_range_anchor, validate_index_range_boundary_anchor_consistency,
            },
            validate_cursor_boundary_for_order, validate_cursor_direction,
            validate_cursor_window_offset,
        },
        direction::Direction,
        query::plan::OrderSpec,
        schema::SchemaInfo,
    },
    types::EntityTag,
};

/// Immutable plan facts required by structured cursor validation.
///
/// Keeping these facts together prevents the nested invariant gates from
/// accepting independently mismatched plan parameters.
struct CursorValidationContext<'a, K> {
    access: Option<ExecutionPathPayload<'a, K>>,
    schema: &'a SchemaInfo,
    order: &'a OrderSpec,
    expected_direction: Direction,
    expected_initial_offset: u32,
}

/// Validate and materialize an executable cursor through the canonical spine.
#[expect(clippy::too_many_arguments)]
pub(in crate::db::cursor) fn validate_cursor_token<K>(
    cursor: Option<&[u8]>,
    access: Option<ExecutionPathPayload<'_, K>>,
    entity_path: &'static str,
    entity_tag: EntityTag,
    schema: &SchemaInfo,
    order: &OrderSpec,
    expected_signature: ContinuationSignature,
    direction: Direction,
    expected_initial_offset: u32,
) -> Result<ValidatedCursor, CursorPlanError>
where
    K: KeyValueCodec,
{
    let Some(cursor) = cursor else {
        return Ok(ValidatedCursor::none());
    };

    let context = CursorValidationContext {
        access,
        schema,
        order,
        expected_direction: direction,
        expected_initial_offset,
    };
    let token = decode_validated_cursor(cursor, entity_path, expected_signature)?;
    validate_structured_cursor(
        token.boundary().clone(),
        token.index_range_anchor().cloned(),
        token.direction(),
        token.initial_offset(),
        entity_tag,
        &context,
        true,
    )
}

/// Validate an executor-provided cursor state through the canonical cursor spine.
pub(in crate::db::cursor) fn validate_cursor_state<K>(
    cursor: ValidatedCursor,
    access: Option<ExecutionPathPayload<'_, K>>,
    entity_tag: EntityTag,
    schema: &SchemaInfo,
    order: &OrderSpec,
    direction: Direction,
    expected_initial_offset: u32,
) -> Result<ValidatedCursor, CursorPlanError>
where
    K: KeyValueCodec,
{
    if cursor.is_empty() {
        return Ok(ValidatedCursor::none());
    }

    let context = CursorValidationContext {
        access,
        schema,
        order,
        expected_direction: direction,
        expected_initial_offset,
    };
    let boundary = cursor
        .boundary()
        .cloned()
        .ok_or_else(CursorPlanError::continuation_cursor_invariant)?;
    let index_range_anchor = cursor
        .index_range_anchor()
        .map(ValidatedInEnvelopeIndexRangeCursorAnchor::as_unvalidated_anchor);

    validate_structured_cursor(
        boundary,
        index_range_anchor,
        direction,
        cursor.initial_offset(),
        entity_tag,
        &context,
        false,
    )
}

/// Decode and validate one continuation cursor against a canonical plan surface.
fn decode_validated_cursor(
    cursor: &[u8],
    entity_path: &'static str,
    expected_signature: ContinuationSignature,
) -> Result<ContinuationToken, CursorPlanError> {
    let token =
        ContinuationToken::decode(cursor).map_err(CursorPlanError::from_token_wire_error)?;

    // Signature is validated at token-decode boundary. Direction/window and
    // boundary/anchor invariants are validated together in one shared gate.
    validate_cursor_signature(entity_path, &expected_signature, &token.signature())?;

    Ok(token)
}
/// Validate continuation token signature against the executable signature.
fn validate_cursor_signature(
    entity_path: &'static str,
    expected_signature: &ContinuationSignature,
    actual_signature: &ContinuationSignature,
) -> Result<(), CursorPlanError> {
    if actual_signature != expected_signature {
        return Err(CursorPlanError::continuation_cursor_signature_mismatch(
            entity_path,
            expected_signature,
            actual_signature,
        ));
    }

    Ok(())
}

/// Validate the canonical structured cursor payload and materialize executor state.
fn validate_structured_cursor<K: KeyValueCodec>(
    boundary: CursorBoundary,
    index_range_anchor: Option<IndexRangeCursorAnchor>,
    actual_direction: Direction,
    actual_initial_offset: u32,
    entity_tag: EntityTag,
    context: &CursorValidationContext<'_, K>,
    require_index_range_anchor: bool,
) -> Result<ValidatedCursor, CursorPlanError> {
    let validated_index_range_anchor = validate_cursor_boundary_anchor_invariants(
        &boundary,
        index_range_anchor.as_ref(),
        actual_direction,
        actual_initial_offset,
        entity_tag,
        context,
        require_index_range_anchor,
    )?;

    Ok(ValidatedCursor::new_validated(
        boundary,
        validated_index_range_anchor,
        actual_initial_offset,
    ))
}

/// Shared invariant gate for decoded cursor boundary + optional index-range anchor.
///
/// This is the single cursor-spine boundary for direction, window-shape,
/// boundary arity/type, and index-range anchor consistency checks.
fn validate_cursor_boundary_anchor_invariants<K: KeyValueCodec>(
    boundary: &CursorBoundary,
    index_range_anchor: Option<&IndexRangeCursorAnchor>,
    actual_direction: Direction,
    actual_initial_offset: u32,
    entity_tag: EntityTag,
    context: &CursorValidationContext<'_, K>,
    require_index_range_anchor: bool,
) -> Result<Option<ValidatedInEnvelopeIndexRangeCursorAnchor>, CursorPlanError> {
    validate_cursor_direction(context.expected_direction, actual_direction)?;

    validate_cursor_window_offset(context.expected_initial_offset, actual_initial_offset)?;
    let validated_index_range_anchor = validate_index_range_anchor(
        index_range_anchor,
        context.access.as_ref(),
        entity_tag,
        actual_direction,
        require_index_range_anchor,
    )?;

    let pk_key = validate_cursor_boundary_for_order(context.schema, context.order, boundary)?;
    validate_index_range_boundary_anchor_consistency(
        validated_index_range_anchor.as_ref(),
        context.access.as_ref(),
        &pk_key,
    )?;

    Ok(validated_index_range_anchor)
}

/// Validate and materialize grouped cursor state through the canonical cursor spine.
#[cfg(test)]
pub(in crate::db::cursor) fn validate_grouped_cursor(
    cursor: Option<&[u8]>,
    entity_path: &'static str,
    continuation_signature: ContinuationSignature,
    expected_direction: Direction,
    expected_initial_offset: u32,
) -> Result<ValidatedGroupedCursor, CursorPlanError> {
    let Some(cursor) = cursor else {
        return Ok(ValidatedGroupedCursor::none());
    };
    let token =
        GroupedContinuationToken::decode(cursor).map_err(CursorPlanError::from_token_wire_error)?;

    validate_cursor_signature(entity_path, &continuation_signature, &token.signature())?;
    validate_grouped_cursor_direction(expected_direction, token.direction())?;
    validate_cursor_window_offset(expected_initial_offset, token.initial_offset())?;

    Ok(ValidatedGroupedCursor::new_validated(
        token.last_group_key().to_vec(),
        token.initial_offset(),
    ))
}

/// Validate and materialize already-decoded grouped cursor state through the
/// canonical grouped cursor spine.
pub(in crate::db::cursor) fn validate_grouped_cursor_token(
    cursor: Option<GroupedContinuationToken>,
    entity_path: &'static str,
    continuation_signature: ContinuationSignature,
    expected_direction: Direction,
    expected_initial_offset: u32,
) -> Result<ValidatedGroupedCursor, CursorPlanError> {
    let Some(token) = cursor else {
        return Ok(ValidatedGroupedCursor::none());
    };
    let (signature, last_group_key, direction, initial_offset) = token.into_components();

    validate_cursor_signature(entity_path, &continuation_signature, &signature)?;
    validate_grouped_cursor_direction(expected_direction, direction)?;
    validate_cursor_window_offset(expected_initial_offset, initial_offset)?;

    Ok(ValidatedGroupedCursor::new_validated(
        last_group_key,
        initial_offset,
    ))
}

/// Revalidate grouped cursor offset compatibility for executor-provided state.
pub(in crate::db::cursor) fn validate_grouped_cursor_state(
    expected_initial_offset: u32,
    cursor: ValidatedGroupedCursor,
) -> Result<ValidatedGroupedCursor, CursorPlanError> {
    if cursor.is_empty() {
        return Ok(ValidatedGroupedCursor::none());
    }
    validate_cursor_window_offset(expected_initial_offset, cursor.initial_offset())?;

    Ok(cursor)
}
// Grouped continuation cursors must match the grouped execution direction so
// resume-boundary filtering stays consistent with grouped page ordering.
fn validate_grouped_cursor_direction(
    expected_direction: Direction,
    actual_direction: Direction,
) -> Result<(), CursorPlanError> {
    if actual_direction != expected_direction {
        return Err(CursorPlanError::grouped_continuation_cursor_direction_mismatch());
    }

    Ok(())
}
