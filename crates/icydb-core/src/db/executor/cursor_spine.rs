use crate::{
    db::{
        executor::{
            PlannedCursor, decode_typed_primary_key_cursor_slot, validate_index_range_anchor,
            validate_index_range_boundary_anchor_consistency,
        },
        query::{
            contracts::cursor::{
                ContinuationSignature, ContinuationToken, ContinuationTokenError, CursorBoundary,
                CursorBoundarySlot, IndexRangeCursorAnchor,
            },
            plan::{AccessPath, CursorPlanError, Direction, OrderPlanError, OrderSpec, PlanError},
            predicate::{SchemaInfo, validate::literal_matches_type},
        },
    },
    model::entity::EntityModel,
    traits::{EntityKind, FieldValue},
    value::Value,
};

/// Validate and materialize an executable cursor through the canonical spine.
#[expect(clippy::too_many_arguments)]
pub(in crate::db) fn validate_planned_cursor<E>(
    cursor: Option<&[u8]>,
    access: Option<&AccessPath<E::Key>>,
    entity_path: &'static str,
    model: &EntityModel,
    order: &OrderSpec,
    expected_signature: ContinuationSignature,
    direction: Direction,
    expected_initial_offset: u32,
) -> Result<PlannedCursor, PlanError>
where
    E: EntityKind,
    E::Key: FieldValue,
{
    let Some(cursor) = cursor else {
        return Ok(PlannedCursor::none());
    };

    let token = decode_validated_cursor(cursor, entity_path, expected_signature)?;
    validate_structured_cursor::<E>(
        token.boundary().clone(),
        token.index_range_anchor().cloned(),
        token.initial_offset(),
        access,
        model,
        order,
        direction,
        token.direction(),
        expected_initial_offset,
        true,
    )
}

/// Validate an executor-provided cursor state through the canonical cursor spine.
pub(in crate::db) fn validate_planned_cursor_state<E>(
    cursor: PlannedCursor,
    access: Option<&AccessPath<E::Key>>,
    model: &EntityModel,
    order: &OrderSpec,
    direction: Direction,
    expected_initial_offset: u32,
) -> Result<PlannedCursor, PlanError>
where
    E: EntityKind,
    E::Key: FieldValue,
{
    if cursor.is_empty() {
        return Ok(PlannedCursor::none());
    }

    let boundary = cursor.boundary().cloned().ok_or_else(|| {
        PlanError::invalid_continuation_cursor_payload("continuation cursor boundary is missing")
    })?;
    let index_range_anchor = cursor.index_range_anchor().cloned();

    validate_structured_cursor::<E>(
        boundary,
        index_range_anchor,
        cursor.initial_offset(),
        access,
        model,
        order,
        direction,
        direction,
        expected_initial_offset,
        false,
    )
}

impl PlanError {
    /// Build the standard invalid-continuation payload error variant.
    fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> Self {
        Self::from(CursorPlanError::InvalidContinuationCursorPayload {
            reason: reason.into(),
        })
    }
}

// Decode and validate one continuation cursor against a canonical plan surface.
fn decode_validated_cursor(
    cursor: &[u8],
    entity_path: &'static str,
    expected_signature: ContinuationSignature,
) -> Result<ContinuationToken, PlanError> {
    let token = ContinuationToken::decode(cursor).map_err(map_token_decode_error)?;

    // Signature is validated at token-decode boundary. Direction/window and
    // boundary/anchor invariants are validated together in one shared gate.
    validate_cursor_signature(entity_path, &expected_signature, &token.signature())?;

    Ok(token)
}

// Map cursor token decode failures into canonical plan-surface cursor errors.
fn map_token_decode_error(err: ContinuationTokenError) -> PlanError {
    match err {
        ContinuationTokenError::Encode(message) | ContinuationTokenError::Decode(message) => {
            PlanError::invalid_continuation_cursor_payload(message)
        }
        ContinuationTokenError::UnsupportedVersion { version } => {
            PlanError::from(CursorPlanError::ContinuationCursorVersionMismatch { version })
        }
    }
}

// Validate continuation token signature against the executable signature.
fn validate_cursor_signature(
    entity_path: &'static str,
    expected_signature: &ContinuationSignature,
    actual_signature: &ContinuationSignature,
) -> Result<(), PlanError> {
    if actual_signature != expected_signature {
        return Err(PlanError::from(
            CursorPlanError::ContinuationCursorSignatureMismatch {
                entity_path,
                expected: expected_signature.to_string(),
                actual: actual_signature.to_string(),
            },
        ));
    }

    Ok(())
}

// Validate continuation token direction against the executable direction.
fn validate_cursor_direction(
    expected_direction: Direction,
    actual_direction: Direction,
) -> Result<(), PlanError> {
    if actual_direction != expected_direction {
        return Err(PlanError::invalid_continuation_cursor_payload(
            "continuation cursor direction does not match executable plan direction",
        ));
    }

    Ok(())
}

// Validate continuation window shape compatibility (initial offset).
fn validate_cursor_window_offset(
    expected_initial_offset: u32,
    actual_initial_offset: u32,
) -> Result<(), PlanError> {
    if actual_initial_offset != expected_initial_offset {
        return Err(PlanError::from(
            CursorPlanError::ContinuationCursorWindowMismatch {
                expected_offset: expected_initial_offset,
                actual_offset: actual_initial_offset,
            },
        ));
    }

    Ok(())
}

// Validate the canonical structured cursor payload and materialize executor state.
#[expect(clippy::too_many_arguments)]
fn validate_structured_cursor<E: EntityKind>(
    boundary: CursorBoundary,
    index_range_anchor: Option<IndexRangeCursorAnchor>,
    initial_offset: u32,
    access: Option<&AccessPath<E::Key>>,
    model: &EntityModel,
    order: &OrderSpec,
    expected_direction: Direction,
    actual_direction: Direction,
    expected_initial_offset: u32,
    require_index_range_anchor: bool,
) -> Result<PlannedCursor, PlanError>
where
    E::Key: FieldValue,
{
    validate_cursor_boundary_anchor_invariants::<E>(
        &boundary,
        index_range_anchor.as_ref(),
        access,
        model,
        order,
        expected_direction,
        actual_direction,
        expected_initial_offset,
        initial_offset,
        require_index_range_anchor,
    )?;

    Ok(PlannedCursor::new(
        boundary,
        index_range_anchor,
        initial_offset,
    ))
}

// Shared invariant gate for decoded cursor boundary + optional index-range anchor.
//
// This is the single cursor-spine boundary for direction, window-shape,
// boundary arity/type, and index-range anchor compatibility checks.
#[expect(clippy::too_many_arguments)]
fn validate_cursor_boundary_anchor_invariants<E: EntityKind>(
    boundary: &CursorBoundary,
    index_range_anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&AccessPath<E::Key>>,
    model: &EntityModel,
    order: &OrderSpec,
    expected_direction: Direction,
    actual_direction: Direction,
    expected_initial_offset: u32,
    actual_initial_offset: u32,
    require_index_range_anchor: bool,
) -> Result<(), PlanError>
where
    E::Key: FieldValue,
{
    validate_cursor_direction(expected_direction, actual_direction)?;
    validate_cursor_window_offset(expected_initial_offset, actual_initial_offset)?;

    if boundary.slots.len() != order.fields.len() {
        return Err(PlanError::from(
            CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                expected: order.fields.len(),
                found: boundary.slots.len(),
            },
        ));
    }
    validate_cursor_boundary_types(model, order, boundary)?;
    validate_index_range_anchor::<E>(
        index_range_anchor,
        access,
        actual_direction,
        require_index_range_anchor,
    )?;

    let pk_key = decode_typed_primary_key_cursor_slot::<E::Key>(model, order, boundary)?;
    validate_index_range_boundary_anchor_consistency(index_range_anchor, access, pk_key)?;

    Ok(())
}

// Validate decoded cursor boundary slot types against canonical order fields.
fn validate_cursor_boundary_types(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<(), PlanError> {
    let schema = SchemaInfo::from_entity_model(model).map_err(PlanError::from)?;

    for ((field, _), slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let field_type = schema
            .field(field)
            .ok_or_else(|| OrderPlanError::UnknownField {
                field: field.clone(),
            })
            .map_err(PlanError::from)?;

        match slot {
            CursorBoundarySlot::Missing => {
                if field == model.primary_key.name {
                    return Err(PlanError::from(
                        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                            field: field.clone(),
                            expected: field_type.to_string(),
                            value: None,
                        },
                    ));
                }
            }
            CursorBoundarySlot::Present(value) => {
                if !literal_matches_type(value, field_type) {
                    if field == model.primary_key.name {
                        return Err(PlanError::from(
                            CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                                field: field.clone(),
                                expected: field_type.to_string(),
                                value: Some(value.clone()),
                            },
                        ));
                    }

                    return Err(PlanError::from(
                        CursorPlanError::ContinuationCursorBoundaryTypeMismatch {
                            field: field.clone(),
                            expected: field_type.to_string(),
                            value: value.clone(),
                        },
                    ));
                }

                // Primary-key slots must also satisfy key decoding semantics.
                if field == model.primary_key.name && Value::as_storage_key(value).is_none() {
                    return Err(PlanError::from(
                        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                            field: field.clone(),
                            expected: field_type.to_string(),
                            value: Some(value.clone()),
                        },
                    ));
                }
            }
        }
    }

    Ok(())
}
