use crate::{
    db::{
        access::AccessPath,
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, CursorPlanError,
            IndexRangeCursorAnchor, PlannedCursor,
            anchor::{
                validate_index_range_anchor, validate_index_range_boundary_anchor_consistency,
            },
            validate_cursor_boundary_for_order, validate_cursor_direction,
            validate_cursor_window_offset,
        },
        direction::Direction,
        query::plan::OrderSpec,
    },
    model::entity::EntityModel,
    traits::{EntityKind, FieldValue},
};

///
/// CursorPlanSurface
///
/// Thin plan-surface contract for cursor validation.
/// This keeps structured cursor checks coupled to one semantic owner instead
/// of threading many independent plan parameters through validation helpers.
///
trait CursorPlanSurface<K: FieldValue> {
    fn entity_model(&self) -> &EntityModel;

    fn order_spec(&self) -> &OrderSpec;

    fn direction(&self) -> Direction;

    fn access(&self) -> Option<&AccessPath<K>>;

    fn initial_offset(&self) -> u32;
}

///
/// StructuredCursorPlanSurface
///
/// Concrete adapter that exposes the canonical cursor validation surface.
///
struct StructuredCursorPlanSurface<'a, K> {
    access: Option<&'a AccessPath<K>>,
    model: &'a EntityModel,
    order: &'a OrderSpec,
    direction: Direction,
    initial_offset: u32,
}

impl<K: FieldValue> CursorPlanSurface<K> for StructuredCursorPlanSurface<'_, K> {
    fn entity_model(&self) -> &EntityModel {
        self.model
    }

    fn order_spec(&self) -> &OrderSpec {
        self.order
    }

    fn direction(&self) -> Direction {
        self.direction
    }

    fn access(&self) -> Option<&AccessPath<K>> {
        self.access
    }

    fn initial_offset(&self) -> u32 {
        self.initial_offset
    }
}

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
) -> Result<PlannedCursor, CursorPlanError>
where
    E: EntityKind,
    E::Key: FieldValue,
{
    let Some(cursor) = cursor else {
        return Ok(PlannedCursor::none());
    };

    let surface = StructuredCursorPlanSurface {
        access,
        model,
        order,
        direction,
        initial_offset: expected_initial_offset,
    };
    let token = decode_validated_cursor(cursor, entity_path, expected_signature)?;
    validate_structured_cursor::<E, _>(
        token.boundary().clone(),
        token.index_range_anchor().cloned(),
        token.direction(),
        token.initial_offset(),
        &surface,
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
) -> Result<PlannedCursor, CursorPlanError>
where
    E: EntityKind,
    E::Key: FieldValue,
{
    if cursor.is_empty() {
        return Ok(PlannedCursor::none());
    }

    let surface = StructuredCursorPlanSurface {
        access,
        model,
        order,
        direction,
        initial_offset: expected_initial_offset,
    };
    let boundary = cursor.boundary().cloned().ok_or_else(|| {
        CursorPlanError::invalid_continuation_cursor_payload(
            "continuation cursor boundary is missing",
        )
    })?;
    let index_range_anchor = cursor.index_range_anchor().cloned();

    validate_structured_cursor::<E, _>(
        boundary,
        index_range_anchor,
        direction,
        cursor.initial_offset(),
        &surface,
        false,
    )
}

// Decode and validate one continuation cursor against a canonical plan surface.
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
// Validate continuation token signature against the executable signature.
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

// Validate the canonical structured cursor payload and materialize executor state.
fn validate_structured_cursor<E: EntityKind, S: CursorPlanSurface<E::Key>>(
    boundary: CursorBoundary,
    index_range_anchor: Option<IndexRangeCursorAnchor>,
    actual_direction: Direction,
    actual_initial_offset: u32,
    surface: &S,
    require_index_range_anchor: bool,
) -> Result<PlannedCursor, CursorPlanError>
where
    E::Key: FieldValue,
{
    validate_cursor_boundary_anchor_invariants::<E, S>(
        &boundary,
        index_range_anchor.as_ref(),
        actual_direction,
        actual_initial_offset,
        surface,
        require_index_range_anchor,
    )?;

    Ok(PlannedCursor::new(
        boundary,
        index_range_anchor,
        actual_initial_offset,
    ))
}

// Shared invariant gate for decoded cursor boundary + optional index-range anchor.
//
// This is the single cursor-spine boundary for direction, window-shape,
// boundary arity/type, and index-range anchor compatibility checks.
fn validate_cursor_boundary_anchor_invariants<E: EntityKind, S: CursorPlanSurface<E::Key>>(
    boundary: &CursorBoundary,
    index_range_anchor: Option<&IndexRangeCursorAnchor>,
    actual_direction: Direction,
    actual_initial_offset: u32,
    surface: &S,
    require_index_range_anchor: bool,
) -> Result<(), CursorPlanError>
where
    E::Key: FieldValue,
{
    let expected_direction = surface.direction();
    validate_cursor_direction(expected_direction, actual_direction)?;

    let expected_initial_offset = surface.initial_offset();
    validate_cursor_window_offset(expected_initial_offset, actual_initial_offset)?;
    validate_index_range_anchor::<E>(
        index_range_anchor,
        surface.access(),
        actual_direction,
        require_index_range_anchor,
    )?;

    let pk_key = validate_cursor_boundary_for_order::<E::Key>(
        surface.entity_model(),
        surface.order_spec(),
        boundary,
    )?;
    validate_index_range_boundary_anchor_consistency(index_range_anchor, surface.access(), pk_key)?;

    Ok(())
}
