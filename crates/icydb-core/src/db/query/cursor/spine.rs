use crate::{
    db::{
        index::Direction,
        query::{
            plan::{
                AccessPath, ContinuationSignature, ContinuationToken, CursorBoundary,
                CursorBoundarySlot, CursorPlanError, IndexRangeCursorAnchor, OrderPlanError,
                OrderSpec, PlanError, PlannedCursor,
                anchor::{
                    validate_index_range_anchor, validate_index_range_boundary_anchor_consistency,
                },
                continuation::{ContinuationTokenError, decode_typed_primary_key_cursor_slot},
            },
            predicate::{SchemaInfo, validate::literal_matches_type},
        },
    },
    model::entity::EntityModel,
    traits::{EntityKind, FieldValue},
    value::Value,
};
use std::{cmp::Ordering, ops::Bound};

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

    let token = decode_validated_cursor(
        cursor,
        entity_path,
        expected_signature,
        direction,
        expected_initial_offset,
    )?;
    validate_structured_cursor::<E>(
        token.boundary().clone(),
        token.index_range_anchor().cloned(),
        token.initial_offset(),
        access,
        model,
        order,
        direction,
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

    // Reuse the canonical cursor window compatibility check.
    validate_cursor_window_offset(expected_initial_offset, cursor.initial_offset())?;

    let boundary = cursor.boundary().cloned().ok_or_else(|| {
        PlanError::invalid_continuation_cursor_payload("continuation cursor boundary is missing")
    })?;
    let index_range_anchor = cursor
        .index_range_anchor()
        .cloned()
        .map(IndexRangeCursorAnchor::new);

    validate_structured_cursor::<E>(
        boundary,
        index_range_anchor,
        cursor.initial_offset(),
        access,
        model,
        order,
        direction,
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

///
/// DirectionComparator
///
/// Direction-aware key comparator used by cursor resume and continuation checks.
/// Keeps strict "after anchor" semantics in one place.
///

struct DirectionComparator {
    direction: Direction,
}

impl DirectionComparator {
    const fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn compare<K: Ord>(&self, left: &K, right: &K) -> Ordering {
        match self.direction {
            Direction::Asc => left.cmp(right),
            Direction::Desc => right.cmp(left),
        }
    }

    fn is_strictly_after<K: Ord>(&self, candidate: &K, anchor: &K) -> bool {
        self.compare(candidate, anchor).is_gt()
    }

    fn apply_anchor<K: Clone>(
        &self,
        lower: Bound<K>,
        upper: Bound<K>,
        anchor: &K,
    ) -> (Bound<K>, Bound<K>) {
        match self.direction {
            Direction::Asc => (Bound::Excluded(anchor.clone()), upper),
            Direction::Desc => (lower, Bound::Excluded(anchor.clone())),
        }
    }
}

///
/// KeyEnvelope
///
/// Canonical raw-key envelope with direction-aware continuation semantics.
/// Centralizes anchor rewrite, containment checks, monotonic advancement, and
/// empty-envelope detection for cursor continuation paths.
///

pub(in crate::db) struct KeyEnvelope<K> {
    comparator: DirectionComparator,
    lower: Bound<K>,
    upper: Bound<K>,
}

impl<K> KeyEnvelope<K>
where
    K: Ord + Clone,
{
    pub(in crate::db) const fn new(direction: Direction, lower: Bound<K>, upper: Bound<K>) -> Self {
        Self {
            comparator: DirectionComparator::new(direction),
            lower,
            upper,
        }
    }

    // Rewrite the directional continuation edge to strict "after anchor".
    pub(in crate::db) fn apply_anchor(self, anchor: &K) -> Self {
        let (lower, upper) = self.comparator.apply_anchor(self.lower, self.upper, anchor);
        Self {
            comparator: self.comparator,
            lower,
            upper,
        }
    }

    pub(in crate::db) fn contains(&self, key: &K) -> bool {
        let lower_ok = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(boundary) => key >= boundary,
            Bound::Excluded(boundary) => key > boundary,
        };
        let upper_ok = match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(boundary) => key <= boundary,
            Bound::Excluded(boundary) => key < boundary,
        };

        lower_ok && upper_ok
    }

    pub(in crate::db) fn continuation_advanced(&self, candidate: &K, anchor: &K) -> bool {
        self.comparator.is_strictly_after(candidate, anchor)
    }

    pub(in crate::db) fn is_empty(&self) -> bool {
        let (Some(lower), Some(upper)) = (
            Self::bound_key_ref(&self.lower),
            Self::bound_key_ref(&self.upper),
        ) else {
            return false;
        };

        if lower < upper {
            return false;
        }
        if lower > upper {
            return true;
        }

        !matches!(&self.lower, Bound::Included(_)) || !matches!(&self.upper, Bound::Included(_))
    }

    pub(in crate::db) fn into_bounds(self) -> (Bound<K>, Bound<K>) {
        (self.lower, self.upper)
    }

    const fn bound_key_ref(bound: &Bound<K>) -> Option<&K> {
        match bound {
            Bound::Included(value) | Bound::Excluded(value) => Some(value),
            Bound::Unbounded => None,
        }
    }
}

// Decode and validate one continuation cursor against a canonical plan surface.
fn decode_validated_cursor(
    cursor: &[u8],
    entity_path: &'static str,
    expected_signature: ContinuationSignature,
    expected_direction: Direction,
    expected_initial_offset: u32,
) -> Result<ContinuationToken, PlanError> {
    let token = ContinuationToken::decode(cursor).map_err(map_token_decode_error)?;

    // Canonical compatibility gates: signature, direction, then window shape.
    validate_cursor_signature(entity_path, &expected_signature, &token.signature())?;
    validate_cursor_direction(expected_direction, token.direction())?;
    validate_cursor_window_offset(expected_initial_offset, token.initial_offset())?;

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
    direction: Direction,
    require_index_range_anchor: bool,
) -> Result<PlannedCursor, PlanError>
where
    E::Key: FieldValue,
{
    if boundary.slots.len() != order.fields.len() {
        return Err(PlanError::from(
            CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                expected: order.fields.len(),
                found: boundary.slots.len(),
            },
        ));
    }
    validate_cursor_boundary_types(model, order, &boundary)?;
    validate_index_range_anchor::<E>(
        index_range_anchor.as_ref(),
        access,
        direction,
        require_index_range_anchor,
    )?;

    let pk_key = decode_typed_primary_key_cursor_slot::<E::Key>(model, order, &boundary)?;
    validate_index_range_boundary_anchor_consistency(index_range_anchor.as_ref(), access, pk_key)?;

    let index_range_anchor = index_range_anchor.map(|anchor| anchor.last_raw_key().clone());

    Ok(PlannedCursor::new(
        boundary,
        index_range_anchor,
        initial_offset,
    ))
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
