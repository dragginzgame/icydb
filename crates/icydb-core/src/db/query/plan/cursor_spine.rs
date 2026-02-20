use crate::{
    db::{
        data::StorageKey,
        index::{
            Direction, IndexId, IndexKey, IndexKeyKind, RawIndexKey, map_bound_encode_error,
            raw_bounds_for_index_component_range,
        },
        query::{
            plan::{
                AccessPath, ContinuationSignature, ContinuationToken, CursorBoundary,
                CursorBoundarySlot, CursorPlanError, IndexRangeCursorAnchor, OrderPlanError,
                OrderSpec, PlanError, PlannedCursor,
                continuation::{ContinuationTokenError, decode_typed_primary_key_cursor_slot},
            },
            predicate::{SchemaInfo, validate::literal_matches_type},
        },
    },
    model::entity::EntityModel,
    traits::{EntityKind, FieldValue},
    value::Value,
};
use std::ops::Bound;

/// Validate and materialize an executable cursor through the canonical spine.
pub(in crate::db) fn validate_planned_cursor<E>(
    cursor: Option<&[u8]>,
    access: Option<&AccessPath<E::Key>>,
    entity_path: &'static str,
    model: &EntityModel,
    order: &OrderSpec,
    expected_signature: ContinuationSignature,
    direction: Direction,
) -> Result<PlannedCursor, PlanError>
where
    E: EntityKind,
    E::Key: FieldValue,
{
    let Some(cursor) = cursor else {
        return Ok(PlannedCursor::none());
    };

    let token = decode_validated_cursor(cursor, entity_path, expected_signature, direction)?;
    validate_structured_cursor::<E>(
        token.boundary().clone(),
        token.index_range_anchor().cloned(),
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
    let index_range_anchor = cursor
        .index_range_anchor()
        .cloned()
        .map(IndexRangeCursorAnchor::new);

    validate_structured_cursor::<E>(
        boundary,
        index_range_anchor,
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
/// KeyEnvelope
///
/// Canonical raw-key envelope with direction-aware continuation semantics.
/// Centralizes anchor rewrite, containment checks, monotonic advancement, and
/// empty-envelope detection for cursor continuation paths.
///

struct KeyEnvelope<K> {
    direction: Direction,
    lower: Bound<K>,
    upper: Bound<K>,
}

impl<K> KeyEnvelope<K>
where
    K: Ord + Clone,
{
    const fn new(direction: Direction, lower: Bound<K>, upper: Bound<K>) -> Self {
        Self {
            direction,
            lower,
            upper,
        }
    }

    // Rewrite the directional continuation edge to strict "after anchor".
    fn apply_anchor(self, anchor: &K) -> Self {
        match self.direction {
            Direction::Asc => Self {
                direction: self.direction,
                lower: Bound::Excluded(anchor.clone()),
                upper: self.upper,
            },
            Direction::Desc => Self {
                direction: self.direction,
                lower: self.lower,
                upper: Bound::Excluded(anchor.clone()),
            },
        }
    }

    fn contains(&self, key: &K) -> bool {
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

    fn continuation_advanced(&self, candidate: &K, anchor: &K) -> bool {
        match self.direction {
            Direction::Asc => candidate > anchor,
            Direction::Desc => candidate < anchor,
        }
    }

    fn is_empty(&self) -> bool {
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

    fn into_bounds(self) -> (Bound<K>, Bound<K>) {
        (self.lower, self.upper)
    }

    const fn bound_key_ref(bound: &Bound<K>) -> Option<&K> {
        match bound {
            Bound::Included(value) | Bound::Excluded(value) => Some(value),
            Bound::Unbounded => None,
        }
    }
}

/// Central continuation bound rewrite for cursor resume semantics.
#[must_use]
pub(in crate::db) fn cursor_resume_bounds(
    direction: Direction,
    lower: Bound<RawIndexKey>,
    upper: Bound<RawIndexKey>,
    anchor: &RawIndexKey,
) -> (Bound<RawIndexKey>, Bound<RawIndexKey>) {
    KeyEnvelope::new(direction, lower, upper)
        .apply_anchor(anchor)
        .into_bounds()
}

/// Central envelope containment check for cursor anchors.
#[must_use]
pub(in crate::db) fn cursor_anchor_within_envelope(
    direction: Direction,
    anchor: &RawIndexKey,
    lower: &Bound<RawIndexKey>,
    upper: &Bound<RawIndexKey>,
) -> bool {
    KeyEnvelope::new(direction, lower.clone(), upper.clone()).contains(anchor)
}

/// Central strict monotonic continuation advancement check.
#[must_use]
pub(in crate::db) fn cursor_continuation_advanced(
    direction: Direction,
    candidate: &RawIndexKey,
    anchor: &RawIndexKey,
) -> bool {
    KeyEnvelope::new(direction, Bound::Unbounded, Bound::Unbounded)
        .continuation_advanced(candidate, anchor)
}

/// Central empty-envelope check for raw key bounds.
#[must_use]
pub(in crate::db) fn cursor_envelope_is_empty(
    lower: &Bound<RawIndexKey>,
    upper: &Bound<RawIndexKey>,
) -> bool {
    KeyEnvelope::new(Direction::Asc, lower.clone(), upper.clone()).is_empty()
}

// Decode and validate one continuation cursor against a canonical plan surface.
fn decode_validated_cursor(
    cursor: &[u8],
    entity_path: &'static str,
    expected_signature: ContinuationSignature,
    expected_direction: Direction,
) -> Result<ContinuationToken, PlanError> {
    let token = ContinuationToken::decode(cursor).map_err(|err| match err {
        ContinuationTokenError::Encode(message) | ContinuationTokenError::Decode(message) => {
            PlanError::invalid_continuation_cursor_payload(message)
        }
        ContinuationTokenError::UnsupportedVersion { version } => {
            PlanError::from(CursorPlanError::ContinuationCursorVersionMismatch { version })
        }
    })?;

    if token.signature() != expected_signature {
        return Err(PlanError::from(
            CursorPlanError::ContinuationCursorSignatureMismatch {
                entity_path,
                expected: expected_signature.to_string(),
                actual: token.signature().to_string(),
            },
        ));
    }
    if token.direction() != expected_direction {
        return Err(PlanError::invalid_continuation_cursor_payload(
            "continuation cursor direction does not match executable plan direction",
        ));
    }

    Ok(token)
}

// Validate the canonical structured cursor payload and materialize executor state.
fn validate_structured_cursor<E: EntityKind>(
    boundary: CursorBoundary,
    index_range_anchor: Option<IndexRangeCursorAnchor>,
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

    Ok(PlannedCursor::new(boundary, index_range_anchor))
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

// Validate optional index-range cursor anchor against the planned access envelope.
fn validate_index_range_anchor<E: EntityKind>(
    anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&AccessPath<E::Key>>,
    direction: Direction,
    require_anchor: bool,
) -> Result<(), PlanError> {
    let Some(access) = access else {
        if anchor.is_some() {
            return Err(PlanError::invalid_continuation_cursor_payload(
                "unexpected index-range continuation anchor for composite access plan",
            ));
        }

        return Ok(());
    };

    if let Some((index, prefix, lower, upper)) = access.as_index_range() {
        let Some(anchor) = anchor else {
            if require_anchor {
                return Err(PlanError::invalid_continuation_cursor_payload(
                    "index-range continuation cursor is missing a raw-key anchor",
                ));
            }

            return Ok(());
        };

        let decoded_key = IndexKey::try_from_raw(anchor.last_raw_key()).map_err(|err| {
            PlanError::invalid_continuation_cursor_payload(format!(
                "index-range continuation anchor decode failed: {err}"
            ))
        })?;
        let expected_index_id = IndexId::new::<E>(index);

        if decoded_key.index_id() != &expected_index_id {
            return Err(PlanError::invalid_continuation_cursor_payload(
                "index-range continuation anchor index id mismatch",
            ));
        }
        if decoded_key.key_kind() != IndexKeyKind::User {
            return Err(PlanError::invalid_continuation_cursor_payload(
                "index-range continuation anchor key namespace mismatch",
            ));
        }
        if decoded_key.component_count() != index.fields.len() {
            return Err(PlanError::invalid_continuation_cursor_payload(
                "index-range continuation anchor component arity mismatch",
            ));
        }
        let (range_start, range_end) = raw_bounds_for_index_component_range::<E>(
            index, prefix, lower, upper,
        )
        .map_err(|err| {
            PlanError::invalid_continuation_cursor_payload(map_bound_encode_error(
                err,
                "index-range continuation anchor prefix is not indexable",
                "index-range cursor lower continuation bound is not indexable",
                "index-range cursor upper continuation bound is not indexable",
            ))
        })?;

        if !cursor_anchor_within_envelope(
            direction,
            anchor.last_raw_key(),
            &range_start,
            &range_end,
        ) {
            return Err(PlanError::invalid_continuation_cursor_payload(
                "index-range continuation anchor is outside the original range envelope",
            ));
        }
    } else if anchor.is_some() {
        return Err(PlanError::invalid_continuation_cursor_payload(
            "unexpected index-range continuation anchor for non-index-range access path",
        ));
    }

    Ok(())
}

// Enforce that boundary and raw anchor identify the same ordered row position.
fn validate_index_range_boundary_anchor_consistency<K: FieldValue>(
    anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&AccessPath<K>>,
    boundary_pk_key: K,
) -> Result<(), PlanError> {
    let Some(anchor) = anchor else {
        return Ok(());
    };
    let Some(access) = access else {
        return Ok(());
    };
    if !access.cursor_support().supports_index_range_anchor() {
        return Ok(());
    }

    let anchor_key = IndexKey::try_from_raw(anchor.last_raw_key()).map_err(|err| {
        PlanError::invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor decode failed: {err}"
        ))
    })?;
    let anchor_storage_key = anchor_key.primary_storage_key().map_err(|err| {
        PlanError::invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor primary key decode failed: {err}"
        ))
    })?;
    let boundary_storage_key =
        StorageKey::try_from_value(&boundary_pk_key.to_value()).map_err(|err| {
            PlanError::invalid_continuation_cursor_payload(format!(
                "index-range continuation boundary primary key decode failed: {err}"
            ))
        })?;

    if anchor_storage_key != boundary_storage_key {
        return Err(PlanError::invalid_continuation_cursor_payload(
            "index-range continuation boundary/anchor mismatch",
        ));
    }

    Ok(())
}
