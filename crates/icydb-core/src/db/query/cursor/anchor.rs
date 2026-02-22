use crate::{
    db::{
        data::StorageKey,
        index::{
            Direction, IndexId, IndexKey, IndexKeyKind, IndexRangeNotIndexableReasonScope,
            map_index_range_not_indexable_reason,
        },
        query::plan::{
            AccessPath, CursorPlanError, IndexRangeCursorAnchor, KeyEnvelope, PlanError,
            raw_bounds_for_semantic_index_component_range,
        },
    },
    traits::{EntityKind, FieldValue},
};

// Build the canonical invalid-continuation payload error variant.
fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> PlanError {
    PlanError::from(CursorPlanError::InvalidContinuationCursorPayload {
        reason: reason.into(),
    })
}

// Validate optional index-range cursor anchor against the planned access envelope.
pub(in crate::db::query) fn validate_index_range_anchor<E: EntityKind>(
    anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&AccessPath<E::Key>>,
    direction: Direction,
    require_anchor: bool,
) -> Result<(), PlanError> {
    let Some(access) = access else {
        if anchor.is_some() {
            return Err(invalid_continuation_cursor_payload(
                "unexpected index-range continuation anchor for composite access plan",
            ));
        }

        return Ok(());
    };

    if let Some((index, prefix, lower, upper)) = access.as_index_range() {
        let Some(anchor) = anchor else {
            if require_anchor {
                return Err(invalid_continuation_cursor_payload(
                    "index-range continuation cursor is missing a raw-key anchor",
                ));
            }

            return Ok(());
        };

        // Phase 1: decode and classify anchor key-space shape.
        let decoded_key = IndexKey::try_from_raw(anchor.last_raw_key()).map_err(|err| {
            invalid_continuation_cursor_payload(format!(
                "index-range continuation anchor decode failed: {err}"
            ))
        })?;
        let expected_index_id = IndexId::new::<E>(index);

        if decoded_key.index_id() != &expected_index_id {
            return Err(invalid_continuation_cursor_payload(
                "index-range continuation anchor index id mismatch",
            ));
        }
        if decoded_key.key_kind() != IndexKeyKind::User {
            return Err(invalid_continuation_cursor_payload(
                "index-range continuation anchor key namespace mismatch",
            ));
        }
        if decoded_key.component_count() != index.fields.len() {
            return Err(invalid_continuation_cursor_payload(
                "index-range continuation anchor component arity mismatch",
            ));
        }

        // Phase 2: validate envelope membership against planned range bounds.
        let (range_start, range_end) =
            raw_bounds_for_semantic_index_component_range::<E>(index, prefix, lower, upper)
                .map_err(|err| {
                    invalid_continuation_cursor_payload(map_index_range_not_indexable_reason(
                        IndexRangeNotIndexableReasonScope::CursorContinuationAnchor,
                        err,
                    ))
                })?;

        if !KeyEnvelope::new(direction, range_start, range_end).contains(anchor.last_raw_key()) {
            return Err(invalid_continuation_cursor_payload(
                "index-range continuation anchor is outside the original range envelope",
            ));
        }
    } else if anchor.is_some() {
        return Err(invalid_continuation_cursor_payload(
            "unexpected index-range continuation anchor for non-index-range access path",
        ));
    }

    Ok(())
}

// Enforce that boundary and raw anchor identify the same ordered row position.
pub(in crate::db::query) fn validate_index_range_boundary_anchor_consistency<K: FieldValue>(
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
        invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor decode failed: {err}"
        ))
    })?;
    let anchor_storage_key = anchor_key.primary_storage_key().map_err(|err| {
        invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor primary key decode failed: {err}"
        ))
    })?;
    let boundary_storage_key =
        StorageKey::try_from_value(&boundary_pk_key.to_value()).map_err(|err| {
            invalid_continuation_cursor_payload(format!(
                "index-range continuation boundary primary key decode failed: {err}"
            ))
        })?;

    if anchor_storage_key != boundary_storage_key {
        return Err(invalid_continuation_cursor_payload(
            "index-range continuation boundary/anchor mismatch",
        ));
    }

    Ok(())
}
