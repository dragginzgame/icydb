use crate::{
    db::{
        access::AccessPath,
        cursor::{CursorPlanError, IndexRangeCursorAnchor},
        direction::Direction,
        executor::lower_cursor_anchor_index_range_bounds,
        executor::{CursorAnchor, ExecutorPlanError, decode_canonical_cursor_anchor_index_key},
        index::{
            Direction as IndexDirection, IndexId, IndexKeyKind, KeyEnvelope,
            PrimaryKeyEquivalenceError, primary_key_matches_value,
        },
    },
    traits::{EntityKind, FieldValue},
};

// Build the canonical invalid-continuation payload error variant.
fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> ExecutorPlanError {
    ExecutorPlanError::from(CursorPlanError::InvalidContinuationCursorPayload {
        reason: reason.into(),
    })
}

// Validate optional index-range cursor anchor against the planned access envelope.
//
// IMPORTANT CROSS-LAYER CONTRACT:
// - This planner-layer validation checks token/envelope shape and compatibility.
// - Store-layer lookup still performs strict continuation advancement checks.
// - These two validations are intentionally redundant and must not be merged.
pub(in crate::db) fn validate_index_range_anchor<E: EntityKind>(
    anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&AccessPath<E::Key>>,
    direction: Direction,
    require_anchor: bool,
) -> Result<(), ExecutorPlanError> {
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
        let decoded_key = decode_canonical_cursor_anchor_index_key(CursorAnchor::new(anchor))?;
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
            lower_cursor_anchor_index_range_bounds::<E>(index, prefix, lower, upper)
                .map_err(invalid_continuation_cursor_payload)?;
        let anchor_raw = decoded_key.to_raw();
        let index_direction = match direction {
            Direction::Asc => IndexDirection::Asc,
            Direction::Desc => IndexDirection::Desc,
        };

        if !KeyEnvelope::new(index_direction, range_start, range_end).contains(&anchor_raw) {
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
pub(in crate::db) fn validate_index_range_boundary_anchor_consistency<K: FieldValue>(
    anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&AccessPath<K>>,
    boundary_pk_key: K,
) -> Result<(), ExecutorPlanError> {
    let Some(anchor) = anchor else {
        return Ok(());
    };
    let Some(access) = access else {
        return Ok(());
    };
    if !matches!(access, AccessPath::IndexRange { .. }) {
        return Ok(());
    }

    let anchor_key = decode_canonical_cursor_anchor_index_key(CursorAnchor::new(anchor))?;
    let matches_boundary = primary_key_matches_value(&anchor_key, &boundary_pk_key.to_value())
        .map_err(|err| match err {
            PrimaryKeyEquivalenceError::AnchorDecode { source } => {
                invalid_continuation_cursor_payload(format!(
                    "index-range continuation anchor primary key decode failed: {source}"
                ))
            }
            PrimaryKeyEquivalenceError::BoundaryEncode { source } => {
                invalid_continuation_cursor_payload(format!(
                    "index-range continuation boundary primary key decode failed: {source}"
                ))
            }
        })?;

    if !matches_boundary {
        return Err(invalid_continuation_cursor_payload(
            "index-range continuation boundary/anchor mismatch",
        ));
    }

    Ok(())
}
