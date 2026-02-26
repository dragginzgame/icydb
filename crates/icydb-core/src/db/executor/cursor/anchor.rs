use crate::{
    db::{
        cursor::IndexRangeCursorAnchor,
        direction::Direction,
        index::{
            Direction as IndexDirection, IndexId, IndexKey, IndexKeyKind, KeyEnvelope,
            PrimaryKeyEquivalenceError, RawIndexKey, primary_key_matches_value,
        },
        lowering::lower_cursor_anchor_index_range_bounds,
        query::plan::{AccessPath, CursorPlanError, PlanError},
    },
    traits::Storable,
    traits::{EntityKind, FieldValue},
};
use std::borrow::Cow;

// Build the canonical invalid-continuation payload error variant.
fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> PlanError {
    PlanError::from(CursorPlanError::InvalidContinuationCursorPayload {
        reason: reason.into(),
    })
}

// Decode one index-range anchor raw key and enforce canonical round-trip encoding.
// This defends against future token-shape drift where decode might accept a
// representation that does not serialize back to identical raw bytes.
fn decode_canonical_index_range_anchor_key(
    anchor: &IndexRangeCursorAnchor,
) -> Result<IndexKey, PlanError> {
    let anchor_raw = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(anchor.last_raw_key()));
    let decoded_key = IndexKey::try_from_raw(&anchor_raw).map_err(|err| {
        invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor decode failed: {err}"
        ))
    })?;
    let canonical_raw = decoded_key.to_raw();
    debug_assert_eq!(
        canonical_raw.as_bytes(),
        anchor.last_raw_key(),
        "index-range continuation anchor must round-trip to identical raw bytes",
    );
    if canonical_raw.as_bytes() != anchor.last_raw_key() {
        return Err(invalid_continuation_cursor_payload(
            "index-range continuation anchor canonical encoding mismatch",
        ));
    }

    Ok(decoded_key)
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
        let decoded_key = decode_canonical_index_range_anchor_key(anchor)?;
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
) -> Result<(), PlanError> {
    let Some(anchor) = anchor else {
        return Ok(());
    };
    let Some(access) = access else {
        return Ok(());
    };
    if !matches!(access, AccessPath::IndexRange { .. }) {
        return Ok(());
    }

    let anchor_key = decode_canonical_index_range_anchor_key(anchor)?;
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
