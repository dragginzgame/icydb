//! Module: cursor::anchor
//! Responsibility: validate and normalize index-range cursor anchors against runtime invariants.
//! Does not own: cursor token wire encoding or planner continuation policy semantics.
//! Boundary: proves anchor identity/key-namespace/component invariants before resume use.

use crate::{
    db::{
        cursor::{CursorPlanError, IndexRangeCursorAnchor},
        direction::Direction,
        executor::ExecutableAccessPath,
        index::{
            IndexId, IndexKey, IndexKeyKind, IndexRangeBoundEncodeError, KeyEnvelope,
            PrimaryKeyEquivalenceError, RawIndexKey, primary_key_matches_value,
            raw_bounds_for_semantic_index_component_range,
        },
    },
    traits::{FieldValue, Storable},
    types::EntityTag,
};
use std::borrow::Cow;

///
/// ValidatedIdentityIndexRangeCursorAnchor
///
/// Cursor anchor with canonical decode + identity/namespace invariants.
/// This state proves index-id, key-namespace, and component-arity correctness
/// but does not yet prove envelope containment.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedIdentityIndexRangeCursorAnchor {
    decoded_key: IndexKey,
    canonical_raw_key: RawIndexKey,
}

impl ValidatedIdentityIndexRangeCursorAnchor {
    const fn new(decoded_key: IndexKey, canonical_raw_key: RawIndexKey) -> Self {
        Self {
            decoded_key,
            canonical_raw_key,
        }
    }

    #[must_use]
    const fn decoded_key(&self) -> &IndexKey {
        &self.decoded_key
    }

    #[must_use]
    const fn lowered_key(&self) -> &RawIndexKey {
        &self.canonical_raw_key
    }

    #[must_use]
    fn last_raw_key(&self) -> &[u8] {
        self.canonical_raw_key.as_bytes()
    }
}

///
/// ValidatedInEnvelopeIndexRangeCursorAnchor
///
/// Cursor anchor proven against canonical index-range envelope invariants.
/// This is the only anchor state allowed to leave cursor validation boundaries.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ValidatedInEnvelopeIndexRangeCursorAnchor {
    identity: ValidatedIdentityIndexRangeCursorAnchor,
}

impl ValidatedInEnvelopeIndexRangeCursorAnchor {
    const fn from_identity(identity: ValidatedIdentityIndexRangeCursorAnchor) -> Self {
        Self { identity }
    }

    #[must_use]
    const fn decoded_key(&self) -> &IndexKey {
        self.identity.decoded_key()
    }

    #[must_use]
    pub(in crate::db::cursor) const fn lowered_key(&self) -> &RawIndexKey {
        self.identity.lowered_key()
    }

    #[must_use]
    pub(in crate::db::cursor) fn last_raw_key(&self) -> &[u8] {
        self.identity.last_raw_key()
    }

    #[must_use]
    pub(in crate::db::cursor) fn as_unvalidated_anchor(&self) -> IndexRangeCursorAnchor {
        IndexRangeCursorAnchor::new(self.last_raw_key().to_vec())
    }
}

// Decode one continuation anchor into one validated canonical anchor and enforce
// canonical round-trip encoding.
fn decode_canonical_cursor_anchor(
    anchor: &IndexRangeCursorAnchor,
) -> Result<ValidatedIdentityIndexRangeCursorAnchor, CursorPlanError> {
    let anchor_raw = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(anchor.last_raw_key()));
    let decoded_key = IndexKey::try_from_raw(&anchor_raw)
        .map_err(CursorPlanError::index_range_anchor_decode_failed)?;
    let canonical_raw = decoded_key.to_raw();
    debug_assert_eq!(
        canonical_raw.as_bytes(),
        anchor.last_raw_key(),
        "index-range continuation anchor must round-trip to identical raw bytes",
    );
    if canonical_raw.as_bytes() != anchor.last_raw_key() {
        return Err(CursorPlanError::index_range_anchor_canonical_encoding_mismatch());
    }

    Ok(ValidatedIdentityIndexRangeCursorAnchor::new(
        decoded_key,
        canonical_raw,
    ))
}

// Enforce anchor identity invariants before envelope containment checks.
fn validate_anchor_identity(
    anchor: ValidatedIdentityIndexRangeCursorAnchor,
    entity_tag: EntityTag,
    index: &crate::model::index::IndexModel,
) -> Result<ValidatedIdentityIndexRangeCursorAnchor, CursorPlanError> {
    let decoded_key = anchor.decoded_key();
    let expected_index_id = IndexId::new(entity_tag, index.ordinal());

    if decoded_key.index_id() != &expected_index_id {
        return Err(CursorPlanError::index_range_anchor_index_id_mismatch());
    }
    if decoded_key.key_kind() != IndexKeyKind::User {
        return Err(CursorPlanError::index_range_anchor_key_namespace_mismatch());
    }
    if decoded_key.component_count() != index.fields().len() {
        return Err(CursorPlanError::index_range_anchor_component_arity_mismatch());
    }

    Ok(anchor)
}

// Enforce envelope containment over identity-validated anchors.
fn validate_anchor_in_envelope(
    anchor: ValidatedIdentityIndexRangeCursorAnchor,
    entity_tag: EntityTag,
    index: &crate::model::index::IndexModel,
    prefix: &[crate::value::Value],
    lower: &std::ops::Bound<crate::value::Value>,
    upper: &std::ops::Bound<crate::value::Value>,
    _direction: Direction,
) -> Result<ValidatedInEnvelopeIndexRangeCursorAnchor, CursorPlanError> {
    let (range_start, range_end) =
        lower_cursor_anchor_index_range_bounds(entity_tag, index, prefix, lower, upper)
            .map_err(CursorPlanError::invalid_continuation_cursor_payload)?;

    if !KeyEnvelope::new(range_start, range_end).contains(anchor.lowered_key()) {
        return Err(CursorPlanError::index_range_anchor_outside_envelope());
    }

    Ok(ValidatedInEnvelopeIndexRangeCursorAnchor::from_identity(
        anchor,
    ))
}

// Lower one semantic index-range envelope into raw bounds for cursor-anchor
// containment checks. Cursor owns the scope-specific reason mapping.
fn lower_cursor_anchor_index_range_bounds(
    entity_tag: EntityTag,
    index: &crate::model::index::IndexModel,
    prefix: &[crate::value::Value],
    lower: &std::ops::Bound<crate::value::Value>,
    upper: &std::ops::Bound<crate::value::Value>,
) -> Result<(std::ops::Bound<RawIndexKey>, std::ops::Bound<RawIndexKey>), &'static str> {
    let index_id = IndexId::new(entity_tag, index.ordinal());

    raw_bounds_for_semantic_index_component_range(&index_id, index, prefix, lower, upper)
        .map_err(IndexRangeBoundEncodeError::cursor_anchor_not_indexable_reason)
}

// Validate optional index-range cursor anchor against the planned access envelope.
//
// IMPORTANT CROSS-LAYER CONTRACT:
// - This planner-layer validation checks token/envelope shape and compatibility.
// - Store-layer lookup still performs strict continuation advancement checks.
// - These two validations are intentionally redundant and must not be merged.
pub(in crate::db) fn validate_index_range_anchor<K>(
    anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&ExecutableAccessPath<'_, K>>,
    entity_tag: EntityTag,
    direction: Direction,
    require_anchor: bool,
) -> Result<Option<ValidatedInEnvelopeIndexRangeCursorAnchor>, CursorPlanError> {
    let Some(access) = access else {
        if anchor.is_some() {
            return Err(CursorPlanError::unexpected_index_range_anchor_for_composite_plan());
        }

        return Ok(None);
    };

    if let Some((index, _prefix_len)) = access.index_range_details() {
        let Some((prefix, lower, upper)) = access.index_range_semantic_bounds() else {
            return Err(CursorPlanError::index_range_anchor_semantic_bounds_required());
        };
        let Some(anchor) = anchor else {
            if require_anchor {
                return Err(CursorPlanError::index_range_anchor_required());
            }

            return Ok(None);
        };

        // Phase 1: decode and classify anchor key-space shape.
        let validated_identity =
            validate_anchor_identity(decode_canonical_cursor_anchor(anchor)?, entity_tag, &index)?;
        let validated_in_envelope = validate_anchor_in_envelope(
            validated_identity,
            entity_tag,
            &index,
            prefix,
            lower,
            upper,
            direction,
        )?;

        return Ok(Some(validated_in_envelope));
    } else if anchor.is_some() {
        return Err(CursorPlanError::unexpected_index_range_anchor_for_non_range_path());
    }

    Ok(None)
}

// Enforce that boundary and raw anchor identify the same ordered row position.
pub(in crate::db) fn validate_index_range_boundary_anchor_consistency<K: FieldValue>(
    anchor: Option<&ValidatedInEnvelopeIndexRangeCursorAnchor>,
    access: Option<&ExecutableAccessPath<'_, K>>,
    boundary_pk_key: K,
) -> Result<(), CursorPlanError> {
    let Some(anchor) = anchor else {
        return Ok(());
    };
    let Some(access) = access else {
        return Ok(());
    };
    if access.index_range_details().is_none() {
        return Ok(());
    }

    let matches_boundary =
        primary_key_matches_value(anchor.decoded_key(), &boundary_pk_key.to_value()).map_err(
            |err| match err {
                PrimaryKeyEquivalenceError::AnchorDecode { source } => {
                    CursorPlanError::index_range_anchor_primary_key_decode_failed(source)
                }
                PrimaryKeyEquivalenceError::BoundaryEncode { source } => {
                    CursorPlanError::index_range_boundary_primary_key_decode_failed(source)
                }
            },
        )?;

    if !matches_boundary {
        return Err(CursorPlanError::index_range_boundary_anchor_mismatch());
    }

    Ok(())
}
