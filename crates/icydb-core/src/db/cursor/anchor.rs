//! Module: cursor::anchor
//! Responsibility: validate and normalize index-range cursor anchors against runtime invariants.
//! Does not own: cursor token wire encoding or planner continuation policy semantics.
//! Boundary: proves anchor identity/key-namespace/component invariants before resume use.

use crate::{
    db::{
        access::lower_cursor_anchor_index_range_bounds,
        cursor::{CursorPlanError, IndexRangeCursorAnchor},
        direction::Direction,
        executor::ExecutableAccessPath,
        index::{
            IndexId, IndexKey, IndexKeyKind, KeyEnvelope, PrimaryKeyEquivalenceError, RawIndexKey,
            primary_key_matches_value,
        },
    },
    traits::{EntityKind, FieldValue, Storable},
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
    let decoded_key = IndexKey::try_from_raw(&anchor_raw).map_err(|err| {
        CursorPlanError::invalid_continuation_cursor_payload(format!(
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
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "index-range continuation anchor canonical encoding mismatch",
        ));
    }

    Ok(ValidatedIdentityIndexRangeCursorAnchor::new(
        decoded_key,
        canonical_raw,
    ))
}

// Enforce anchor identity invariants before envelope containment checks.
fn validate_anchor_identity<E: EntityKind>(
    anchor: ValidatedIdentityIndexRangeCursorAnchor,
    index: &crate::model::index::IndexModel,
) -> Result<ValidatedIdentityIndexRangeCursorAnchor, CursorPlanError> {
    let decoded_key = anchor.decoded_key();
    let expected_index_id = IndexId::new::<E>(index);

    if decoded_key.index_id() != &expected_index_id {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "index-range continuation anchor index id mismatch",
        ));
    }
    if decoded_key.key_kind() != IndexKeyKind::User {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "index-range continuation anchor key namespace mismatch",
        ));
    }
    if decoded_key.component_count() != index.fields().len() {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "index-range continuation anchor component arity mismatch",
        ));
    }

    Ok(anchor)
}

// Enforce envelope containment over identity-validated anchors.
fn validate_anchor_in_envelope<E: EntityKind>(
    anchor: ValidatedIdentityIndexRangeCursorAnchor,
    index: &crate::model::index::IndexModel,
    prefix: &[crate::value::Value],
    lower: &std::ops::Bound<crate::value::Value>,
    upper: &std::ops::Bound<crate::value::Value>,
    direction: Direction,
) -> Result<ValidatedInEnvelopeIndexRangeCursorAnchor, CursorPlanError> {
    let (range_start, range_end) =
        lower_cursor_anchor_index_range_bounds::<E>(index, prefix, lower, upper)
            .map_err(CursorPlanError::invalid_continuation_cursor_payload)?;

    if !KeyEnvelope::new(direction, range_start, range_end).contains(anchor.lowered_key()) {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "index-range continuation anchor is outside the original range envelope",
        ));
    }

    Ok(ValidatedInEnvelopeIndexRangeCursorAnchor::from_identity(
        anchor,
    ))
}

// Validate optional index-range cursor anchor against the planned access envelope.
//
// IMPORTANT CROSS-LAYER CONTRACT:
// - This planner-layer validation checks token/envelope shape and compatibility.
// - Store-layer lookup still performs strict continuation advancement checks.
// - These two validations are intentionally redundant and must not be merged.
pub(in crate::db) fn validate_index_range_anchor<E: EntityKind>(
    anchor: Option<&IndexRangeCursorAnchor>,
    access: Option<&ExecutableAccessPath<'_, E::Key>>,
    direction: Direction,
    require_anchor: bool,
) -> Result<Option<ValidatedInEnvelopeIndexRangeCursorAnchor>, CursorPlanError> {
    let Some(access) = access else {
        if anchor.is_some() {
            return Err(CursorPlanError::invalid_continuation_cursor_payload(
                "unexpected index-range continuation anchor for composite access plan",
            ));
        }

        return Ok(None);
    };

    if let Some((index, _prefix_len)) = access.index_range_details() {
        let Some((prefix, lower, upper)) = access.index_range_semantic_bounds() else {
            return Err(CursorPlanError::invalid_continuation_cursor_payload(
                "index-range continuation validation is missing semantic bounds payload",
            ));
        };
        let Some(anchor) = anchor else {
            if require_anchor {
                return Err(CursorPlanError::invalid_continuation_cursor_payload(
                    "index-range continuation cursor is missing a raw-key anchor",
                ));
            }

            return Ok(None);
        };

        // Phase 1: decode and classify anchor key-space shape.
        let validated_identity =
            validate_anchor_identity::<E>(decode_canonical_cursor_anchor(anchor)?, &index)?;
        let validated_in_envelope = validate_anchor_in_envelope::<E>(
            validated_identity,
            &index,
            prefix,
            lower,
            upper,
            direction,
        )?;

        return Ok(Some(validated_in_envelope));
    } else if anchor.is_some() {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "unexpected index-range continuation anchor for non-index-range access path",
        ));
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
                    CursorPlanError::invalid_continuation_cursor_payload(format!(
                        "index-range continuation anchor primary key decode failed: {source}"
                    ))
                }
                PrimaryKeyEquivalenceError::BoundaryEncode { source } => {
                    CursorPlanError::invalid_continuation_cursor_payload(format!(
                        "index-range continuation boundary primary key decode failed: {source}"
                    ))
                }
            },
        )?;

    if !matches_boundary {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "index-range continuation boundary/anchor mismatch",
        ));
    }

    Ok(())
}
