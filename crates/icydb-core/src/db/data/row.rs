//! Module: data::row
//! Responsibility: bounded raw row bytes and decode boundary helpers.
//! Does not own: row-key encoding, commit-window ordering, or index updates.
//! Boundary: data::store persists RawRow values produced by higher layers.

use crate::{
    db::{
        codec::{MAX_ROW_BYTES, serialize_row_payload},
        data::{
            DataKey, PersistedRow, SerializedUpdatePatch, StructuralSlotReader, UpdatePatch,
            apply_serialized_update_patch_to_raw_row, apply_update_patch_to_raw_row,
            persisted_row::{SlotBufferWriter, SlotWriter},
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;
use thiserror::Error as ThisError;

#[cfg(test)]
use crate::db::data::serialize_entity_slots_as_update_patch;

///
/// DataRow
///

pub(crate) type DataRow = (DataKey, RawRow);

///
/// RawRowError
/// Construction / storage-boundary errors.
///

#[derive(Debug, ThisError)]
pub(crate) enum RawRowError {
    #[error("row exceeds max size: {len} bytes (limit {MAX_ROW_BYTES})")]
    TooLarge { len: usize },
}

impl From<RawRowError> for InternalError {
    fn from(err: RawRowError) -> Self {
        Self::store_unsupported(err.to_string())
    }
}

///
/// RowDecodeError
/// Logical / format errors during decode.
///

#[derive(Debug, ThisError)]
pub(crate) enum RowDecodeError {
    #[error("row failed to deserialize: {source}")]
    Deserialize {
        #[source]
        source: InternalError,
    },
}

///
/// RawRow
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRow(Vec<u8>);

impl RawRow {
    /// Validate serialized row size against protocol bounds.
    pub(crate) const fn ensure_size(bytes: &[u8]) -> Result<(), RawRowError> {
        if bytes.len() > MAX_ROW_BYTES as usize {
            return Err(RawRowError::TooLarge { len: bytes.len() });
        }

        Ok(())
    }

    /// Construct a raw row from serialized bytes.
    pub(crate) fn try_new(bytes: Vec<u8>) -> Result<Self, RawRowError> {
        Self::ensure_size(&bytes)?;
        Ok(Self(bytes))
    }

    /// Encode one entity into the canonical persisted row envelope.
    #[cfg(test)]
    pub(crate) fn from_entity<E>(entity: &E) -> Result<Self, InternalError>
    where
        E: PersistedRow,
    {
        let serialized_patch = serialize_entity_slots_as_update_patch(entity)?;
        Self::from_serialized_update_patch(E::MODEL, &serialized_patch)
    }

    /// Build one raw row from one serialized structural patch that already
    /// describes a full canonical row image.
    pub(in crate::db) fn from_serialized_update_patch(
        model: &'static EntityModel,
        patch: &SerializedUpdatePatch,
    ) -> Result<Self, InternalError> {
        let mut payloads = vec![None; model.fields().len()];

        // Phase 1: project the serialized patch onto the full model slot set
        // with last-write-wins semantics.
        for entry in patch.entries() {
            let slot = entry.slot().index();
            let target = payloads.get_mut(slot).ok_or_else(|| {
                InternalError::persisted_row_encode_failed(format!(
                    "serialized patch slot {slot} is outside the row layout for entity '{}'",
                    model.path()
                ))
            })?;
            *target = Some(entry.payload());
        }

        // Phase 2: require a dense row image so new-row construction never
        // depends on absent placeholder slots.
        let mut writer = SlotBufferWriter::for_model(model);
        for (slot, payload) in payloads.into_iter().enumerate() {
            let Some(payload) = payload else {
                return Err(InternalError::persisted_row_encode_failed(format!(
                    "serialized patch did not emit slot {slot} for entity '{}'",
                    model.path()
                )));
            };
            writer.write_slot(slot, Some(payload))?;
        }

        // Phase 3: wrap the dense slot payloads into the canonical row
        // envelope directly.
        let encoded = serialize_row_payload(writer.finish()?)?;

        Self::try_new(encoded).map_err(InternalError::from)
    }

    /// Apply one ordered structural patch through the persisted-row boundary.
    #[allow(dead_code)]
    pub(in crate::db) fn apply_update_patch(
        &self,
        model: &'static EntityModel,
        patch: &UpdatePatch,
    ) -> Result<Self, InternalError> {
        apply_update_patch_to_raw_row(model, self, patch)
    }

    /// Apply one pre-serialized structural patch through the persisted-row boundary.
    #[allow(dead_code)]
    pub(in crate::db) fn apply_serialized_update_patch(
        &self,
        model: &'static EntityModel,
        patch: &SerializedUpdatePatch,
    ) -> Result<Self, InternalError> {
        apply_serialized_update_patch_to_raw_row(model, self, patch)
    }

    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Length in bytes (in-memory; bounded by construction).
    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Decode into an entity.
    pub(crate) fn try_decode<E: PersistedRow>(&self) -> Result<E, RowDecodeError> {
        // Keep deserialize failures structured so callers can classify decode
        // boundary errors without parsing free-form strings.
        let mut slots = StructuralSlotReader::from_raw_row(self, E::MODEL)
            .map_err(|source| RowDecodeError::Deserialize { source })?;
        E::materialize_from_slots(&mut slots)
            .map_err(|source| RowDecodeError::Deserialize { source })
    }
}

impl Storable for RawRow {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        // Trusted store boundary: bounded by BOUND
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_ROW_BYTES,
        is_fixed_size: false,
    };
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorClass, ErrorOrigin};

    #[test]
    fn raw_row_rejects_oversized_payload() {
        let bytes = vec![0u8; MAX_ROW_BYTES as usize + 1];
        let err = RawRow::try_new(bytes).unwrap_err();
        assert!(matches!(err, RawRowError::TooLarge { .. }));
    }

    #[test]
    fn raw_row_error_maps_to_store_unsupported() {
        let err: InternalError = RawRowError::TooLarge {
            len: MAX_ROW_BYTES as usize + 1,
        }
        .into();
        assert_eq!(err.class, ErrorClass::Unsupported);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }
}
