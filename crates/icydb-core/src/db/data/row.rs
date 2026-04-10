//! Module: data::row
//! Responsibility: bounded raw row bytes and decode boundary helpers.
//! Does not own: row-key encoding, commit-window ordering, or index updates.
//! Boundary: data::store persists RawRow values produced by higher layers.

use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        data::{
            DataKey, PersistedRow, SerializedUpdatePatch, StructuralSlotReader,
            apply_serialized_update_patch_to_raw_row, canonical_row_from_entity,
            persisted_row::canonical_row_from_serialized_update_patch,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use std::{borrow::Cow, ops::Deref};
use thiserror::Error as ThisError;

///
/// DataRow
///

pub(crate) type DataRow = (DataKey, RawRow);

///
/// CanonicalRow
///
/// Write-capability wrapper for canonical persisted row bytes.
/// Values of this type may cross storage write boundaries.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CanonicalRow(RawRow);

impl CanonicalRow {
    /// Wrap one raw row that is already known to satisfy canonical write invariants.
    pub(in crate::db::data) const fn from_canonical_raw_row(raw_row: RawRow) -> Self {
        Self(raw_row)
    }

    /// Consume the write-capability wrapper back into the underlying raw row.
    pub(in crate::db) fn into_raw_row(self) -> RawRow {
        self.0
    }

    /// Borrow the underlying raw row for read-side decoding helpers.
    #[must_use]
    pub(in crate::db) const fn as_raw_row(&self) -> &RawRow {
        &self.0
    }

    /// Encode one full typed entity into canonical persisted row bytes.
    pub(in crate::db) fn from_entity<E>(entity: &E) -> Result<Self, InternalError>
    where
        E: PersistedRow,
    {
        canonical_row_from_entity(entity)
    }

    /// Build one canonical row from one serialized structural patch that
    /// already describes a full canonical row image.
    pub(in crate::db) fn from_serialized_update_patch(
        model: &'static EntityModel,
        patch: &SerializedUpdatePatch,
    ) -> Result<Self, InternalError> {
        canonical_row_from_serialized_update_patch(model, patch)
    }
}

impl Deref for CanonicalRow {
    type Target = RawRow;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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

    /// Construct one bounded raw row for internal decode/read boundaries.
    pub(in crate::db) fn from_untrusted_bytes(bytes: Vec<u8>) -> Result<Self, RawRowError> {
        Self::ensure_size(&bytes)?;
        Ok(Self(bytes))
    }

    /// Construct a raw row from serialized bytes.
    #[cfg(test)]
    pub(crate) fn try_new(bytes: Vec<u8>) -> Result<Self, RawRowError> {
        Self::from_untrusted_bytes(bytes)
    }

    /// Encode one entity into the canonical persisted row envelope.
    #[cfg(test)]
    pub(crate) fn from_entity<E>(entity: &E) -> Result<Self, InternalError>
    where
        E: PersistedRow,
    {
        CanonicalRow::from_entity(entity).map(CanonicalRow::into_raw_row)
    }

    /// Build one raw row from one serialized structural patch that already
    /// describes a full canonical row image.
    pub(in crate::db) fn from_serialized_update_patch(
        model: &'static EntityModel,
        patch: &SerializedUpdatePatch,
    ) -> Result<CanonicalRow, InternalError> {
        CanonicalRow::from_serialized_update_patch(model, patch)
    }

    /// Apply one pre-serialized structural patch through the persisted-row boundary.
    pub(in crate::db) fn apply_serialized_update_patch(
        &self,
        model: &'static EntityModel,
        patch: &SerializedUpdatePatch,
    ) -> Result<CanonicalRow, InternalError> {
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
