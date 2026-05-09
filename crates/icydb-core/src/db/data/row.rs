//! Module: data::row
//! Responsibility: bounded raw row bytes and decode boundary helpers.
//! Does not own: row-key encoding, commit-window ordering, or index updates.
//! Boundary: data::store persists RawRow values produced by higher layers.

#[cfg(test)]
use crate::{
    db::data::{
        PersistedRow, SerializedStructuralPatch, StructuralSlotReader,
        persisted_row::{
            canonical_row_from_complete_serialized_structural_patch_for_generated_model_for_test,
            canonical_row_from_entity_for_generated_model_for_test,
        },
    },
    model::entity::EntityModel,
};
use crate::{
    db::{codec::MAX_ROW_BYTES, data::DataKey},
    error::InternalError,
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use std::{borrow::Cow, ops::Deref};
use thiserror::Error as ThisError;

///
/// DataRow
///

pub(in crate::db) type DataRow = (DataKey, RawRow);

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
    #[cfg(test)]
    pub(in crate::db) fn from_generated_entity_for_test<E>(
        entity: &E,
    ) -> Result<Self, InternalError>
    where
        E: PersistedRow,
    {
        canonical_row_from_entity_for_generated_model_for_test(entity)
    }

    /// Build one canonical row from one complete serialized slot image.
    #[cfg(test)]
    pub(in crate::db) fn from_complete_serialized_structural_patch_for_generated_model_for_test(
        model: &'static EntityModel,
        patch: &SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        canonical_row_from_complete_serialized_structural_patch_for_generated_model_for_test(
            model, patch,
        )
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
pub(in crate::db) enum RawRowError {
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

#[cfg(test)]
#[derive(Debug, ThisError)]
pub(in crate::db) enum RowDecodeError {
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
pub(in crate::db) struct RawRow(Vec<u8>);

impl RawRow {
    /// Validate serialized row size against protocol bounds.
    const fn ensure_size(bytes: &[u8]) -> Result<(), RawRowError> {
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
    pub(in crate::db) fn try_new(bytes: Vec<u8>) -> Result<Self, RawRowError> {
        Self::from_untrusted_bytes(bytes)
    }

    /// Build one raw row from one complete serialized slot image.
    #[cfg(test)]
    pub(in crate::db) fn from_complete_serialized_structural_patch_for_generated_model_for_test(
        model: &'static EntityModel,
        patch: &SerializedStructuralPatch,
    ) -> Result<CanonicalRow, InternalError> {
        CanonicalRow::from_complete_serialized_structural_patch_for_generated_model_for_test(
            model, patch,
        )
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Length in bytes (in-memory; bounded by construction).
    #[must_use]
    pub(in crate::db) const fn len(&self) -> usize {
        self.0.len()
    }

    /// Decode into an entity.
    #[cfg(test)]
    pub(in crate::db) fn try_decode_with_generated_model_for_test<E: PersistedRow>(
        &self,
    ) -> Result<E, RowDecodeError> {
        // Keep deserialize failures structured so callers can classify decode
        // boundary errors without parsing free-form strings.
        let mut slots =
            StructuralSlotReader::from_raw_row_with_generated_model_for_test(self, E::MODEL)
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
