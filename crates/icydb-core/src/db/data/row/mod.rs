//! Module: data::row
//! Responsibility: bounded raw row bytes and decode boundary helpers.
//! Does not own: row-key encoding, commit-window ordering, or index updates.
//! Boundary: data::store persists RawRow values produced by higher layers.

use crate::db::data::DecodedDataStoreKey;
use crate::{db::codec::MAX_ROW_BYTES, error::InternalError};
use ic_storage::adapter::{Storable, storable::Bound};
use std::borrow::Cow;

///
/// DataRow
///

pub(in crate::db) type DataRow = (DecodedDataStoreKey, RawRow);

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
}

///
/// RawRow
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct RawRow(Vec<u8>);

impl RawRow {
    /// Validate serialized row size against protocol bounds.
    pub(in crate::db) fn ensure_size(bytes: &[u8]) -> Result<(), InternalError> {
        if bytes.len() > MAX_ROW_BYTES as usize {
            return Err(InternalError::store_unsupported());
        }

        Ok(())
    }

    /// Construct one bounded raw row for internal decode/read boundaries.
    pub(in crate::db) fn from_untrusted_bytes(bytes: Vec<u8>) -> Result<Self, InternalError> {
        Self::ensure_size(&bytes)?;
        Ok(Self(bytes))
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
}

impl Storable for RawRow {
    fn to_bytes(&self) -> Result<Cow<'_, [u8]>, ic_storage::storable::CodecError> {
        Ok(Cow::Borrowed(&self.0))
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Result<Self, ic_storage::storable::CodecError> {
        Self::check_bounds(bytes.as_ref())?;
        // Trusted store boundary: row-size admission happens before persistence.
        Ok(Self(bytes.into_owned()))
    }

    fn into_bytes(self) -> Result<Vec<u8>, ic_storage::storable::CodecError> {
        Ok(self.0)
    }

    const BOUND: Bound = Bound::Unbounded;
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;
