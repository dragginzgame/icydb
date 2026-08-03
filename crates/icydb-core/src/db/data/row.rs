//! Module: data::row
//! Responsibility: bounded raw row bytes and decode boundary helpers.
//! Does not own: row-key encoding, commit-window ordering, or index updates.
//! Boundary: data::store persists RawRow values produced by higher layers.

use crate::db::data::DecodedDataStoreKey;
use crate::{db::codec::MAX_ROW_BYTES, error::InternalError};
use ic_stable_structures::{Storable, storable::Bound};
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
/// RawRowError
/// Construction / storage-boundary errors.
///

#[derive(Debug)]
pub(in crate::db) enum RawRowError {
    TooLarge { len: usize },
}

impl From<RawRowError> for InternalError {
    fn from(err: RawRowError) -> Self {
        match err {
            RawRowError::TooLarge { len } => {
                let _ = len;
                Self::store_unsupported()
            }
        }
    }
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

    const BOUND: Bound = Bound::Unbounded;
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
        std::assert_matches!(err, RawRowError::TooLarge { .. });
    }

    #[test]
    fn raw_row_storable_bound_does_not_amplify_stable_btree_nodes() {
        assert_eq!(RawRow::BOUND, Bound::Unbounded);
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
