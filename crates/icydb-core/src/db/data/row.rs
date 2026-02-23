use super::DataKey;
use crate::{
    db::codec::deserialize_row,
    error::InternalError,
    traits::{EntityKind, Storable},
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;
use thiserror::Error as ThisError;

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

/// Max serialized bytes for a single row (protocol-level limit).
pub(crate) const MAX_ROW_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRow(Vec<u8>);

impl RawRow {
    /// Construct a raw row from serialized bytes.
    pub(crate) fn try_new(bytes: Vec<u8>) -> Result<Self, RawRowError> {
        if bytes.len() > MAX_ROW_BYTES as usize {
            return Err(RawRowError::TooLarge { len: bytes.len() });
        }
        Ok(Self(bytes))
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
    pub(crate) fn try_decode<E: EntityKind>(&self) -> Result<E, RowDecodeError> {
        deserialize_row::<E>(&self.0).map_err(|source| RowDecodeError::Deserialize { source })
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
