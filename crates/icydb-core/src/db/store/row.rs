use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    serialize::deserialize,
    traits::{EntityKind, Storable},
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;
use thiserror::Error as ThisError;

///
/// RawRowError
///

#[derive(Debug, ThisError)]
pub enum RawRowError {
    #[error("row exceeds max size: {len} bytes (limit {MAX_ROW_BYTES})")]
    TooLarge { len: usize },
}

impl RawRowError {
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        ErrorClass::Unsupported
    }

    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        ErrorOrigin::Store
    }
}

impl From<RawRowError> for InternalError {
    fn from(err: RawRowError) -> Self {
        Self::new(err.class(), err.origin(), err.to_string())
    }
}

///
/// RawDecodeError
///

#[derive(Debug, ThisError)]
pub enum RowDecodeError {
    #[error("row exceeds max size: {len} bytes (limit {MAX_ROW_BYTES})")]
    TooLarge { len: usize },
    #[error("row failed to deserialize")]
    Deserialize,
}

///
/// RawRow
///

/// Max serialized bytes for a single row to keep value loads bounded.
pub const MAX_ROW_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRow(Vec<u8>);

impl RawRow {
    pub fn try_new(bytes: Vec<u8>) -> Result<Self, RawRowError> {
        if bytes.len() > MAX_ROW_BYTES as usize {
            return Err(RawRowError::TooLarge { len: bytes.len() });
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn try_decode<E: EntityKind>(&self) -> Result<E, RowDecodeError> {
        if self.0.len() > MAX_ROW_BYTES as usize {
            return Err(RowDecodeError::TooLarge { len: self.0.len() });
        }

        deserialize::<E>(&self.0).map_err(|_| RowDecodeError::Deserialize)
    }
}

impl Storable for RawRow {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
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

    #[test]
    fn raw_row_rejects_oversized_payload() {
        let bytes = vec![0u8; MAX_ROW_BYTES as usize + 1];
        let err = RawRow::try_new(bytes).unwrap_err();
        assert!(matches!(err, RawRowError::TooLarge { .. }));
    }
}
