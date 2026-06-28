//! Module: index::key::codec::error
//! Responsibility: compact corruption taxonomy for raw index-key encode/decode.
//! Does not own: error class mapping.
//! Boundary: referenced by codec helpers, callers, and tests.

use crate::{
    db::key_taxonomy::{CompactPrimaryKeyEncodeError, CompactStoreKeyEncodeError},
    error::InternalError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexKeyEncodeError {
    EmptySegment,
    InvalidPrimaryKey,
    SegmentTooLarge,
    TooManyComponents,
}

impl From<CompactPrimaryKeyEncodeError> for IndexKeyEncodeError {
    fn from(_err: CompactPrimaryKeyEncodeError) -> Self {
        Self::InvalidPrimaryKey
    }
}

impl From<CompactStoreKeyEncodeError> for IndexKeyEncodeError {
    fn from(err: CompactStoreKeyEncodeError) -> Self {
        match err {
            CompactStoreKeyEncodeError::TooManyIndexComponents => Self::TooManyComponents,
            CompactStoreKeyEncodeError::IndexSegmentTooLarge => Self::SegmentTooLarge,
        }
    }
}

impl From<IndexKeyEncodeError> for InternalError {
    fn from(_err: IndexKeyEncodeError) -> Self {
        Self::index_invariant()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexKeyDecodeError {
    InvalidKeyKind,
    InvalidSize,
    InvalidIndexIdBytes,
    InvalidIndexLength,
    InvalidPrimaryKey,
    TrailingBytes,
    TruncatedKey,
    ZeroLengthSegment,
    OverlongSegment,
    SegmentOverflow,
}
