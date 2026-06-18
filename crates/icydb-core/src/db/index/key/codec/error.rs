//! Module: index::key::codec::error
//! Responsibility: compact corruption taxonomy for raw index-key decode.
//! Does not own: error class mapping.
//! Boundary: referenced by codec decode helpers and tests.

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
