//! Module: db::cursor::range_token
//! Responsibility: module-local ownership and contracts for db::cursor::range_token.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    access::LoweredKey,
    cursor::{IndexRangeCursorAnchor, ValidatedInEnvelopeIndexRangeCursorAnchor},
    index::{IndexKey, RawIndexKey},
};

///
/// LogicalKeyHandle
///
/// Executor-owned logical handle for lowered index keys.
/// Keeps physical key representation behind the storage-port boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct LogicalKeyHandle {
    lowered_key: LoweredKey,
}

impl LogicalKeyHandle {
    #[must_use]
    pub(in crate::db) const fn from_lowered(lowered_key: LoweredKey) -> Self {
        Self { lowered_key }
    }

    #[must_use]
    pub(in crate::db) const fn as_lowered(&self) -> &LoweredKey {
        &self.lowered_key
    }
}

///
/// RangeToken
///
/// Executor-owned continuation token payload for index-range scans.
/// Route/load layers carry this token without touching physical key bytes.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct RangeToken {
    anchor: LogicalKeyHandle,
}

impl RangeToken {
    #[must_use]
    pub(in crate::db) const fn new(anchor: LogicalKeyHandle) -> Self {
        Self { anchor }
    }

    #[must_use]
    pub(in crate::db) const fn anchor(&self) -> &LogicalKeyHandle {
        &self.anchor
    }
}

/// Build a continuation anchor from one canonical index key.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) fn cursor_anchor_from_index_key(index_key: &IndexKey) -> IndexRangeCursorAnchor {
    IndexRangeCursorAnchor::new(index_key.to_raw().as_bytes().to_vec())
}

/// Build a continuation anchor directly from one raw index key.
#[must_use]
pub(in crate::db) fn cursor_anchor_from_raw_index_key(
    index_key: &RawIndexKey,
) -> IndexRangeCursorAnchor {
    IndexRangeCursorAnchor::new(index_key.as_bytes().to_vec())
}

/// Decode one continuation anchor into one executor range token.
#[must_use]
pub(in crate::db) fn range_token_from_validated_cursor_anchor(
    anchor: &ValidatedInEnvelopeIndexRangeCursorAnchor,
) -> RangeToken {
    RangeToken::new(LogicalKeyHandle::from_lowered(anchor.lowered_key().clone()))
}

/// Borrow the lowered anchor key carried by one range token.
#[must_use]
pub(in crate::db) const fn range_token_anchor_key(range_token: &RangeToken) -> &LoweredKey {
    range_token.anchor().as_lowered()
}
