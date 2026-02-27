use crate::{
    db::{cursor::IndexRangeCursorAnchor, executor::LoweredKey, index::IndexKey},
    traits::Storable,
};
use std::borrow::Cow;

///
/// LogicalKeyHandle
///
/// Executor-owned logical handle for lowered index keys.
/// Keeps physical key representation behind the storage-port boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct LogicalKeyHandle {
    lowered_key: LoweredKey,
}

impl LogicalKeyHandle {
    #[must_use]
    pub(in crate::db::executor) const fn from_lowered(lowered_key: LoweredKey) -> Self {
        Self { lowered_key }
    }

    #[must_use]
    pub(in crate::db::executor) const fn as_lowered(&self) -> &LoweredKey {
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
pub(in crate::db::executor) struct RangeToken {
    anchor: LogicalKeyHandle,
}

impl RangeToken {
    #[must_use]
    pub(in crate::db::executor) const fn new(anchor: LogicalKeyHandle) -> Self {
        Self { anchor }
    }

    #[must_use]
    pub(in crate::db::executor) const fn anchor(&self) -> &LogicalKeyHandle {
        &self.anchor
    }
}

/// Build a continuation anchor from one canonical index key.
#[must_use]
pub(in crate::db::executor) fn cursor_anchor_from_index_key(
    index_key: &IndexKey,
) -> IndexRangeCursorAnchor {
    IndexRangeCursorAnchor::new(index_key.to_raw().as_bytes().to_vec())
}

/// Decode one continuation anchor into one executor range token.
#[must_use]
pub(in crate::db::executor) fn range_token_from_cursor_anchor(
    anchor: &IndexRangeCursorAnchor,
) -> RangeToken {
    let lowered_key = <LoweredKey as Storable>::from_bytes(Cow::Borrowed(anchor.last_raw_key()));
    RangeToken::new(LogicalKeyHandle::from_lowered(lowered_key))
}

/// Build one range token from one lowered anchor key.
#[must_use]
pub(in crate::db::executor) fn range_token_from_lowered_anchor(anchor: &LoweredKey) -> RangeToken {
    RangeToken::new(LogicalKeyHandle::from_lowered(anchor.clone()))
}

/// Borrow the lowered anchor key carried by one range token.
#[must_use]
pub(in crate::db::executor) const fn range_token_anchor_key(
    range_token: &RangeToken,
) -> &LoweredKey {
    range_token.anchor().as_lowered()
}
