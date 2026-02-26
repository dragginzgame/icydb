use crate::{
    db::{
        cursor::{CursorPlanError, IndexRangeCursorAnchor},
        executor::ExecutorPlanError,
        index::{IndexKey, RawIndexKey},
        lowering::LoweredKey,
    },
    traits::Storable,
};
use std::borrow::Cow;

// Build the canonical invalid-continuation payload error variant.
fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> ExecutorPlanError {
    ExecutorPlanError::from(CursorPlanError::InvalidContinuationCursorPayload {
        reason: reason.into(),
    })
}

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
/// CursorAnchor
///
/// Executor-owned wrapper around continuation anchor payload bytes.
/// Storage adapters decode this into canonical index keys.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct CursorAnchor<'a> {
    anchor: &'a IndexRangeCursorAnchor,
}

impl<'a> CursorAnchor<'a> {
    #[must_use]
    pub(in crate::db::executor) const fn new(anchor: &'a IndexRangeCursorAnchor) -> Self {
        Self { anchor }
    }

    #[must_use]
    const fn raw_bytes(self) -> &'a [u8] {
        self.anchor.last_raw_key()
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

/// Decode one continuation anchor into a canonical index key and enforce
/// canonical round-trip encoding.
pub(in crate::db::executor) fn decode_canonical_cursor_anchor_index_key(
    anchor: CursorAnchor<'_>,
) -> Result<IndexKey, ExecutorPlanError> {
    let anchor_raw = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(anchor.raw_bytes()));
    let decoded_key = IndexKey::try_from_raw(&anchor_raw).map_err(|err| {
        invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor decode failed: {err}"
        ))
    })?;
    let canonical_raw = decoded_key.to_raw();
    debug_assert_eq!(
        canonical_raw.as_bytes(),
        anchor.raw_bytes(),
        "index-range continuation anchor must round-trip to identical raw bytes",
    );
    if canonical_raw.as_bytes() != anchor.raw_bytes() {
        return Err(invalid_continuation_cursor_payload(
            "index-range continuation anchor canonical encoding mismatch",
        ));
    }

    Ok(decoded_key)
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
