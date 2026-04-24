//! Module: executor::planning::continuation::range_token
//! Responsibility: executor-owned index-range scan continuation handles.
//! Does not own: cursor wire encoding or cursor anchor validation.
//! Boundary: consumes cursor-validated anchors and exposes lowered scan keys.

use crate::db::{access::LoweredKey, cursor::ValidatedInEnvelopeIndexRangeCursorAnchor};

///
/// IndexRangeScanAnchor
///
/// Executor-owned logical handle for a lowered index-range scan anchor.
/// This keeps scalar continuation planning from carrying cursor token or
/// validation types deeper into scan execution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct IndexRangeScanAnchor {
    lowered_key: LoweredKey,
}

impl IndexRangeScanAnchor {
    #[must_use]
    const fn from_lowered(lowered_key: LoweredKey) -> Self {
        Self { lowered_key }
    }

    #[must_use]
    const fn as_lowered(&self) -> &LoweredKey {
        &self.lowered_key
    }
}

///
/// IndexRangeScanToken
///
/// Executor-owned continuation token for index-range scans.
/// Cursor validation proves the anchor; this type carries only the lowered
/// scan key needed by executor traversal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IndexRangeScanToken {
    anchor: IndexRangeScanAnchor,
}

impl IndexRangeScanToken {
    /// Build one executor scan token from one cursor-validated anchor.
    #[must_use]
    pub(super) fn from_anchor(anchor: &ValidatedInEnvelopeIndexRangeCursorAnchor) -> Self {
        Self {
            anchor: IndexRangeScanAnchor::from_lowered(anchor.clone_raw_key()),
        }
    }

    /// Borrow the lowered anchor key used to resume index-range traversal.
    #[must_use]
    pub(super) const fn anchor_key(&self) -> &LoweredKey {
        self.anchor.as_lowered()
    }
}
