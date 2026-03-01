mod error;
mod grouped;
mod scalar;
mod wire;

pub(crate) use error::TokenWireError;
pub(in crate::db) use grouped::GroupedContinuationToken;
pub(crate) use scalar::ContinuationToken;
pub(in crate::db::cursor::token) use wire::{
    ContinuationTokenWire, CursorTokenVersion, GroupedContinuationTokenWire,
    GroupedCursorTokenVersion, IndexRangeCursorAnchorWire, MAX_CONTINUATION_TOKEN_BYTES,
    MAX_GROUPED_CONTINUATION_TOKEN_BYTES,
};

///
/// IndexRangeCursorAnchor
/// Dedicated continuation anchor for `AccessPath::IndexRange`.
///
/// This tracks the exact raw index key of the last emitted row so continuation
/// can resume from `Bound::Excluded(last_raw_key)` in store traversal space.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexRangeCursorAnchor {
    last_raw_key: Vec<u8>,
}

impl IndexRangeCursorAnchor {
    pub(in crate::db) const fn new(last_raw_key: Vec<u8>) -> Self {
        Self { last_raw_key }
    }

    pub(in crate::db) const fn last_raw_key(&self) -> &[u8] {
        self.last_raw_key.as_slice()
    }
}
