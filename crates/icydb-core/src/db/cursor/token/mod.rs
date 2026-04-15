//! Module: cursor::token
//! Responsibility: typed continuation token wire contracts for scalar/grouped cursor paths.
//! Does not own: higher-level cursor validation, ordering policy, or
//! resume-bound semantics.
//! Boundary: defines the current token payloads consumed by cursor
//! encode/decode boundaries.

mod codec;
mod error;
mod grouped;
mod scalar;

pub(in crate::db::cursor::token) use codec::{
    decode_grouped_token, decode_scalar_token, encode_grouped_token, encode_scalar_token,
};
pub(crate) use error::TokenWireError;
pub(in crate::db) use grouped::GroupedContinuationToken;
pub(crate) use scalar::ContinuationToken;

///
/// IndexRangeCursorAnchor
/// Dedicated continuation anchor for index-range access paths.
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
