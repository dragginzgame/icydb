//! Module: cursor::token::wire
//! Responsibility: wire-format token payload structures for the current cursor protocol.
//! Does not own: cursor planning policy or runtime traversal semantics.
//! Boundary: defines the serializable cursor token schema for scalar/grouped
//! continuations.

use crate::{
    db::{
        cursor::{CursorBoundary, token::IndexRangeCursorAnchor},
        direction::Direction,
    },
    value::Value,
};
use serde::{Deserialize, Serialize};

pub(in crate::db::cursor::token) const MAX_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;
pub(in crate::db::cursor::token) const MAX_GROUPED_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;

///
/// ContinuationTokenWire
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::db::cursor::token) struct ContinuationTokenWire {
    pub(in crate::db::cursor::token) signature: [u8; 32],
    pub(in crate::db::cursor::token) boundary: CursorBoundary,
    #[serde(default)]
    pub(in crate::db::cursor::token) direction: Direction,
    #[serde(default)]
    pub(in crate::db::cursor::token) initial_offset: u32,
    #[serde(default)]
    pub(in crate::db::cursor::token) index_range_anchor: Option<IndexRangeCursorAnchorWire>,
}

///
/// ContinuationTokenWireRef
///
/// Borrowed scalar cursor token wire view used only for encode-time
/// serialization so hot cursor-emission paths do not clone boundary payloads.
///

#[derive(Serialize)]
pub(in crate::db::cursor::token) struct ContinuationTokenWireRef<'a> {
    pub(in crate::db::cursor::token) signature: [u8; 32],
    pub(in crate::db::cursor::token) boundary: &'a CursorBoundary,
    #[serde(default)]
    pub(in crate::db::cursor::token) direction: Direction,
    #[serde(default)]
    pub(in crate::db::cursor::token) initial_offset: u32,
    #[serde(default)]
    pub(in crate::db::cursor::token) index_range_anchor: Option<IndexRangeCursorAnchorWireRef<'a>>,
}

///
/// GroupedContinuationTokenWire
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::db::cursor::token) struct GroupedContinuationTokenWire {
    pub(in crate::db::cursor::token) signature: [u8; 32],
    pub(in crate::db::cursor::token) last_group_key: Vec<Value>,
    #[serde(default)]
    pub(in crate::db::cursor::token) direction: Direction,
    #[serde(default)]
    pub(in crate::db::cursor::token) initial_offset: u32,
}

///
/// GroupedContinuationTokenWireRef
///
/// Borrowed grouped cursor token wire view used only for encode-time
/// serialization so grouped continuation emission avoids cloning group keys.
///

#[derive(Serialize)]
pub(in crate::db::cursor::token) struct GroupedContinuationTokenWireRef<'a> {
    pub(in crate::db::cursor::token) signature: [u8; 32],
    pub(in crate::db::cursor::token) last_group_key: &'a [Value],
    #[serde(default)]
    pub(in crate::db::cursor::token) direction: Direction,
    #[serde(default)]
    pub(in crate::db::cursor::token) initial_offset: u32,
}

///
/// IndexRangeCursorAnchorWire
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::db::cursor::token) struct IndexRangeCursorAnchorWire {
    last_raw_key: Vec<u8>,
}

///
/// IndexRangeCursorAnchorWireRef
///
/// Borrowed encode-only anchor payload for scalar continuation token emission.
///

#[derive(Serialize)]
pub(in crate::db::cursor::token) struct IndexRangeCursorAnchorWireRef<'a> {
    last_raw_key: &'a [u8],
}

impl From<&IndexRangeCursorAnchor> for IndexRangeCursorAnchorWire {
    fn from(anchor: &IndexRangeCursorAnchor) -> Self {
        Self {
            last_raw_key: anchor.last_raw_key().to_vec(),
        }
    }
}

impl<'a> From<&'a IndexRangeCursorAnchor> for IndexRangeCursorAnchorWireRef<'a> {
    fn from(anchor: &'a IndexRangeCursorAnchor) -> Self {
        Self {
            last_raw_key: anchor.last_raw_key(),
        }
    }
}

impl IndexRangeCursorAnchorWire {
    pub(in crate::db::cursor::token) fn into_anchor(self) -> IndexRangeCursorAnchor {
        IndexRangeCursorAnchor::new(self.last_raw_key)
    }
}
