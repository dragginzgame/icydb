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
/// CursorTokenVersion
///
/// Wire-level cursor token version owned by the cursor protocol boundary.
/// This keeps version parsing and compatibility behavior centralized.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::cursor::token) enum CursorTokenVersion {
    V1,
    V2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::cursor::token) enum GroupedCursorTokenVersion {
    V1,
}

impl GroupedCursorTokenVersion {
    const V1_TAG: u8 = 1;

    pub(in crate::db::cursor::token) const fn decode(raw: u8) -> Option<Self> {
        match raw {
            Self::V1_TAG => Some(Self::V1),
            _ => None,
        }
    }

    pub(in crate::db::cursor::token) const fn encode(self) -> u8 {
        match self {
            Self::V1 => Self::V1_TAG,
        }
    }

    pub(in crate::db::cursor::token) const fn decode_initial_offset(
        self,
        wire_initial_offset: u32,
    ) -> u32 {
        match self {
            Self::V1 => wire_initial_offset,
        }
    }
}

impl CursorTokenVersion {
    const V1_TAG: u8 = 1;
    const V2_TAG: u8 = 2;

    // Decode one raw wire version into the protocol enum.
    pub(in crate::db::cursor::token) const fn decode(raw: u8) -> Option<Self> {
        match raw {
            Self::V1_TAG => Some(Self::V1),
            Self::V2_TAG => Some(Self::V2),
            _ => None,
        }
    }

    // Encode this protocol version for wire format output.
    pub(in crate::db::cursor::token) const fn encode(self) -> u8 {
        match self {
            Self::V1 => Self::V1_TAG,
            Self::V2 => Self::V2_TAG,
        }
    }

    // Apply version compatibility behavior for initial offset.
    // V1 tokens did not carry offset and must decode as zero.
    pub(in crate::db::cursor::token) const fn decode_initial_offset(
        self,
        wire_initial_offset: u32,
    ) -> u32 {
        match self {
            Self::V1 => 0,
            Self::V2 => wire_initial_offset,
        }
    }
}

///
/// ContinuationTokenWire
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::db::cursor::token) struct ContinuationTokenWire {
    pub(in crate::db::cursor::token) version: u8,
    pub(in crate::db::cursor::token) signature: [u8; 32],
    pub(in crate::db::cursor::token) boundary: CursorBoundary,
    #[serde(default)]
    pub(in crate::db::cursor::token) direction: Direction,
    #[serde(default)]
    pub(in crate::db::cursor::token) initial_offset: u32,
    #[serde(default)]
    pub(in crate::db::cursor::token) index_range_anchor: Option<IndexRangeCursorAnchorWire>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::db::cursor::token) struct GroupedContinuationTokenWire {
    pub(in crate::db::cursor::token) version: u8,
    pub(in crate::db::cursor::token) signature: [u8; 32],
    pub(in crate::db::cursor::token) last_group_key: Vec<Value>,
    #[serde(default)]
    pub(in crate::db::cursor::token) direction: Direction,
    #[serde(default)]
    pub(in crate::db::cursor::token) initial_offset: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::db::cursor::token) struct IndexRangeCursorAnchorWire {
    last_raw_key: Vec<u8>,
}

impl From<&IndexRangeCursorAnchor> for IndexRangeCursorAnchorWire {
    fn from(anchor: &IndexRangeCursorAnchor) -> Self {
        Self {
            last_raw_key: anchor.last_raw_key().to_vec(),
        }
    }
}

impl IndexRangeCursorAnchorWire {
    pub(in crate::db::cursor::token) fn into_anchor(self) -> IndexRangeCursorAnchor {
        IndexRangeCursorAnchor::new(self.last_raw_key)
    }
}
