use crate::{
    db::{codec::deserialize_protocol_payload, cursor::CursorBoundary, direction::Direction},
    serialize::serialize,
    value::Value,
};
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

const MAX_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;
const MAX_GROUPED_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;

///
/// ContinuationSignature
///
/// Stable, deterministic hash of continuation-relevant plan semantics.
/// Excludes windowing state (`limit`, `offset`) and cursor boundaries.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContinuationSignature([u8; 32]);

impl ContinuationSignature {
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn as_hex(&self) -> String {
        crate::db::codec::cursor::encode_cursor(&self.0)
    }
}

impl std::fmt::Display for ContinuationSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

///
/// ContinuationToken
/// Opaque cursor payload bound to a continuation signature.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ContinuationToken {
    signature: ContinuationSignature,
    boundary: CursorBoundary,
    direction: Direction,
    initial_offset: u32,
    index_range_anchor: Option<IndexRangeCursorAnchor>,
}

impl ContinuationToken {
    pub(in crate::db) const fn new_with_direction(
        signature: ContinuationSignature,
        boundary: CursorBoundary,
        direction: Direction,
        initial_offset: u32,
    ) -> Self {
        Self {
            signature,
            boundary,
            direction,
            initial_offset,
            index_range_anchor: None,
        }
    }

    pub(in crate::db) const fn new_index_range_with_direction(
        signature: ContinuationSignature,
        boundary: CursorBoundary,
        index_range_anchor: IndexRangeCursorAnchor,
        direction: Direction,
        initial_offset: u32,
    ) -> Self {
        Self {
            signature,
            boundary,
            direction,
            initial_offset,
            index_range_anchor: Some(index_range_anchor),
        }
    }

    pub(crate) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    pub(crate) const fn boundary(&self) -> &CursorBoundary {
        &self.boundary
    }

    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }

    pub(in crate::db) const fn index_range_anchor(&self) -> Option<&IndexRangeCursorAnchor> {
        self.index_range_anchor.as_ref()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, ContinuationTokenError> {
        let index_range_anchor = self
            .index_range_anchor()
            .map(IndexRangeCursorAnchorWire::from);
        let wire = ContinuationTokenWire {
            version: CursorTokenVersion::V2.encode(),
            signature: self.signature.into_bytes(),
            boundary: self.boundary.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
            index_range_anchor,
        };

        serialize(&wire).map_err(|err| ContinuationTokenError::Encode(err.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ContinuationTokenError> {
        let wire: ContinuationTokenWire =
            deserialize_protocol_payload(bytes, MAX_CONTINUATION_TOKEN_BYTES)
                .map_err(|err| ContinuationTokenError::Decode(err.to_string()))?;

        // Decode the protocol version first so compatibility behavior remains centralized.
        let version = CursorTokenVersion::decode(wire.version)?;
        let signature = ContinuationSignature::from_bytes(wire.signature);
        let boundary = wire.boundary;
        let direction = wire.direction;
        let initial_offset = version.decode_initial_offset(wire.initial_offset);

        match wire
            .index_range_anchor
            .map(IndexRangeCursorAnchorWire::into_anchor)
        {
            Some(anchor) => Ok(Self::new_index_range_with_direction(
                signature,
                boundary,
                anchor,
                direction,
                initial_offset,
            )),
            None => Ok(Self::new_with_direction(
                signature,
                boundary,
                direction,
                initial_offset,
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn encode_with_version_for_test(
        &self,
        version: u8,
    ) -> Result<Vec<u8>, ContinuationTokenError> {
        let index_range_anchor = self
            .index_range_anchor()
            .map(IndexRangeCursorAnchorWire::from);
        let wire = ContinuationTokenWire {
            version,
            signature: self.signature.into_bytes(),
            boundary: self.boundary.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
            index_range_anchor,
        };

        serialize(&wire).map_err(|err| ContinuationTokenError::Encode(err.to_string()))
    }
}

///
/// ContinuationTokenError
/// Cursor token encoding/decoding failures.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum ContinuationTokenError {
    #[error("failed to encode continuation token: {0}")]
    Encode(String),

    #[error("failed to decode continuation token: {0}")]
    Decode(String),

    #[error("unsupported continuation token version: {version}")]
    UnsupportedVersion { version: u8 },
}

///
/// GroupedContinuationToken
///
/// Dedicated continuation payload for grouped result pagination.
/// This audit-pass token type is additive and intentionally not wired into
/// existing load execution paths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedContinuationToken {
    signature: ContinuationSignature,
    last_group_key: Vec<Value>,
    direction: Direction,
    initial_offset: u32,
}

impl GroupedContinuationToken {
    pub(in crate::db) const fn new_with_direction(
        signature: ContinuationSignature,
        last_group_key: Vec<Value>,
        direction: Direction,
        initial_offset: u32,
    ) -> Self {
        Self {
            signature,
            last_group_key,
            direction,
            initial_offset,
        }
    }

    pub(in crate::db) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    pub(in crate::db) const fn last_group_key(&self) -> &[Value] {
        self.last_group_key.as_slice()
    }

    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }

    pub(in crate::db) fn encode(&self) -> Result<Vec<u8>, GroupedContinuationTokenError> {
        let wire = GroupedContinuationTokenWire {
            version: GroupedCursorTokenVersion::V1.encode(),
            signature: self.signature.into_bytes(),
            last_group_key: self.last_group_key.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
        };

        serialize(&wire).map_err(|err| GroupedContinuationTokenError::Encode(err.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn encode_with_version_for_test(
        &self,
        version: u8,
    ) -> Result<Vec<u8>, GroupedContinuationTokenError> {
        let wire = GroupedContinuationTokenWire {
            version,
            signature: self.signature.into_bytes(),
            last_group_key: self.last_group_key.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
        };

        serialize(&wire).map_err(|err| GroupedContinuationTokenError::Encode(err.to_string()))
    }

    pub(in crate::db) fn decode(bytes: &[u8]) -> Result<Self, GroupedContinuationTokenError> {
        let wire: GroupedContinuationTokenWire =
            deserialize_protocol_payload(bytes, MAX_GROUPED_CONTINUATION_TOKEN_BYTES)
                .map_err(|err| GroupedContinuationTokenError::Decode(err.to_string()))?;
        let version = GroupedCursorTokenVersion::decode(wire.version)?;

        Ok(Self::new_with_direction(
            ContinuationSignature::from_bytes(wire.signature),
            wire.last_group_key,
            wire.direction,
            version.decode_initial_offset(wire.initial_offset),
        ))
    }
}

///
/// GroupedContinuationTokenError
/// Grouped continuation token encode/decode failures.
///
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum GroupedContinuationTokenError {
    #[error("failed to encode grouped continuation token: {0}")]
    Encode(String),

    #[error("failed to decode grouped continuation token: {0}")]
    Decode(String),

    #[error("unsupported grouped continuation token version: {version}")]
    UnsupportedVersion { version: u8 },
}

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

///
/// CursorTokenVersion
///
/// Wire-level cursor token version owned by the cursor protocol boundary.
/// This keeps version parsing and compatibility behavior centralized.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorTokenVersion {
    V1,
    V2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedCursorTokenVersion {
    V1,
}

impl GroupedCursorTokenVersion {
    const V1_TAG: u8 = 1;

    const fn decode(raw: u8) -> Result<Self, GroupedContinuationTokenError> {
        match raw {
            Self::V1_TAG => Ok(Self::V1),
            version => Err(GroupedContinuationTokenError::UnsupportedVersion { version }),
        }
    }

    const fn encode(self) -> u8 {
        match self {
            Self::V1 => Self::V1_TAG,
        }
    }

    const fn decode_initial_offset(self, wire_initial_offset: u32) -> u32 {
        match self {
            Self::V1 => wire_initial_offset,
        }
    }
}

impl CursorTokenVersion {
    const V1_TAG: u8 = 1;
    const V2_TAG: u8 = 2;

    // Decode one raw wire version into the protocol enum.
    const fn decode(raw: u8) -> Result<Self, ContinuationTokenError> {
        match raw {
            Self::V1_TAG => Ok(Self::V1),
            Self::V2_TAG => Ok(Self::V2),
            version => Err(ContinuationTokenError::UnsupportedVersion { version }),
        }
    }

    // Encode this protocol version for wire format output.
    const fn encode(self) -> u8 {
        match self {
            Self::V1 => Self::V1_TAG,
            Self::V2 => Self::V2_TAG,
        }
    }

    // Apply version compatibility behavior for initial offset.
    // V1 tokens did not carry offset and must decode as zero.
    const fn decode_initial_offset(self, wire_initial_offset: u32) -> u32 {
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
struct ContinuationTokenWire {
    version: u8,
    signature: [u8; 32],
    boundary: CursorBoundary,
    #[serde(default)]
    direction: Direction,
    #[serde(default)]
    initial_offset: u32,
    #[serde(default)]
    index_range_anchor: Option<IndexRangeCursorAnchorWire>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct GroupedContinuationTokenWire {
    version: u8,
    signature: [u8; 32],
    last_group_key: Vec<Value>,
    #[serde(default)]
    direction: Direction,
    #[serde(default)]
    initial_offset: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IndexRangeCursorAnchorWire {
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
    fn into_anchor(self) -> IndexRangeCursorAnchor {
        IndexRangeCursorAnchor::new(self.last_raw_key)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{ContinuationSignature, GroupedContinuationToken, GroupedContinuationTokenError};
    use crate::{
        db::{codec::cursor::encode_cursor, direction::Direction},
        value::Value,
    };

    fn grouped_token_fixture(direction: Direction) -> GroupedContinuationToken {
        GroupedContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([0x42; 32]),
            vec![
                Value::Text("tenant-a".to_string()),
                Value::Uint(7),
                Value::Bool(true),
            ],
            direction,
            4,
        )
    }

    #[test]
    fn grouped_continuation_token_round_trip_preserves_fields() {
        let token = grouped_token_fixture(Direction::Asc);

        let encoded = token
            .encode()
            .expect("grouped continuation token should encode");
        let decoded = GroupedContinuationToken::decode(encoded.as_slice())
            .expect("grouped continuation token should decode");

        assert_eq!(decoded.signature(), token.signature());
        assert_eq!(decoded.last_group_key(), token.last_group_key());
        assert_eq!(decoded.direction(), token.direction());
        assert_eq!(decoded.initial_offset(), token.initial_offset());
    }

    #[test]
    fn grouped_continuation_token_v1_wire_vector_is_frozen() {
        let token = grouped_token_fixture(Direction::Asc);

        let encoded = token
            .encode()
            .expect("grouped continuation token should encode");
        let actual_hex = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_hex,
            "a56776657273696f6e01697369676e61747572659820184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218421842184218426e6c6173745f67726f75705f6b657983a164546578746874656e616e742d61a16455696e7407a164426f6f6cf569646972656374696f6e634173636e696e697469616c5f6f666673657404"
        );
    }

    #[test]
    fn grouped_continuation_token_decode_rejects_unsupported_version() {
        let token = grouped_token_fixture(Direction::Asc);
        let encoded = token
            .encode_with_version_for_test(9)
            .expect("grouped continuation token test wire should encode");
        let err = GroupedContinuationToken::decode(encoded.as_slice())
            .expect_err("unknown grouped cursor wire version must fail");

        assert_eq!(
            err,
            GroupedContinuationTokenError::UnsupportedVersion { version: 9 }
        );
    }

    #[test]
    fn grouped_continuation_token_decode_rejects_oversized_payload() {
        let oversized = vec![0_u8; 8 * 1024 + 1];
        let err = GroupedContinuationToken::decode(oversized.as_slice())
            .expect_err("oversized grouped cursor payload must fail");

        assert!(matches!(err, GroupedContinuationTokenError::Decode(_)));
    }
}
