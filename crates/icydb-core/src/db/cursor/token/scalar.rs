use crate::{
    db::{
        codec::deserialize_protocol_payload,
        cursor::{ContinuationSignature, CursorBoundary, IndexRangeCursorAnchor},
        direction::Direction,
    },
    serialize::serialize,
};

use crate::db::cursor::token::{
    ContinuationTokenWire, CursorTokenVersion, IndexRangeCursorAnchorWire,
    MAX_CONTINUATION_TOKEN_BYTES, TokenWireError,
};

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

    pub(crate) fn encode(&self) -> Result<Vec<u8>, TokenWireError> {
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

        serialize(&wire).map_err(|err| TokenWireError::encode(err.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, TokenWireError> {
        let wire: ContinuationTokenWire =
            deserialize_protocol_payload(bytes, MAX_CONTINUATION_TOKEN_BYTES)
                .map_err(|err| TokenWireError::decode(err.to_string()))?;

        // Decode the protocol version first so compatibility behavior remains centralized.
        let version = CursorTokenVersion::decode(wire.version)
            .ok_or_else(|| TokenWireError::unsupported_version(wire.version))?;
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
    ) -> Result<Vec<u8>, TokenWireError> {
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

        serialize(&wire).map_err(|err| TokenWireError::encode(err.to_string()))
    }
}
