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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            codec::cursor::encode_cursor,
            cursor::{
                ContinuationSignature, ContinuationToken, CursorBoundary, CursorBoundarySlot,
                IndexRangeCursorAnchor, TokenWireError,
            },
            direction::Direction,
        },
        value::Value,
    };

    fn scalar_token_fixture(direction: Direction) -> ContinuationToken {
        ContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([0x24; 32]),
            CursorBoundary {
                slots: vec![
                    CursorBoundarySlot::Present(Value::Uint(7)),
                    CursorBoundarySlot::Present(Value::Text("tenant-a".to_string())),
                ],
            },
            direction,
            3,
        )
    }

    fn scalar_index_range_token_fixture() -> ContinuationToken {
        ContinuationToken::new_index_range_with_direction(
            ContinuationSignature::from_bytes([0x51; 32]),
            CursorBoundary {
                slots: vec![CursorBoundarySlot::Present(Value::Uint(11))],
            },
            IndexRangeCursorAnchor::new(vec![0xAA, 0xBB, 0xCC]),
            Direction::Asc,
            9,
        )
    }

    #[test]
    fn continuation_token_round_trip_preserves_fields() {
        let token = scalar_token_fixture(Direction::Desc);

        let encoded = token
            .encode()
            .expect("scalar continuation token should encode");
        let decoded = ContinuationToken::decode(encoded.as_slice())
            .expect("scalar continuation token should decode");

        assert_eq!(decoded.signature(), token.signature());
        assert_eq!(decoded.boundary(), token.boundary());
        assert_eq!(decoded.direction(), token.direction());
        assert_eq!(decoded.initial_offset(), token.initial_offset());
        assert_eq!(decoded.index_range_anchor(), token.index_range_anchor());
    }

    #[test]
    fn continuation_token_v2_wire_vector_is_frozen() {
        let token = scalar_token_fixture(Direction::Asc);

        let encoded = token
            .encode()
            .expect("scalar continuation token should encode");
        let actual_hex = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_hex,
            "a66776657273696f6e02697369676e617475726598201824182418241824182418241824182418241824182418241824182418241824182418241824182418241824182418241824182418241824182418241824182468626f756e64617279a165736c6f747382a16750726573656e74a16455696e7407a16750726573656e74a164546578746874656e616e742d6169646972656374696f6e634173636e696e697469616c5f6f66667365740372696e6465785f72616e67655f616e63686f72f6",
            "scalar continuation token v2 wire encoding must remain stable",
        );
    }

    #[test]
    fn continuation_token_v2_index_range_anchor_wire_vector_is_frozen() {
        let token = scalar_index_range_token_fixture();

        let encoded = token
            .encode()
            .expect("scalar index-range continuation token should encode");
        let actual_hex = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_hex,
            "a66776657273696f6e02697369676e617475726598201851185118511851185118511851185118511851185118511851185118511851185118511851185118511851185118511851185118511851185118511851185168626f756e64617279a165736c6f747381a16750726573656e74a16455696e740b69646972656374696f6e634173636e696e697469616c5f6f66667365740972696e6465785f72616e67655f616e63686f72a16c6c6173745f7261775f6b65798318aa18bb18cc",
            "scalar continuation token v2 with index-range anchor wire encoding must remain stable",
        );
    }

    #[test]
    fn continuation_token_decode_rejects_unsupported_version() {
        let token = scalar_token_fixture(Direction::Asc);
        let encoded = token
            .encode_with_version_for_test(99)
            .expect("unknown-version scalar token should encode");
        let err = ContinuationToken::decode(encoded.as_slice())
            .expect_err("unknown scalar token version must fail");

        assert_eq!(err, TokenWireError::UnsupportedVersion { version: 99 });
    }
}
