//! Module: cursor::token::scalar
//! Responsibility: scalar continuation token domain type and wire conversion helpers.
//! Does not own: grouped continuation semantics or planner continuation policy decisions.
//! Boundary: maps scalar cursor token between in-memory domain and bounded wire payload.

use crate::db::{
    cursor::{ContinuationSignature, CursorBoundary, IndexRangeCursorAnchor},
    direction::Direction,
};

use crate::db::cursor::token::{TokenWireError, decode_scalar_token, encode_scalar_token};

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
        encode_scalar_token(
            self.signature,
            &self.boundary,
            self.direction,
            self.initial_offset,
            self.index_range_anchor(),
        )
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, TokenWireError> {
        let parts = decode_scalar_token(bytes)?;

        match parts.index_range_anchor {
            Some(anchor) => Ok(Self::new_index_range_with_direction(
                parts.signature,
                parts.boundary,
                anchor,
                parts.direction,
                parts.initial_offset,
            )),
            None => Ok(Self::new_with_direction(
                parts.signature,
                parts.boundary,
                parts.direction,
                parts.initial_offset,
            )),
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            cursor::{
                ContinuationSignature, ContinuationToken, CursorBoundary, CursorBoundarySlot,
                IndexRangeCursorAnchor, TokenWireError, encode_cursor,
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
                    CursorBoundarySlot::Present(Value::Nat(7)),
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
                slots: vec![CursorBoundarySlot::Present(Value::Nat(11))],
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
    fn continuation_token_wire_vector_is_frozen() {
        let token = scalar_token_fixture(Direction::Asc);

        let encoded = token
            .encode()
            .expect("scalar continuation token should encode");
        let actual_hex = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_hex,
            "010124242424242424242424242424242424242424242424242424242424242424240000000003000000020113000000000000000701110000000874656e616e742d6100",
            "scalar continuation token wire encoding must remain stable",
        );
    }

    #[test]
    fn continuation_token_index_range_anchor_wire_vector_is_frozen() {
        let token = scalar_index_range_token_fixture();

        let encoded = token
            .encode()
            .expect("scalar index-range continuation token should encode");
        let actual_hex = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_hex,
            "010151515151515151515151515151515151515151515151515151515151515151510000000009000000010113000000000000000b0100000003aabbcc",
            "scalar continuation token with index-range anchor wire encoding must remain stable",
        );
    }

    #[test]
    fn continuation_token_encode_rejects_oversized_payload() {
        let token = ContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([0x24; 32]),
            CursorBoundary {
                slots: vec![CursorBoundarySlot::Present(Value::Blob(vec![
                    0xAA;
                    8 * 1024
                ]))],
            },
            Direction::Asc,
            0,
        );

        let err = token
            .encode()
            .expect_err("oversized scalar cursor payload must fail before emission");

        assert!(matches!(err, TokenWireError::Encode(_)));
    }
}
