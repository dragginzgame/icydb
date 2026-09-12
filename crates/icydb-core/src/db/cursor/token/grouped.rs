//! Module: cursor::token::grouped
//! Responsibility: grouped continuation token domain type and wire conversion helpers.
//! Does not own: scalar continuation token policy or planner continuation semantics.
//! Boundary: maps grouped cursor tokens between runtime domain and bounded wire payloads.

use crate::{
    db::{cursor::ContinuationSignature, direction::Direction},
    value::Value,
};

use crate::db::cursor::token::{TokenWireError, decode_grouped_token, encode_grouped_token};

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

    #[cfg(test)]
    pub(in crate::db) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    #[cfg(test)]
    pub(in crate::db) const fn last_group_key(&self) -> &[Value] {
        self.last_group_key.as_slice()
    }

    #[cfg(test)]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    #[cfg(test)]
    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }

    pub(in crate::db) fn into_components(
        self,
    ) -> (ContinuationSignature, Vec<Value>, Direction, u32) {
        (
            self.signature,
            self.last_group_key,
            self.direction,
            self.initial_offset,
        )
    }

    pub(in crate::db) fn encode(&self) -> Result<Vec<u8>, TokenWireError> {
        encode_grouped_token(
            self.signature,
            self.last_group_key.as_slice(),
            self.direction,
            self.initial_offset,
        )
    }

    pub(in crate::db) fn decode(bytes: &[u8]) -> Result<Self, TokenWireError> {
        let payload = decode_grouped_token(bytes)?;

        Ok(Self::new_with_direction(
            payload.signature,
            payload.last_group_key,
            payload.direction,
            payload.initial_offset,
        ))
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
                ContinuationSignature, GroupedContinuationToken, TokenWireError, encode_cursor,
                encode_grouped_cursor_token,
            },
            direction::Direction,
        },
        value::{CanonicalEnumBody, EnumTypeId, EnumVariantId, Value, ValueEnum},
    };

    fn grouped_token_fixture(direction: Direction) -> GroupedContinuationToken {
        GroupedContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([0x42; 32]),
            vec![
                Value::Text("tenant-a".to_string()),
                Value::Nat64(7),
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
    fn grouped_continuation_token_external_encoding_matches_encoded_bytes() {
        let token = grouped_token_fixture(Direction::Asc);
        let encoded = token
            .encode()
            .expect("grouped continuation token should encode");
        let encoded_text = encode_grouped_cursor_token(&token)
            .expect("grouped continuation token text encoder should succeed");

        assert_eq!(encoded_text, encode_cursor(encoded.as_slice()));
    }

    #[test]
    fn grouped_continuation_token_wire_vector_is_frozen() {
        let token = grouped_token_fixture(Direction::Asc);

        let encoded = token
            .encode()
            .expect("grouped continuation token should encode");
        let actual_text = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_text,
            "SUNZUQECQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkIAAAAABAAAAAMRAAAACHRlbmFudC1hEwAAAAAAAAAHAgE"
        );
    }

    #[test]
    fn grouped_continuation_token_desc_wire_vector_is_frozen() {
        let token = grouped_token_fixture(Direction::Desc);

        let encoded = token
            .encode()
            .expect("grouped continuation token should encode");
        let actual_text = encode_cursor(encoded.as_slice());
        assert_eq!(
            actual_text,
            "SUNZUQECQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkIBAAAABAAAAAMRAAAACHRlbmFudC1hEwAAAAAAAAAHAgE",
            "grouped continuation token DESC wire encoding must remain stable",
        );
    }

    #[test]
    fn grouped_continuation_token_decode_rejects_oversized_payload() {
        let oversized = vec![0_u8; 8 * 1024 + 1];
        let err = GroupedContinuationToken::decode(oversized.as_slice())
            .expect_err("oversized grouped cursor payload must fail");

        std::assert_matches!(err, TokenWireError::Decode);
    }

    #[test]
    fn grouped_continuation_token_encode_rejects_oversized_payload() {
        let token = GroupedContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([0x42; 32]),
            vec![Value::Blob(vec![0xAA; 8 * 1024])],
            Direction::Asc,
            0,
        );

        let err = token
            .encode()
            .expect_err("oversized grouped cursor payload must fail before emission");

        std::assert_matches!(err, TokenWireError::Encode);
    }

    #[test]
    fn grouped_continuation_token_unit_enum_wire_vector_is_frozen() {
        let token = GroupedContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([0x33; 32]),
            vec![Value::Enum(ValueEnum::new(
                EnumTypeId::new(2).expect("test enum type ID should be valid"),
                EnumVariantId::new(3).expect("test enum variant ID should be valid"),
                CanonicalEnumBody::Unit,
            ))],
            Direction::Asc,
            0,
        );
        let encoded = token
            .encode()
            .expect("grouped unit-enum continuation token should encode");
        let mut expected = b"ICYQ".to_vec();
        expected.extend_from_slice(&[1, 2]);
        expected.extend_from_slice(&[0x33; 32]);
        expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 1, 6, 0, 0, 0, 2, 0, 0, 0, 3, 0]);

        assert_eq!(encoded, expected);
    }

    #[test]
    fn compact_grouped_cursor_round_trips_and_measures_external_bytes() {
        use crate::{db::cursor::decode_cursor, types::NatBig};
        use num_bigint::BigUint;

        let magnitude = (BigUint::from(1_u8) << 256_usize) - BigUint::from(1_u8);
        let token = GroupedContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([0x42; 32]),
            vec![Value::NatBig(NatBig::from_biguint(magnitude))],
            Direction::Desc,
            4,
        );
        for _ in 0..1000 {
            let bytes = token.encode().unwrap();
            let text = encode_cursor(&bytes);
            assert_eq!(bytes.len(), 84);
            assert_eq!(text.len(), 112);
            let decoded = GroupedContinuationToken::decode(&decode_cursor(&text).unwrap()).unwrap();
            assert_eq!(decoded.last_group_key(), token.last_group_key());
            assert_eq!(decoded.direction(), token.direction());
            assert_eq!(decoded.initial_offset(), token.initial_offset());
        }
    }
}
