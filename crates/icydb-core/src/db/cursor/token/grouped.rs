use crate::{
    db::{
        codec::deserialize_protocol_payload, cursor::ContinuationSignature, direction::Direction,
    },
    serialize::serialize,
    value::Value,
};

use crate::db::cursor::token::{
    GroupedContinuationTokenWire, GroupedCursorTokenVersion, MAX_GROUPED_CONTINUATION_TOKEN_BYTES,
    TokenWireError,
};

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

    pub(in crate::db) fn encode(&self) -> Result<Vec<u8>, TokenWireError> {
        let wire = GroupedContinuationTokenWire {
            version: GroupedCursorTokenVersion::V1.encode(),
            signature: self.signature.into_bytes(),
            last_group_key: self.last_group_key.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
        };

        serialize(&wire).map_err(|err| TokenWireError::encode(err.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn encode_with_version_for_test(
        &self,
        version: u8,
    ) -> Result<Vec<u8>, TokenWireError> {
        let wire = GroupedContinuationTokenWire {
            version,
            signature: self.signature.into_bytes(),
            last_group_key: self.last_group_key.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
        };

        serialize(&wire).map_err(|err| TokenWireError::encode(err.to_string()))
    }

    pub(in crate::db) fn decode(bytes: &[u8]) -> Result<Self, TokenWireError> {
        let wire: GroupedContinuationTokenWire =
            deserialize_protocol_payload(bytes, MAX_GROUPED_CONTINUATION_TOKEN_BYTES)
                .map_err(|err| TokenWireError::decode(err.to_string()))?;
        let version = GroupedCursorTokenVersion::decode(wire.version)
            .ok_or_else(|| TokenWireError::unsupported_version(wire.version))?;

        Ok(Self::new_with_direction(
            ContinuationSignature::from_bytes(wire.signature),
            wire.last_group_key,
            wire.direction,
            version.decode_initial_offset(wire.initial_offset),
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
            codec::cursor::encode_cursor,
            cursor::{ContinuationSignature, GroupedContinuationToken, TokenWireError},
            direction::Direction,
        },
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

        assert_eq!(err, TokenWireError::UnsupportedVersion { version: 9 });
    }

    #[test]
    fn grouped_continuation_token_decode_rejects_oversized_payload() {
        let oversized = vec![0_u8; 8 * 1024 + 1];
        let err = GroupedContinuationToken::decode(oversized.as_slice())
            .expect_err("oversized grouped cursor payload must fail");

        assert!(matches!(err, TokenWireError::Decode(_)));
    }
}
