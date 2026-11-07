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

const MAC_KEY: [u8; 32] = [0x55; 32];

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
fn grouped_cursor_authenticates_every_byte_and_key() {
    let token = grouped_token_fixture(Direction::Asc);
    let encoded = token.encode(&MAC_KEY).unwrap();
    for offset in 0..encoded.len() {
        let mut changed = encoded.clone();
        changed[offset] ^= 1;
        assert_eq!(
            GroupedContinuationToken::decode(&changed, &MAC_KEY),
            Err(TokenWireError::Decode)
        );
    }
    for end in 0..encoded.len() {
        assert_eq!(
            GroupedContinuationToken::decode(&encoded[..end], &MAC_KEY),
            Err(TokenWireError::Decode)
        );
    }
    assert_eq!(
        GroupedContinuationToken::decode(&encoded, &[0x56; 32]),
        Err(TokenWireError::Decode)
    );
    let mut trailing = encoded;
    trailing.push(0);
    assert_eq!(
        GroupedContinuationToken::decode(&trailing, &MAC_KEY),
        Err(TokenWireError::Decode)
    );
}

#[test]
fn grouped_continuation_token_round_trip_preserves_fields() {
    let token = grouped_token_fixture(Direction::Asc);

    let encoded = token
        .encode(&MAC_KEY)
        .expect("grouped continuation token should encode");
    let decoded = GroupedContinuationToken::decode(encoded.as_slice(), &MAC_KEY)
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
        .encode(&MAC_KEY)
        .expect("grouped continuation token should encode");
    let encoded_text = encode_grouped_cursor_token(&token, &MAC_KEY)
        .expect("grouped continuation token text encoder should succeed");

    assert_eq!(encoded_text, encode_cursor(encoded.as_slice()));
}

#[test]
fn grouped_continuation_token_wire_vector_is_frozen() {
    let token = grouped_token_fixture(Direction::Asc);

    let encoded = token
        .encode(&MAC_KEY)
        .expect("grouped continuation token should encode");
    let actual_text = encode_cursor(encoded.as_slice());
    assert_eq!(
        actual_text,
        "SUNZTgECAAAAQUJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCAAAAAAQAAAADEAAAAAh0ZW5hbnQtYRIAAAAAAAAABwIBqZckel8SjqCa4SM5dAfPNaAj41TSBUy3xNmS6cdjtX4"
    );
}

#[test]
fn grouped_continuation_token_desc_wire_vector_is_frozen() {
    let token = grouped_token_fixture(Direction::Desc);

    let encoded = token
        .encode(&MAC_KEY)
        .expect("grouped continuation token should encode");
    let actual_text = encode_cursor(encoded.as_slice());
    assert_eq!(
        actual_text,
        "SUNZTgECAAAAQUJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCAQAAAAQAAAADEAAAAAh0ZW5hbnQtYRIAAAAAAAAABwIBGcTjbDo2jqcN4c8nQtN_wdd6vfEZfQty1-VLPfXuMS4",
        "grouped continuation token DESC wire encoding must remain stable",
    );
}

#[test]
fn grouped_continuation_token_decode_rejects_oversized_payload() {
    let oversized = vec![0_u8; 8 * 1024 + 1];
    let err = GroupedContinuationToken::decode(oversized.as_slice(), &MAC_KEY)
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
        .encode(&MAC_KEY)
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
        .encode(&MAC_KEY)
        .expect("grouped unit-enum continuation token should encode");
    assert_eq!(
        encode_cursor(&encoded),
        "SUNZTgECAAAAMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzAAAAAAAAAAABBgAAAAIAAAADAAqrcmtNxdKOUC9UQ1vyOvww1tQpGv_o0SY6RCbw2yBt"
    );
}

#[test]
fn compact_grouped_cursor_round_trips_and_measures_external_bytes() {
    use crate::db::cursor::decode_cursor;

    let token = GroupedContinuationToken::new_with_direction(
        ContinuationSignature::from_bytes([0x42; 32]),
        vec![Value::U256(crate::types::U256::MAX)],
        Direction::Desc,
        4,
    );
    for _ in 0..1000 {
        let bytes = token.encode(&MAC_KEY).unwrap();
        let text = encode_cursor(&bytes);
        assert_eq!(bytes.len(), 116);
        assert_eq!(text.len(), 155);
        let decoded =
            GroupedContinuationToken::decode(&decode_cursor(&text).unwrap(), &MAC_KEY).unwrap();
        assert_eq!(decoded.last_group_key(), token.last_group_key());
        assert_eq!(decoded.direction(), token.direction());
        assert_eq!(decoded.initial_offset(), token.initial_offset());
    }
}
