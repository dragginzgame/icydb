use crate::{
    db::{
        data::{
            decode_structural_field_by_accepted_kind_bytes,
            encode_structural_field_by_accepted_kind_bytes,
            validate_structural_field_by_accepted_kind_bytes,
        },
        schema::AcceptedFieldKind,
    },
    types::{Decimal, IntBig},
    value::Value,
};

fn assert_accepted_roundtrip(kind: &AcceptedFieldKind, value: &Value, field_name: &str) {
    let encoded = encode_structural_field_by_accepted_kind_bytes(kind, value, field_name)
        .expect("accepted payload should encode");
    let decoded = decode_structural_field_by_accepted_kind_bytes(&encoded, kind)
        .expect("accepted payload should decode");

    validate_structural_field_by_accepted_kind_bytes(&encoded, kind)
        .expect("accepted payload should validate");
    assert_eq!(decoded, *value);
}

fn assert_accepted_rejects(kind: &AcceptedFieldKind, raw_bytes: &[u8]) {
    assert!(decode_structural_field_by_accepted_kind_bytes(raw_bytes, kind).is_err());
    assert!(validate_structural_field_by_accepted_kind_bytes(raw_bytes, kind).is_err());
}

#[test]
fn accepted_kind_codec_roundtrips_nested_collections() {
    let kind = AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: None }),
        value: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
    };
    let value = Value::Map(vec![
        (
            Value::Text("alpha".to_string()),
            Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
        ),
        (
            Value::Text("beta".to_string()),
            Value::List(vec![Value::Nat64(3)]),
        ),
    ]);

    assert_accepted_roundtrip(&kind, &value, "payload");
}

#[test]
fn accepted_kind_codec_rejects_truncated_nested_collections() {
    let cases = [
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
            "numbers",
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Text { max_len: None }),
                value: Box::new(AcceptedFieldKind::Nat64),
            },
            Value::Map(vec![(Value::Text("alpha".to_string()), Value::Nat64(1))]),
            "entries",
        ),
    ];

    for (kind, value, field_name) in cases {
        let mut malformed =
            encode_structural_field_by_accepted_kind_bytes(&kind, &value, field_name)
                .expect("accepted payload should encode");
        malformed.pop();
        assert_accepted_rejects(&kind, malformed.as_slice());
    }
}

#[test]
fn accepted_kind_codec_roundtrips_relation_lists() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Relation {
        target_path: "tests::Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
        target_store_path: "tests::TargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    }));
    let value = Value::List(vec![
        Value::Ulid(crate::types::Ulid::from_u128(11)),
        Value::Ulid(crate::types::Ulid::from_u128(12)),
    ]);

    assert_accepted_roundtrip(&kind, &value, "targets");
}

// Both operations must accept the same wire language. Materialization may
// allocate output, but must not depend on a preceding validation-only walk.
fn assert_decode_validation_parity(kind: &AcceptedFieldKind, bytes: &[u8]) {
    assert_eq!(
        decode_structural_field_by_accepted_kind_bytes(bytes, kind).is_ok(),
        validate_structural_field_by_accepted_kind_bytes(bytes, kind).is_ok(),
        "decode/validation parity for {kind:?} and {bytes:?}",
    );
}

#[test]
fn accepted_by_kind_decode_and_validation_agree_on_mutated_wire() {
    let cases = [
        (AcceptedFieldKind::Nat8, Value::Nat64(255)),
        (AcceptedFieldKind::Int8, Value::Int64(-128)),
        (AcceptedFieldKind::Bool, Value::Bool(true)),
        (
            AcceptedFieldKind::Text { max_len: None },
            Value::Text("text".into()),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            Value::Blob(vec![1, 2, 3]),
        ),
        (
            AcceptedFieldKind::Decimal { scale: 2 },
            Value::Decimal(Decimal::from_i128_with_scale(123, 2)),
        ),
        (
            AcceptedFieldKind::IntBig { max_bytes: 16 },
            Value::IntBig(IntBig::from(-123_i64)),
        ),
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Text { max_len: None }),
                value: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
            },
            Value::Map(vec![(
                Value::Text("key".into()),
                Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
            )]),
        ),
    ];
    for (kind, value) in cases {
        assert_accepted_roundtrip(&kind, &value, "payload");
        let wire =
            encode_structural_field_by_accepted_kind_bytes(&kind, &value, "payload").unwrap();
        for end in 0..wire.len() {
            assert_accepted_rejects(&kind, &wire[..end]);
        }
        let mut trailing = wire.clone();
        trailing.push(0);
        assert_accepted_rejects(&kind, &trailing);
        for index in 0..wire.len() {
            for byte in [0, 1, 127, 255] {
                let mut mutated = wire.clone();
                mutated[index] = byte;
                assert_decode_validation_parity(&kind, &mutated);
            }
        }
    }
}

#[test]
fn accepted_relation_null_filtering_still_checks_following_items() {
    let relation = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Relation {
        target_path: "tests::Target".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
        target_store_path: "tests::TargetStore".into(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    }));
    let key = Value::Ulid(crate::types::Ulid::from_u128(11));
    // Encode as a plain list so a null relation item remains on the wire.
    let wire = encode_structural_field_by_accepted_kind_bytes(
        &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Ulid)),
        &Value::List(vec![Value::Null, key.clone()]),
        "payload",
    )
    .unwrap();
    validate_structural_field_by_accepted_kind_bytes(&wire, &relation).unwrap();
    assert_eq!(
        decode_structural_field_by_accepted_kind_bytes(&wire, &relation).unwrap(),
        Value::List(vec![key])
    );
    for end in 0..wire.len() {
        assert_accepted_rejects(&relation, &wire[..end]);
    }
    let invalid = encode_structural_field_by_accepted_kind_bytes(
        &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        &Value::List(vec![Value::Null, Value::Nat64(11)]),
        "payload",
    )
    .unwrap();
    assert_accepted_rejects(&relation, &invalid);
}
