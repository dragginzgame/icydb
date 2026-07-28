use crate::{
    db::{
        data::{
            decode_structural_field_by_accepted_kind_bytes,
            encode_structural_field_by_accepted_kind_bytes,
            validate_structural_field_by_accepted_kind_bytes,
        },
        schema::AcceptedFieldKind,
    },
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
