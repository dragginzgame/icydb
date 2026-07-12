use crate::{
    db::{
        data::{
            decode_structural_field_by_accepted_kind_bytes, decode_structural_field_by_kind_bytes,
            encode_structural_field_by_kind_bytes,
            validate_structural_field_by_accepted_kind_bytes,
            validate_structural_field_by_kind_bytes,
        },
        schema::AcceptedFieldKind,
    },
    model::field::FieldKind,
    value::Value,
};

fn assert_generated_and_accepted_decode_match(
    generated_kind: FieldKind,
    accepted_kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) {
    let encoded = encode_structural_field_by_kind_bytes(generated_kind, value, field_name)
        .expect("generated-compatible test payload should encode");
    let generated = decode_structural_field_by_kind_bytes(&encoded, generated_kind)
        .expect("generated decoder should decode test payload");
    let accepted = decode_structural_field_by_accepted_kind_bytes(&encoded, accepted_kind)
        .expect("accepted decoder should decode generated-compatible payload");

    validate_structural_field_by_accepted_kind_bytes(&encoded, accepted_kind)
        .expect("accepted kind should validate generated-compatible payload");

    assert_eq!(generated, *value);
    assert_eq!(accepted, generated);
}

fn assert_generated_and_accepted_reject_match(
    generated_kind: FieldKind,
    accepted_kind: &AcceptedFieldKind,
    raw_bytes: &[u8],
) {
    assert!(decode_structural_field_by_kind_bytes(raw_bytes, generated_kind).is_err());
    assert!(decode_structural_field_by_accepted_kind_bytes(raw_bytes, accepted_kind).is_err());
    assert!(validate_structural_field_by_kind_bytes(raw_bytes, generated_kind).is_err());
    assert!(validate_structural_field_by_accepted_kind_bytes(raw_bytes, accepted_kind).is_err());
}

#[test]
fn accepted_kind_decoder_matches_generated_nested_collection_payloads() {
    let generated_kind = FieldKind::Map {
        key: &FieldKind::Text { max_len: None },
        value: &FieldKind::List(&FieldKind::Nat64),
    };
    let accepted_kind = AcceptedFieldKind::Map {
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

    assert_generated_and_accepted_decode_match(generated_kind, &accepted_kind, &value, "payload");
}

#[test]
fn accepted_kind_decoder_rejects_malformed_nested_lists_like_generated_decoder() {
    let generated_kind = FieldKind::List(&FieldKind::Nat64);
    let accepted_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64));
    let value = Value::List(vec![Value::Nat64(1), Value::Nat64(2)]);
    let mut malformed = encode_structural_field_by_kind_bytes(generated_kind, &value, "numbers")
        .expect("generated-compatible list payload should encode");
    malformed.pop();

    assert_generated_and_accepted_reject_match(
        generated_kind,
        &accepted_kind,
        malformed.as_slice(),
    );
}

#[test]
fn accepted_kind_decoder_rejects_malformed_nested_maps_like_generated_decoder() {
    let generated_kind = FieldKind::Map {
        key: &FieldKind::Text { max_len: None },
        value: &FieldKind::Nat64,
    };
    let accepted_kind = AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: None }),
        value: Box::new(AcceptedFieldKind::Nat64),
    };
    let value = Value::Map(vec![(Value::Text("alpha".to_string()), Value::Nat64(1))]);
    let mut malformed = encode_structural_field_by_kind_bytes(generated_kind, &value, "entries")
        .expect("generated-compatible map payload should encode");
    malformed.pop();

    assert_generated_and_accepted_reject_match(
        generated_kind,
        &accepted_kind,
        malformed.as_slice(),
    );
}

#[test]
fn accepted_kind_decoder_matches_generated_relation_list_payloads() {
    const RELATION_KEY_KIND: FieldKind = FieldKind::Ulid;
    let generated_kind = FieldKind::List(&FieldKind::Relation {
        target_path: "tests::Target",
        target_entity_name: "Target",
        target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
        target_store_path: "tests::TargetStore",
        key_kind: &RELATION_KEY_KIND,
        strength: crate::model::field::RelationStrength::Strong,
    });
    let accepted_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Relation {
        target_path: "tests::Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
        target_store_path: "tests::TargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
        strength: crate::db::schema::AcceptedRelationStrength::Strong,
    }));
    let value = Value::List(vec![
        Value::Ulid(crate::types::Ulid::from_u128(11)),
        Value::Ulid(crate::types::Ulid::from_u128(12)),
    ]);

    assert_generated_and_accepted_decode_match(generated_kind, &accepted_kind, &value, "targets");
}
