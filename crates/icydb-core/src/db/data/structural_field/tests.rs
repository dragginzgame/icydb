use super::{
    decode_relation_target_storage_keys_bytes, decode_structural_field_by_kind_bytes,
    decode_structural_value_storage_bytes, encode_storage_key_binary_value_bytes,
    encode_structural_field_by_accepted_kind_bytes, encode_structural_field_by_kind_bytes,
    encode_structural_value_storage_bytes, validate_structural_field_by_kind_bytes,
    validate_structural_value_storage_bytes,
};
use crate::{
    db::data::structural_field::binary::{
        push_binary_bytes, push_binary_list_len, push_binary_text, push_binary_uint64,
    },
    db::schema::PersistedFieldKind,
    model::field::{FieldKind, RelationStrength},
    types::{
        Account, Decimal, EntityTag, Float32, Float64, Int128, Nat128, Principal, Subaccount, Ulid,
    },
    value::{StorageKey, Value, ValueEnum},
};

static RELATION_ULID_KEY_KIND: FieldKind = FieldKind::Ulid;
static STRONG_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: "RelationTargetEntity",
    target_entity_name: "RelationTargetEntity",
    target_entity_tag: EntityTag::new(7),
    target_store_path: "RelationTargetStore",
    key_kind: &RELATION_ULID_KEY_KIND,
    strength: RelationStrength::Strong,
};
static STRONG_RELATION_LIST_KIND: FieldKind = FieldKind::List(&STRONG_RELATION_KIND);

#[test]
fn relation_target_storage_key_decode_handles_single_ulid_and_null() {
    let target = Ulid::from_u128(7);
    let target_bytes =
        encode_storage_key_binary_value_bytes(STRONG_RELATION_KIND, &Value::Ulid(target), "id")
            .expect("storage-key relation bytes should encode")
            .expect("relation kind should use storage-key binary lane");
    let null_bytes =
        encode_storage_key_binary_value_bytes(STRONG_RELATION_KIND, &Value::Null, "id")
            .expect("null relation bytes should encode")
            .expect("relation kind should use storage-key binary lane");

    let decoded = decode_relation_target_storage_keys_bytes(&target_bytes, STRONG_RELATION_KIND)
        .expect("single relation should decode");
    let decoded_null = decode_relation_target_storage_keys_bytes(&null_bytes, STRONG_RELATION_KIND)
        .expect("null relation should decode");

    assert_eq!(decoded, vec![StorageKey::Ulid(target)]);
    assert!(
        decoded_null.is_empty(),
        "null relation should yield no targets"
    );
}

#[test]
fn relation_target_storage_key_decode_handles_list_and_skips_null_items() {
    let left = Ulid::from_u128(8);
    let right = Ulid::from_u128(9);
    let bytes = encode_storage_key_binary_value_bytes(
        STRONG_RELATION_LIST_KIND,
        &Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]),
        "ids",
    )
    .expect("relation list bytes should encode")
    .expect("relation list should use storage-key binary lane");

    let decoded = decode_relation_target_storage_keys_bytes(&bytes, STRONG_RELATION_LIST_KIND)
        .expect("relation list should decode");

    assert_eq!(
        decoded,
        vec![StorageKey::Ulid(left), StorageKey::Ulid(right)],
    );
}

#[test]
fn accepted_structural_field_encode_matches_generated_simple_kind() {
    let value = Value::Text("Ada".to_string());
    let accepted_kind = PersistedFieldKind::Text { max_len: None };
    let generated_kind = FieldKind::Text { max_len: None };

    let accepted = encode_structural_field_by_accepted_kind_bytes(&accepted_kind, &value, "name")
        .expect("accepted text bytes should encode");
    let generated = encode_structural_field_by_kind_bytes(generated_kind, &value, "name")
        .expect("generated-compatible text bytes should encode");

    assert_eq!(accepted, generated);
}

#[test]
fn accepted_structural_field_encode_matches_generated_recursive_kinds() {
    let list_value = Value::List(vec![
        Value::Text("left".to_string()),
        Value::Text("right".to_string()),
    ]);
    let map_value = Value::Map(vec![
        (Value::Text("alpha".to_string()), Value::Uint(1)),
        (Value::Text("beta".to_string()), Value::Uint(2)),
    ]);
    let accepted_list_kind =
        PersistedFieldKind::List(Box::new(PersistedFieldKind::Text { max_len: None }));
    let accepted_map_kind = PersistedFieldKind::Map {
        key: Box::new(PersistedFieldKind::Text { max_len: None }),
        value: Box::new(PersistedFieldKind::Uint),
    };
    let generated_list_kind = FieldKind::List(&FieldKind::Text { max_len: None });
    let generated_map_kind = FieldKind::Map {
        key: &FieldKind::Text { max_len: None },
        value: &FieldKind::Uint,
    };

    let accepted_list =
        encode_structural_field_by_accepted_kind_bytes(&accepted_list_kind, &list_value, "items")
            .expect("accepted list bytes should encode");
    let generated_list =
        encode_structural_field_by_kind_bytes(generated_list_kind, &list_value, "items")
            .expect("generated-compatible list bytes should encode");
    let accepted_map =
        encode_structural_field_by_accepted_kind_bytes(&accepted_map_kind, &map_value, "entries")
            .expect("accepted map bytes should encode");
    let generated_map =
        encode_structural_field_by_kind_bytes(generated_map_kind, &map_value, "entries")
            .expect("generated-compatible map bytes should encode");

    assert_eq!(accepted_list, generated_list);
    assert_eq!(accepted_map, generated_map);
}

#[test]
fn accepted_structural_field_encode_matches_generated_relation_list_null_skip() {
    let left = Ulid::from_u128(10);
    let right = Ulid::from_u128(11);
    let value = Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]);
    let accepted_kind = PersistedFieldKind::from_model_kind(STRONG_RELATION_LIST_KIND);

    let accepted = encode_structural_field_by_accepted_kind_bytes(&accepted_kind, &value, "ids")
        .expect("accepted relation list bytes should encode");
    let generated = encode_structural_field_by_kind_bytes(STRONG_RELATION_LIST_KIND, &value, "ids")
        .expect("generated-compatible relation list bytes should encode");

    assert_eq!(accepted, generated);
}

#[test]
fn structural_field_decode_list_bytes_preserves_scalar_items() {
    let bytes = encode_structural_field_by_kind_bytes(
        FieldKind::List(&FieldKind::Text { max_len: None }),
        &Value::List(vec![
            Value::Text("left".to_string()),
            Value::Text("right".to_string()),
        ]),
        "items",
    )
    .expect("list bytes should encode");

    let decoded = decode_structural_field_by_kind_bytes(
        &bytes,
        FieldKind::List(&FieldKind::Text { max_len: None }),
    )
    .expect("scalar list field should decode");

    assert_eq!(
        decoded,
        Value::List(vec![
            Value::Text("left".to_string()),
            Value::Text("right".to_string()),
        ]),
    );
}

#[test]
fn structural_field_decode_map_bytes_preserves_scalar_entries() {
    let bytes = encode_structural_field_by_kind_bytes(
        FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::Uint,
        },
        &Value::Map(vec![
            (Value::Text("alpha".to_string()), Value::Uint(1)),
            (Value::Text("beta".to_string()), Value::Uint(2)),
        ]),
        "entries",
    )
    .expect("map bytes should encode");

    let decoded = decode_structural_field_by_kind_bytes(
        &bytes,
        FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::Uint,
        },
    )
    .expect("scalar map field should decode");

    assert_eq!(
        decoded,
        Value::Map(vec![
            (Value::Text("alpha".to_string()), Value::Uint(1)),
            (Value::Text("beta".to_string()), Value::Uint(2)),
        ]),
    );
}

#[test]
fn structural_field_decode_float_scalars_uses_binary_lane() {
    let float32 = Value::Float32(Float32::try_new(3.5).expect("finite f32"));
    let float64 = Value::Float64(Float64::try_new(9.25).expect("finite f64"));

    let float32_bytes =
        encode_structural_field_by_kind_bytes(FieldKind::Float32, &float32, "ratio")
            .expect("float32 bytes should encode");
    let float64_bytes =
        encode_structural_field_by_kind_bytes(FieldKind::Float64, &float64, "score")
            .expect("float64 bytes should encode");

    let decoded_float32 = decode_structural_field_by_kind_bytes(&float32_bytes, FieldKind::Float32)
        .expect("float32 payload should decode");
    let decoded_float64 = decode_structural_field_by_kind_bytes(&float64_bytes, FieldKind::Float64)
        .expect("float64 payload should decode");

    assert_eq!(decoded_float32, float32);
    assert_eq!(decoded_float64, float64);
}

#[test]
fn structural_field_decode_value_storage_handles_enum_payload() {
    let value = Value::Enum(
        ValueEnum::new("Active", Some("Status")).with_payload(Value::Map(vec![(
            Value::Text("count".into()),
            Value::Uint(7),
        )])),
    );
    let bytes = encode_structural_value_storage_bytes(&value).expect("value bytes should encode");

    let decoded =
        decode_structural_value_storage_bytes(&bytes).expect("value enum payload should decode");

    assert_eq!(decoded, value);
}

#[test]
fn structural_field_decode_typed_wrappers_preserves_payloads() {
    let account = Account::from_parts(Principal::dummy(7), Some(Subaccount::from([7_u8; 32])));
    let decimal = Decimal::new(1234, 2);

    let account_bytes = encode_structural_field_by_kind_bytes(
        FieldKind::Account,
        &Value::Account(account),
        "account",
    )
    .expect("account bytes should encode");
    let decimal_bytes = encode_structural_field_by_kind_bytes(
        FieldKind::Decimal { scale: 2 },
        &Value::Decimal(decimal),
        "amount",
    )
    .expect("decimal bytes should encode");

    let decoded_account = decode_structural_field_by_kind_bytes(&account_bytes, FieldKind::Account)
        .expect("account payload should decode");
    let decoded_decimal =
        decode_structural_field_by_kind_bytes(&decimal_bytes, FieldKind::Decimal { scale: 2 })
            .expect("decimal payload should decode");

    assert_eq!(decoded_account, Value::Account(account));
    assert_eq!(decoded_decimal, Value::Decimal(decimal));
}

#[test]
fn structural_field_decode_value_storage_roundtrips_nested_bytes_like_variants() {
    let nested = Value::from_map(vec![
        (
            Value::Text("blob".to_string()),
            Value::Blob(vec![0x10, 0x20, 0x30]),
        ),
        (
            Value::Text("i128".to_string()),
            Value::Int128(Int128::from(-123i128)),
        ),
        (
            Value::Text("u128".to_string()),
            Value::Uint128(Nat128::from(456u128)),
        ),
        (
            Value::Text("list".to_string()),
            Value::List(vec![
                Value::Blob(vec![0xAA, 0xBB]),
                Value::Int128(Int128::from(7i128)),
                Value::Uint128(Nat128::from(8u128)),
            ]),
        ),
        (
            Value::Text("enum".to_string()),
            Value::Enum(
                ValueEnum::new("Loaded", Some("tests::StructuredPayload"))
                    .with_payload(Value::Blob(vec![0xCC, 0xDD])),
            ),
        ),
    ])
    .expect("nested value payload should normalize");
    let bytes = encode_structural_value_storage_bytes(&nested)
        .expect("nested value payload should serialize");

    let decoded = decode_structural_value_storage_bytes(&bytes)
        .expect("nested value payload should decode through value storage");

    assert_eq!(decoded, nested);
}

#[test]
fn structural_field_validate_matches_decode_for_malformed_leaf_payloads() {
    let mut bytes = Vec::new();
    push_binary_list_len(&mut bytes, 2);
    push_binary_bytes(&mut bytes, &1_i128.to_be_bytes());
    push_binary_uint64(&mut bytes, u64::from(Decimal::max_supported_scale() + 1));

    let decode =
        decode_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::Decimal { scale: 2 });
    let validate =
        validate_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::Decimal { scale: 2 });

    assert!(
        decode.is_err(),
        "malformed decimal payload must fail decode"
    );
    assert!(
        validate.is_err(),
        "malformed decimal payload must fail validate"
    );
}

#[test]
fn structural_field_validate_matches_decode_for_malformed_storage_key_payloads() {
    let mut bytes = Vec::new();
    push_binary_text(&mut bytes, "aaaaa-aa");

    let decode = decode_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::Principal);
    let validate = validate_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::Principal);

    assert!(decode.is_err(), "principal text payload must fail decode");
    assert!(
        validate.is_err(),
        "principal text payload must fail validate"
    );
}

#[test]
fn structural_field_validate_matches_decode_for_malformed_composite_payloads() {
    let mut bytes = encode_structural_field_by_kind_bytes(
        FieldKind::List(&FieldKind::Text { max_len: None }),
        &Value::List(vec![Value::Text("left".to_string())]),
        "items",
    )
    .expect("list bytes should encode");
    bytes.push(0x00);

    let decode = decode_structural_field_by_kind_bytes(
        bytes.as_slice(),
        FieldKind::List(&FieldKind::Text { max_len: None }),
    );
    let validate = validate_structural_field_by_kind_bytes(
        bytes.as_slice(),
        FieldKind::List(&FieldKind::Text { max_len: None }),
    );

    assert!(decode.is_err(), "trailing list bytes must fail decode");
    assert!(validate.is_err(), "trailing list bytes must fail validate");
}

#[test]
fn structural_value_storage_validate_matches_decode_for_malformed_payloads() {
    let bytes = [0xF6];

    let decode = decode_structural_value_storage_bytes(&bytes);
    let validate = validate_structural_value_storage_bytes(&bytes);

    assert!(decode.is_err(), "unknown value tag must fail decode");
    assert!(validate.is_err(), "unknown value tag must fail validate");
}
