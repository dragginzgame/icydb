use crate::{
    db::data::structural_field::storage_key::{
        decode::decode_relation_target_storage_keys_binary_bytes,
        decode::{decode_storage_key_binary_value_bytes, decode_storage_key_field_binary_bytes},
        encode::encode_relation_target_storage_keys_binary_bytes,
        encode::{encode_storage_key_binary_value_bytes, encode_storage_key_field_binary_bytes},
        validate_storage_key_binary_value_bytes,
    },
    model::field::{FieldKind, RelationStrength},
    types::{Account, EntityTag, Principal, Subaccount, Timestamp, Ulid},
    value::{StorageKey, Value},
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

const TAG_UNIT: u8 = 0x01;
const TAG_UINT64: u8 = 0x10;
const TAG_INT64: u8 = 0x11;
const TAG_TEXT: u8 = 0x12;
const TAG_BYTES: u8 = 0x13;
const TAG_LIST: u8 = 0x20;

fn encode_unit() -> Vec<u8> {
    vec![TAG_UNIT]
}

fn encode_uint64(value: u64) -> Vec<u8> {
    let mut out = vec![TAG_UINT64];
    out.extend_from_slice(&value.to_be_bytes());
    out
}

fn encode_int64(value: i64) -> Vec<u8> {
    let mut out = vec![TAG_INT64];
    out.extend_from_slice(&value.to_be_bytes());
    out
}

fn encode_text(value: &str) -> Vec<u8> {
    let mut out = vec![TAG_TEXT];
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("text len fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(value.as_bytes());
    out
}

fn encode_bytes(value: &[u8]) -> Vec<u8> {
    let mut out = vec![TAG_BYTES];
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("byte len fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(value);
    out
}

fn encode_list(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![TAG_LIST];
    out.extend_from_slice(
        &u32::try_from(items.len())
            .expect("list len fits u32")
            .to_be_bytes(),
    );
    for item in items {
        out.extend_from_slice(item);
    }
    out
}

#[test]
fn storage_key_binary_roundtrips_all_supported_scalar_kinds() {
    let account = Account::from_parts(Principal::dummy(3), Some(Subaccount::from([3_u8; 32])));
    let timestamp = Timestamp::from_millis(1_710_013_530_123);
    let ulid = Ulid::from_u128(77);
    let cases = vec![
        (
            FieldKind::Account,
            StorageKey::Account(account),
            Value::Account(account),
        ),
        (FieldKind::Int, StorageKey::Int(-9), Value::Int(-9)),
        (
            FieldKind::Principal,
            StorageKey::Principal(Principal::dummy(5)),
            Value::Principal(Principal::dummy(5)),
        ),
        (
            FieldKind::Subaccount,
            StorageKey::Subaccount(Subaccount::from([8_u8; 32])),
            Value::Subaccount(Subaccount::from([8_u8; 32])),
        ),
        (
            FieldKind::Timestamp,
            StorageKey::Timestamp(timestamp),
            Value::Timestamp(timestamp),
        ),
        (FieldKind::Uint, StorageKey::Uint(42), Value::Uint(42)),
        (FieldKind::Ulid, StorageKey::Ulid(ulid), Value::Ulid(ulid)),
        (FieldKind::Unit, StorageKey::Unit, Value::Unit),
    ];

    for (kind, key, value) in cases {
        let encoded = encode_storage_key_field_binary_bytes(key, kind, "field")
            .expect("storage-key payload should encode");
        let decoded_key = decode_storage_key_field_binary_bytes(encoded.as_slice(), kind)
            .expect("storage-key payload should decode");
        let decoded_value = decode_storage_key_binary_value_bytes(encoded.as_slice(), kind)
            .expect("storage-key value decode should succeed")
            .expect("supported kind should stay on the storage-key lane");

        assert!(
            validate_storage_key_binary_value_bytes(encoded.as_slice(), kind)
                .expect("storage-key payload should validate"),
            "supported storage-key kind should validate as storage-key-owned"
        );
        assert_eq!(decoded_key, key, "decoded key mismatch for {kind:?}");
        assert_eq!(decoded_value, value, "decoded value mismatch for {kind:?}");
    }
}

#[test]
fn storage_key_binary_roundtrips_relation_payloads() {
    let left = Ulid::from_u128(100);
    let right = Ulid::from_u128(200);
    let single =
        encode_storage_key_binary_value_bytes(STRONG_RELATION_KIND, &Value::Ulid(left), "relation")
            .expect("single relation should encode")
            .expect("relation kind should stay on storage-key lane");
    let many = encode_storage_key_binary_value_bytes(
        STRONG_RELATION_LIST_KIND,
        &Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]),
        "relations",
    )
    .expect("many relation should encode")
    .expect("relation list kind should stay on storage-key lane");

    assert_eq!(
        decode_storage_key_binary_value_bytes(single.as_slice(), STRONG_RELATION_KIND)
            .expect("single relation should decode")
            .expect("single relation should be storage-key-owned"),
        Value::Ulid(left),
    );
    assert_eq!(
        decode_relation_target_storage_keys_binary_bytes(single.as_slice(), STRONG_RELATION_KIND)
            .expect("single relation target keys should decode"),
        vec![StorageKey::Ulid(left)],
    );
    assert_eq!(
        decode_storage_key_binary_value_bytes(many.as_slice(), STRONG_RELATION_LIST_KIND)
            .expect("many relation should decode")
            .expect("relation list should be storage-key-owned"),
        Value::List(vec![Value::Ulid(left), Value::Ulid(right)]),
    );
    assert_eq!(
        decode_relation_target_storage_keys_binary_bytes(
            many.as_slice(),
            STRONG_RELATION_LIST_KIND
        )
        .expect("many relation target keys should decode"),
        vec![StorageKey::Ulid(left), StorageKey::Ulid(right)],
    );
}

#[test]
fn storage_key_binary_rejects_malformed_account_payload() {
    let bytes = encode_list(&[encode_bytes(Principal::dummy(1).as_slice())]);

    let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Account);
    let validate = validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Account);

    assert!(
        decode.is_err(),
        "malformed account payload must fail decode"
    );
    assert!(
        validate.is_err(),
        "malformed account payload must fail validate"
    );
}

#[test]
fn storage_key_binary_rejects_wrong_tag_for_principal_payload() {
    let bytes = encode_text("aaaaa-aa");

    let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Principal);
    let validate = validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Principal);

    assert!(decode.is_err(), "principal text payload must fail decode");
    assert!(
        validate.is_err(),
        "principal text payload must fail validate"
    );
}

#[test]
fn storage_key_binary_rejects_wrong_size_subaccount_payload() {
    let bytes = encode_bytes(&[9_u8; 31]);

    let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Subaccount);
    let validate = validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Subaccount);

    assert!(decode.is_err(), "short subaccount payload must fail decode");
    assert!(
        validate.is_err(),
        "short subaccount payload must fail validate"
    );
}

#[test]
fn storage_key_binary_rejects_invalid_timestamp_and_ulid_payload() {
    let bad_timestamp = encode_bytes(&[7_u8; 7]);
    let bad_ulid = encode_bytes(&[9_u8; 15]);

    assert!(
        decode_storage_key_field_binary_bytes(bad_timestamp.as_slice(), FieldKind::Timestamp)
            .is_err(),
        "invalid timestamp payload must fail decode"
    );
    assert!(
        validate_storage_key_binary_value_bytes(bad_timestamp.as_slice(), FieldKind::Timestamp)
            .is_err(),
        "invalid timestamp payload must fail validate"
    );
    assert!(
        decode_storage_key_field_binary_bytes(bad_ulid.as_slice(), FieldKind::Ulid).is_err(),
        "invalid ulid payload must fail decode"
    );
    assert!(
        validate_storage_key_binary_value_bytes(bad_ulid.as_slice(), FieldKind::Ulid).is_err(),
        "invalid ulid payload must fail validate"
    );
}

#[test]
fn storage_key_binary_rejects_non_unit_unit_payload() {
    let bytes = encode_text("unit");
    let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Unit);
    let validate = validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Unit);

    assert!(decode.is_err(), "text unit payload must fail decode");
    assert!(validate.is_err(), "text unit payload must fail validate");
}

#[test]
fn storage_key_relation_decode_preserves_scalar_null_semantics() {
    let target = Ulid::from_u128(7);
    let target_bytes =
        encode_storage_key_binary_value_bytes(STRONG_RELATION_KIND, &Value::Ulid(target), "id")
            .expect("storage-key relation bytes should encode")
            .expect("relation kind should use storage-key binary lane");
    let null_bytes =
        encode_storage_key_binary_value_bytes(STRONG_RELATION_KIND, &Value::Null, "id")
            .expect("null relation bytes should encode")
            .expect("relation kind should use storage-key binary lane");

    let decoded =
        decode_relation_target_storage_keys_binary_bytes(&target_bytes, STRONG_RELATION_KIND)
            .expect("single relation should decode");
    let decoded_null =
        decode_relation_target_storage_keys_binary_bytes(&null_bytes, STRONG_RELATION_KIND)
            .expect("null relation should decode");

    assert_eq!(decoded, vec![StorageKey::Ulid(target)]);
    assert!(
        decoded_null.is_empty(),
        "null relation should yield no targets"
    );
}

#[test]
fn storage_key_relation_list_decode_skips_null_items() {
    let left = Ulid::from_u128(8);
    let right = Ulid::from_u128(9);
    let bytes = encode_storage_key_binary_value_bytes(
        STRONG_RELATION_LIST_KIND,
        &Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]),
        "ids",
    )
    .expect("relation list bytes should encode")
    .expect("relation list should use storage-key binary lane");

    let decoded =
        decode_relation_target_storage_keys_binary_bytes(&bytes, STRONG_RELATION_LIST_KIND)
            .expect("relation list should decode");

    assert_eq!(
        decoded,
        vec![StorageKey::Ulid(left), StorageKey::Ulid(right)]
    );
}

#[test]
fn storage_key_scalar_decoders_accept_supported_binary_shapes() {
    assert_eq!(
        decode_storage_key_field_binary_bytes(&encode_int64(-5), FieldKind::Int)
            .expect("int payload should decode"),
        StorageKey::Int(-5),
    );
    assert_eq!(
        decode_storage_key_field_binary_bytes(&encode_uint64(7), FieldKind::Uint)
            .expect("uint payload should decode"),
        StorageKey::Uint(7),
    );
    assert_eq!(
        decode_storage_key_field_binary_bytes(
            &encode_bytes(Principal::dummy(11).as_slice()),
            FieldKind::Principal,
        )
        .expect("principal payload should decode"),
        StorageKey::Principal(Principal::dummy(11)),
    );
    assert_eq!(
        decode_storage_key_field_binary_bytes(
            &encode_bytes(&Subaccount::from([4_u8; 32]).to_array()),
            FieldKind::Subaccount,
        )
        .expect("subaccount payload should decode"),
        StorageKey::Subaccount(Subaccount::from([4_u8; 32])),
    );
    assert_eq!(
        decode_storage_key_field_binary_bytes(
            &encode_int64(1_710_013_530_123),
            FieldKind::Timestamp,
        )
        .expect("timestamp payload should decode"),
        StorageKey::Timestamp(Timestamp::from_millis(1_710_013_530_123)),
    );
    assert_eq!(
        decode_storage_key_field_binary_bytes(
            &encode_bytes(Ulid::from_u128(77).to_bytes().as_slice()),
            FieldKind::Ulid,
        )
        .expect("ulid payload should decode"),
        StorageKey::Ulid(Ulid::from_u128(77)),
    );
    assert_eq!(
        decode_storage_key_field_binary_bytes(&encode_unit(), FieldKind::Unit)
            .expect("unit payload should decode"),
        StorageKey::Unit,
    );
}

#[test]
fn storage_key_relation_encode_binary_bytes_preserves_list_shape() {
    let left = StorageKey::Ulid(Ulid::from_u128(1));
    let right = StorageKey::Ulid(Ulid::from_u128(2));
    let encoded = encode_relation_target_storage_keys_binary_bytes(
        &[left, right],
        STRONG_RELATION_LIST_KIND,
        "relations",
    )
    .expect("relation list keys should encode");

    let decoded =
        decode_relation_target_storage_keys_binary_bytes(&encoded, STRONG_RELATION_LIST_KIND)
            .expect("relation list keys should decode");

    assert_eq!(decoded, vec![left, right]);
}

#[test]
fn storage_key_scalar_encode_roundtrips_supported_kinds() {
    let cases = vec![
        (FieldKind::Int, StorageKey::Int(-9)),
        (FieldKind::Uint, StorageKey::Uint(42)),
        (
            FieldKind::Principal,
            StorageKey::Principal(Principal::dummy(5)),
        ),
        (
            FieldKind::Subaccount,
            StorageKey::Subaccount(Subaccount::from([8_u8; 32])),
        ),
        (
            FieldKind::Timestamp,
            StorageKey::Timestamp(Timestamp::from_millis(1_710_013_530_123)),
        ),
        (FieldKind::Ulid, StorageKey::Ulid(Ulid::from_u128(77))),
        (FieldKind::Unit, StorageKey::Unit),
    ];

    for (kind, key) in cases {
        let encoded = encode_storage_key_field_binary_bytes(key, kind, "field")
            .expect("scalar key should encode");
        let decoded = decode_storage_key_field_binary_bytes(&encoded, kind)
            .expect("scalar key should decode");

        assert_eq!(decoded, key, "decoded key mismatch for {kind:?}");
    }
}
