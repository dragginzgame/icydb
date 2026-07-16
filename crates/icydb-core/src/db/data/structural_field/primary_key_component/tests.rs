use crate::{
    db::data::structural_field::primary_key_component::{
        decode::decode_relation_target_primary_key_components_binary_bytes,
        decode::{
            decode_accepted_relation_target_primary_key_components_binary_bytes,
            decode_primary_key_component_binary_value_bytes,
            decode_primary_key_component_field_binary_bytes,
        },
        encode::encode_relation_target_primary_key_components_binary_bytes,
        encode::{
            encode_primary_key_component_binary_value_bytes,
            encode_primary_key_component_field_binary_bytes,
        },
        validate_primary_key_component_binary_value_bytes,
    },
    db::key_taxonomy::PrimaryKeyComponent,
    db::schema::{AcceptedFieldKind, AcceptedRelationEnforcement},
    model::field::{FieldKind, RelationEnforcement},
    types::{Account, EntityTag, Principal, Subaccount, Timestamp, Ulid},
    value::Value,
};

static RELATION_ULID_KEY_KIND: FieldKind = FieldKind::Ulid;
static RELATION_INT128_KEY_KIND: FieldKind = FieldKind::Int128;
static RELATION_NAT128_KEY_KIND: FieldKind = FieldKind::Nat128;
static STRONG_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: "RelationTargetEntity",
    target_entity_name: "RelationTargetEntity",
    target_entity_tag: EntityTag::new(7),
    target_store_path: "RelationTargetStore",
    key_kind: &RELATION_ULID_KEY_KIND,
    enforcement: RelationEnforcement::Enforced,
};
static STRONG_RELATION_LIST_KIND: FieldKind = FieldKind::List(&STRONG_RELATION_KIND);
static STRONG_INT128_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: "RelationTargetEntity",
    target_entity_name: "RelationTargetEntity",
    target_entity_tag: EntityTag::new(7),
    target_store_path: "RelationTargetStore",
    key_kind: &RELATION_INT128_KEY_KIND,
    enforcement: RelationEnforcement::Enforced,
};
static STRONG_NAT128_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: "RelationTargetEntity",
    target_entity_name: "RelationTargetEntity",
    target_entity_tag: EntityTag::new(7),
    target_store_path: "RelationTargetStore",
    key_kind: &RELATION_NAT128_KEY_KIND,
    enforcement: RelationEnforcement::Enforced,
};

const TAG_UNIT: u8 = 0x01;
const TAG_NAT64: u8 = 0x10;
const TAG_INT64: u8 = 0x11;
const TAG_TEXT: u8 = 0x12;
const TAG_BYTES: u8 = 0x13;
const TAG_LIST: u8 = 0x20;

fn encode_unit() -> Vec<u8> {
    vec![TAG_UNIT]
}

fn encode_nat64(value: u64) -> Vec<u8> {
    let mut out = vec![TAG_NAT64];
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
fn primary_key_component_binary_roundtrips_all_supported_scalar_kinds() {
    let account = Account::from_owner_and_subaccount(
        Principal::from_slice(&[3]),
        Some(Subaccount::from_array([3_u8; 32])),
    );
    let timestamp = Timestamp::from_millis(1_710_013_530_123);
    let ulid = Ulid::from_u128(77);
    let cases = vec![
        (
            FieldKind::Account,
            PrimaryKeyComponent::Account(account),
            Value::Account(account),
        ),
        (
            FieldKind::Int64,
            PrimaryKeyComponent::Int64(-9),
            Value::Int64(-9),
        ),
        (
            FieldKind::Int128,
            PrimaryKeyComponent::Int128(i128::MIN + 7),
            Value::Int128(i128::MIN + 7),
        ),
        (
            FieldKind::Principal,
            PrimaryKeyComponent::Principal(Principal::from_slice(&[5])),
            Value::Principal(Principal::from_slice(&[5])),
        ),
        (
            FieldKind::Subaccount,
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([8_u8; 32])),
            Value::Subaccount(Subaccount::from_array([8_u8; 32])),
        ),
        (
            FieldKind::Timestamp,
            PrimaryKeyComponent::Timestamp(timestamp),
            Value::Timestamp(timestamp),
        ),
        (
            FieldKind::Nat64,
            PrimaryKeyComponent::Nat64(42),
            Value::Nat64(42),
        ),
        (
            FieldKind::Nat128,
            PrimaryKeyComponent::Nat128(u128::MAX - 7),
            Value::Nat128(u128::MAX - 7),
        ),
        (
            FieldKind::Ulid,
            PrimaryKeyComponent::Ulid(ulid),
            Value::Ulid(ulid),
        ),
        (FieldKind::Unit, PrimaryKeyComponent::Unit, Value::Unit),
    ];

    for (kind, key, value) in cases {
        let encoded = encode_primary_key_component_field_binary_bytes(key, kind, "field")
            .expect("primary-key component payload should encode");
        let decoded_key = decode_primary_key_component_field_binary_bytes(encoded.as_slice(), kind)
            .expect("primary-key component payload should decode");
        let decoded_value =
            decode_primary_key_component_binary_value_bytes(encoded.as_slice(), kind)
                .expect("primary-key component value decode should succeed")
                .expect("supported kind should stay on the primary-key-component lane");

        assert!(
            validate_primary_key_component_binary_value_bytes(encoded.as_slice(), kind)
                .expect("primary-key component payload should validate"),
            "supported primary-key component kind should validate as component-owned"
        );
        assert_eq!(decoded_key, key, "decoded key mismatch for {kind:?}");
        assert_eq!(decoded_value, value, "decoded value mismatch for {kind:?}");
    }
}

#[test]
fn primary_key_component_binary_roundtrips_128_bit_relation_payloads() {
    let int_key = i128::MIN + 123;
    let nat_key = u128::MAX - 123;

    let encoded_int = encode_primary_key_component_binary_value_bytes(
        STRONG_INT128_RELATION_KIND,
        &Value::Int128(int_key),
        "int_relation",
    )
    .expect("int128 relation should encode")
    .expect("int128 relation kind should stay on primary-key component lane");
    let encoded_nat = encode_primary_key_component_binary_value_bytes(
        STRONG_NAT128_RELATION_KIND,
        &Value::Nat128(nat_key),
        "nat_relation",
    )
    .expect("nat128 relation should encode")
    .expect("nat128 relation kind should stay on primary-key component lane");

    assert_eq!(
        decode_relation_target_primary_key_components_binary_bytes(
            encoded_int.as_slice(),
            STRONG_INT128_RELATION_KIND
        )
        .expect("int128 relation target key should decode"),
        vec![PrimaryKeyComponent::Int128(int_key)],
    );
    assert_eq!(
        decode_relation_target_primary_key_components_binary_bytes(
            encoded_nat.as_slice(),
            STRONG_NAT128_RELATION_KIND
        )
        .expect("nat128 relation target key should decode"),
        vec![PrimaryKeyComponent::Nat128(nat_key)],
    );

    let accepted_int_kind = AcceptedFieldKind::Relation {
        target_path: "RelationTargetEntity".to_string(),
        target_entity_name: "RelationTargetEntity".to_string(),
        target_entity_tag: EntityTag::new(7),
        target_store_path: "RelationTargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Int128),
        enforcement: AcceptedRelationEnforcement::Enforced,
    };
    let accepted_nat_kind = AcceptedFieldKind::Relation {
        target_path: "RelationTargetEntity".to_string(),
        target_entity_name: "RelationTargetEntity".to_string(),
        target_entity_tag: EntityTag::new(7),
        target_store_path: "RelationTargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Nat128),
        enforcement: AcceptedRelationEnforcement::Enforced,
    };

    assert_eq!(
        decode_accepted_relation_target_primary_key_components_binary_bytes(
            encoded_int.as_slice(),
            &accepted_int_kind,
        )
        .expect("accepted int128 relation target key should decode"),
        vec![PrimaryKeyComponent::Int128(int_key)],
    );
    assert_eq!(
        decode_accepted_relation_target_primary_key_components_binary_bytes(
            encoded_nat.as_slice(),
            &accepted_nat_kind,
        )
        .expect("accepted nat128 relation target key should decode"),
        vec![PrimaryKeyComponent::Nat128(nat_key)],
    );
}

#[test]
fn primary_key_component_binary_roundtrips_relation_payloads() {
    let left = Ulid::from_u128(100);
    let right = Ulid::from_u128(200);
    let single = encode_primary_key_component_binary_value_bytes(
        STRONG_RELATION_KIND,
        &Value::Ulid(left),
        "relation",
    )
    .expect("single relation should encode")
    .expect("relation kind should stay on primary-key-component lane");
    let many = encode_primary_key_component_binary_value_bytes(
        STRONG_RELATION_LIST_KIND,
        &Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]),
        "relations",
    )
    .expect("many relation should encode")
    .expect("relation list kind should stay on primary-key-component lane");

    assert_eq!(
        decode_primary_key_component_binary_value_bytes(single.as_slice(), STRONG_RELATION_KIND)
            .expect("single relation should decode")
            .expect("single relation should be primary-key-component-owned"),
        Value::Ulid(left),
    );
    assert_eq!(
        decode_relation_target_primary_key_components_binary_bytes(
            single.as_slice(),
            STRONG_RELATION_KIND
        )
        .expect("single relation target keys should decode"),
        vec![PrimaryKeyComponent::Ulid(left)],
    );
    assert_eq!(
        decode_primary_key_component_binary_value_bytes(many.as_slice(), STRONG_RELATION_LIST_KIND)
            .expect("many relation should decode")
            .expect("relation list should be primary-key-component-owned"),
        Value::List(vec![Value::Ulid(left), Value::Ulid(right)]),
    );
    assert_eq!(
        decode_relation_target_primary_key_components_binary_bytes(
            many.as_slice(),
            STRONG_RELATION_LIST_KIND
        )
        .expect("many relation target keys should decode"),
        vec![
            PrimaryKeyComponent::Ulid(left),
            PrimaryKeyComponent::Ulid(right)
        ],
    );
}

#[test]
fn primary_key_component_binary_rejects_malformed_account_payload() {
    let bytes = encode_list(&[encode_bytes(Principal::from_slice(&[1]).as_slice())]);

    let decode =
        decode_primary_key_component_field_binary_bytes(bytes.as_slice(), FieldKind::Account);
    let validate =
        validate_primary_key_component_binary_value_bytes(bytes.as_slice(), FieldKind::Account);

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
fn primary_key_component_binary_rejects_wrong_tag_for_principal_payload() {
    let bytes = encode_text("aaaaa-aa");

    let decode =
        decode_primary_key_component_field_binary_bytes(bytes.as_slice(), FieldKind::Principal);
    let validate =
        validate_primary_key_component_binary_value_bytes(bytes.as_slice(), FieldKind::Principal);

    assert!(decode.is_err(), "principal text payload must fail decode");
    assert!(
        validate.is_err(),
        "principal text payload must fail validate"
    );
}

#[test]
fn primary_key_component_binary_rejects_wrong_size_subaccount_payload() {
    let bytes = encode_bytes(&[9_u8; 31]);

    let decode =
        decode_primary_key_component_field_binary_bytes(bytes.as_slice(), FieldKind::Subaccount);
    let validate =
        validate_primary_key_component_binary_value_bytes(bytes.as_slice(), FieldKind::Subaccount);

    assert!(decode.is_err(), "short subaccount payload must fail decode");
    assert!(
        validate.is_err(),
        "short subaccount payload must fail validate"
    );
}

#[test]
fn primary_key_component_binary_rejects_invalid_timestamp_and_ulid_payload() {
    let bad_timestamp = encode_bytes(&[7_u8; 7]);
    let bad_ulid = encode_bytes(&[9_u8; 15]);

    assert!(
        decode_primary_key_component_field_binary_bytes(
            bad_timestamp.as_slice(),
            FieldKind::Timestamp
        )
        .is_err(),
        "invalid timestamp payload must fail decode"
    );
    assert!(
        validate_primary_key_component_binary_value_bytes(
            bad_timestamp.as_slice(),
            FieldKind::Timestamp
        )
        .is_err(),
        "invalid timestamp payload must fail validate"
    );
    assert!(
        decode_primary_key_component_field_binary_bytes(bad_ulid.as_slice(), FieldKind::Ulid)
            .is_err(),
        "invalid ulid payload must fail decode"
    );
    assert!(
        validate_primary_key_component_binary_value_bytes(bad_ulid.as_slice(), FieldKind::Ulid)
            .is_err(),
        "invalid ulid payload must fail validate"
    );
}

#[test]
fn primary_key_component_binary_rejects_non_unit_unit_payload() {
    let bytes = encode_text("unit");
    let decode = decode_primary_key_component_field_binary_bytes(bytes.as_slice(), FieldKind::Unit);
    let validate =
        validate_primary_key_component_binary_value_bytes(bytes.as_slice(), FieldKind::Unit);

    assert!(decode.is_err(), "text unit payload must fail decode");
    assert!(validate.is_err(), "text unit payload must fail validate");
}

#[test]
fn primary_key_component_relation_decode_preserves_scalar_null_semantics() {
    let target = Ulid::from_u128(7);
    let target_bytes = encode_primary_key_component_binary_value_bytes(
        STRONG_RELATION_KIND,
        &Value::Ulid(target),
        "id",
    )
    .expect("relation primary-key component bytes should encode")
    .expect("relation kind should use primary-key component binary lane");
    let null_bytes =
        encode_primary_key_component_binary_value_bytes(STRONG_RELATION_KIND, &Value::Null, "id")
            .expect("null relation bytes should encode")
            .expect("relation kind should use primary-key component binary lane");

    let decoded = decode_relation_target_primary_key_components_binary_bytes(
        &target_bytes,
        STRONG_RELATION_KIND,
    )
    .expect("single relation should decode");
    let decoded_null = decode_relation_target_primary_key_components_binary_bytes(
        &null_bytes,
        STRONG_RELATION_KIND,
    )
    .expect("null relation should decode");

    assert_eq!(decoded, vec![PrimaryKeyComponent::Ulid(target)]);
    assert!(
        decoded_null.is_empty(),
        "null relation should yield no targets"
    );
}

#[test]
fn primary_key_component_relation_list_decode_skips_null_items() {
    let left = Ulid::from_u128(8);
    let right = Ulid::from_u128(9);
    let bytes = encode_primary_key_component_binary_value_bytes(
        STRONG_RELATION_LIST_KIND,
        &Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]),
        "ids",
    )
    .expect("relation list bytes should encode")
    .expect("relation list should use primary-key component binary lane");

    let decoded = decode_relation_target_primary_key_components_binary_bytes(
        &bytes,
        STRONG_RELATION_LIST_KIND,
    )
    .expect("relation list should decode");

    assert_eq!(
        decoded,
        vec![
            PrimaryKeyComponent::Ulid(left),
            PrimaryKeyComponent::Ulid(right)
        ]
    );
}

#[test]
fn primary_key_component_scalar_decoders_accept_supported_binary_shapes() {
    assert_eq!(
        decode_primary_key_component_field_binary_bytes(&encode_int64(-5), FieldKind::Int64)
            .expect("int payload should decode"),
        PrimaryKeyComponent::Int64(-5),
    );
    assert_eq!(
        decode_primary_key_component_field_binary_bytes(&encode_nat64(7), FieldKind::Nat64)
            .expect("nat payload should decode"),
        PrimaryKeyComponent::Nat64(7),
    );
    assert_eq!(
        decode_primary_key_component_field_binary_bytes(
            &encode_bytes(Principal::from_slice(&[11]).as_slice()),
            FieldKind::Principal,
        )
        .expect("principal payload should decode"),
        PrimaryKeyComponent::Principal(Principal::from_slice(&[11])),
    );
    assert_eq!(
        decode_primary_key_component_field_binary_bytes(
            &encode_bytes(&Subaccount::from_array([4_u8; 32]).to_array()),
            FieldKind::Subaccount,
        )
        .expect("subaccount payload should decode"),
        PrimaryKeyComponent::Subaccount(Subaccount::from_array([4_u8; 32])),
    );
    assert_eq!(
        decode_primary_key_component_field_binary_bytes(
            &encode_int64(1_710_013_530_123),
            FieldKind::Timestamp,
        )
        .expect("timestamp payload should decode"),
        PrimaryKeyComponent::Timestamp(Timestamp::from_millis(1_710_013_530_123)),
    );
    assert_eq!(
        decode_primary_key_component_field_binary_bytes(
            &encode_bytes(Ulid::from_u128(77).to_bytes().as_slice()),
            FieldKind::Ulid,
        )
        .expect("ulid payload should decode"),
        PrimaryKeyComponent::Ulid(Ulid::from_u128(77)),
    );
    assert_eq!(
        decode_primary_key_component_field_binary_bytes(&encode_unit(), FieldKind::Unit)
            .expect("unit payload should decode"),
        PrimaryKeyComponent::Unit,
    );
}

#[test]
fn primary_key_component_relation_encode_binary_bytes_preserves_list_shape() {
    let left = PrimaryKeyComponent::Ulid(Ulid::from_u128(1));
    let right = PrimaryKeyComponent::Ulid(Ulid::from_u128(2));
    let encoded = encode_relation_target_primary_key_components_binary_bytes(
        &[left, right],
        STRONG_RELATION_LIST_KIND,
        "relations",
    )
    .expect("relation list keys should encode");

    let decoded = decode_relation_target_primary_key_components_binary_bytes(
        &encoded,
        STRONG_RELATION_LIST_KIND,
    )
    .expect("relation list keys should decode");

    assert_eq!(decoded, vec![left, right]);
}

#[test]
fn primary_key_component_scalar_encode_roundtrips_supported_kinds() {
    let cases = vec![
        (FieldKind::Int64, PrimaryKeyComponent::Int64(-9)),
        (FieldKind::Nat64, PrimaryKeyComponent::Nat64(42)),
        (
            FieldKind::Principal,
            PrimaryKeyComponent::Principal(Principal::from_slice(&[5])),
        ),
        (
            FieldKind::Subaccount,
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([8_u8; 32])),
        ),
        (
            FieldKind::Timestamp,
            PrimaryKeyComponent::Timestamp(Timestamp::from_millis(1_710_013_530_123)),
        ),
        (
            FieldKind::Ulid,
            PrimaryKeyComponent::Ulid(Ulid::from_u128(77)),
        ),
        (FieldKind::Unit, PrimaryKeyComponent::Unit),
    ];

    for (kind, key) in cases {
        let encoded = encode_primary_key_component_field_binary_bytes(key, kind, "field")
            .expect("scalar key should encode");
        let decoded = decode_primary_key_component_field_binary_bytes(&encoded, kind)
            .expect("scalar key should decode");

        assert_eq!(decoded, key, "decoded key mismatch for {kind:?}");
    }
}
