use super::*;
use crate::{
    db::{
        KeyValueCodec, PrimaryKeyDecode, PrimaryKeyEncode,
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent},
    },
    error::{ErrorClass, ErrorOrigin},
    types::{Account, Principal, Subaccount, Timestamp, Ulid, Unit},
    value::Value,
};
use std::borrow::Cow;

fn max_width_data_store_key_fixture() -> DecodedDataStoreKey {
    let component = PrimaryKeyComponent::Account(Account::from_owner_and_subaccount(
        Principal::MAX,
        Some(Subaccount::MAX),
    ));
    let key = CompositePrimaryKeyValue::try_from_components(&[
        component, component, component, component,
    ])
    .expect("max-width composite primary key should build");

    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(u64::MAX),
        &PrimaryKeyValue::Composite(key),
    )
}

fn composite_data_key_fixture() -> DecodedDataStoreKey {
    let key = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
    ])
    .expect("composite primary key should build");

    DecodedDataStoreKey::new_primary_key_value(EntityTag::new(11), &PrimaryKeyValue::Composite(key))
}

fn assert_constructor_equivalence<K>(entity: EntityTag, key: K)
where
    K: KeyValueCodec + PrimaryKeyEncode + std::fmt::Debug,
{
    let typed =
        DecodedDataStoreKey::try_from_typed_key(entity, &key).expect("typed key should encode");
    let structural = DecodedDataStoreKey::try_from_structural_key(entity, &key.to_key_value())
        .expect("structural key should encode");

    assert_eq!(
        typed, structural,
        "typed and structural data-key constructors must stay equivalent for {key:?}",
    );
}

fn assert_structural_dedup_matches_typed_dedup<K>(entity: EntityTag, keys: Vec<K>)
where
    K: Clone + KeyValueCodec + PrimaryKeyEncode + Ord + std::fmt::Debug,
{
    let mut typed_keys = keys.clone();
    typed_keys.sort();
    typed_keys.dedup();

    let mut typed_data_keys = typed_keys
        .iter()
        .map(|key| {
            DecodedDataStoreKey::try_from_typed_key(entity, key).expect("typed key should encode")
        })
        .collect::<Vec<_>>();
    typed_data_keys.sort();
    typed_data_keys.dedup();

    let mut structural_data_keys = keys
        .iter()
        .map(KeyValueCodec::to_key_value)
        .map(|key| {
            DecodedDataStoreKey::try_from_structural_key(entity, &key)
                .expect("structural key should encode")
        })
        .collect::<Vec<_>>();
    structural_data_keys.sort();
    structural_data_keys.dedup();

    assert_eq!(
        structural_data_keys, typed_data_keys,
        "structural DecodedDataStoreKey dedup must match typed-key dedup semantics",
    );
}

fn assert_primary_key_roundtrip<K>(key: K)
where
    K: Copy + Eq + std::fmt::Debug + PrimaryKeyEncode + PrimaryKeyDecode,
{
    let primary_key_value = key.to_primary_key_value().expect("typed key should encode");
    let decoded = K::from_primary_key_value(&primary_key_value).expect("primary key should decode");

    assert_eq!(decoded, key);
}

fn assert_key_value_roundtrip<K>(key: K)
where
    K: Eq + KeyValueCodec + std::fmt::Debug,
{
    let value = key.to_key_value();
    let decoded = K::from_key_value(&value).expect("runtime key value should decode");

    assert_eq!(decoded, key);
}

fn composite_value_list_fixture() -> Value {
    Value::List(vec![
        Value::Nat64(7),
        Value::Ulid(Ulid::from_u128(42)),
        Value::Principal(Principal::from_slice(&[1, 2, 3])),
    ])
}

fn composite_primary_key_value_fixture() -> PrimaryKeyValue {
    let composite = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Ulid(Ulid::from_u128(42)),
        PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
    ])
    .expect("composite fixture should build");

    PrimaryKeyValue::Composite(composite)
}

const fn scalar_key(component: PrimaryKeyComponent) -> PrimaryKeyValue {
    PrimaryKeyValue::Scalar(component)
}

fn taxonomy_range_contains_raw_key(range: &RawDataStoreKeyRange, key: &RawDataStoreKey) -> bool {
    key.as_bytes() >= range.lower_inclusive()
        && range
            .upper_exclusive()
            .is_none_or(|upper| key.as_bytes() < upper)
}

#[test]
fn data_key_max_width_fixture_uses_max_compact_size() {
    let data_key = max_width_data_store_key_fixture();
    let size = data_key.to_raw().unwrap().as_bytes().len();
    assert_eq!(size, RawDataStoreKey::MAX_STORED_SIZE_USIZE);
}

#[test]
fn data_key_golden_snapshot_entity_and_compact_primary_key_layout_is_stable() {
    let key = DecodedDataStoreKey::new(
        EntityTag::new(5),
        &scalar_key(PrimaryKeyComponent::Int64(-1)),
    );
    let raw = key.to_raw().expect("data key should encode");

    // Freeze the 0.159 on-disk wire contract:
    // [EntityTag(u64, big-endian)] + [EncodedPrimaryKey].
    let mut expected = Vec::new();
    expected.extend_from_slice(&5u64.to_be_bytes());
    expected.push(
        crate::db::key_taxonomy::PrimaryKeyComponent::Int64(-1)
            .kind()
            .tag(),
    );
    expected.extend_from_slice(&0x7FFF_FFFF_FFFF_FFFFu64.to_be_bytes());

    assert_eq!(raw.as_bytes(), expected.as_slice());
}

#[test]
fn data_key_ordering_matches_bytes() {
    let keys = vec![
        DecodedDataStoreKey::new(
            EntityTag::new(1),
            &scalar_key(PrimaryKeyComponent::Int64(0)),
        ),
        DecodedDataStoreKey::new(
            EntityTag::new(1),
            &scalar_key(PrimaryKeyComponent::Int64(0)),
        ),
        DecodedDataStoreKey::new(
            EntityTag::new(2),
            &scalar_key(PrimaryKeyComponent::Int64(0)),
        ),
        DecodedDataStoreKey::new(
            EntityTag::new(1),
            &scalar_key(PrimaryKeyComponent::Nat64(1)),
        ),
        composite_data_key_fixture(),
    ];

    let mut by_ord = keys.clone();
    by_ord.sort();

    let mut by_bytes = keys;
    by_bytes.sort_by(|a, b| {
        a.to_raw()
            .unwrap()
            .as_bytes()
            .cmp(b.to_raw().unwrap().as_bytes())
    });

    assert_eq!(by_ord, by_bytes);
}

#[test]
fn data_key_roundtrips_composite_primary_key_from_raw_bytes() {
    let key = composite_data_key_fixture();
    let raw = key.to_raw().expect("composite data key should encode");
    let decoded =
        DecodedDataStoreKey::try_from_raw(&raw).expect("composite data key should decode");

    assert_eq!(decoded.entity_tag(), key.entity_tag());
    assert_eq!(decoded.key, key.key);
    assert_eq!(decoded.to_raw().unwrap().as_bytes(), raw.as_bytes());
}

#[test]
fn data_key_primary_key_runtime_value_projects_composite_primary_key() {
    let value = composite_data_key_fixture().primary_key_runtime_value();

    assert_eq!(
        value,
        Value::List(vec![
            Value::Nat64(7),
            Value::Principal(Principal::from_slice(&[1, 2, 3])),
        ]),
    );
}

#[test]
fn data_key_primary_key_component_runtime_value_projects_composite_components() {
    let key = composite_data_key_fixture();

    assert_eq!(
        key.primary_key_component_runtime_value(0)
            .expect("first composite component should project"),
        Value::Nat64(7),
    );
    assert_eq!(
        key.primary_key_component_runtime_value(1)
            .expect("second composite component should project"),
        Value::Principal(Principal::from_slice(&[1, 2, 3])),
    );
    assert!(
        key.primary_key_component_runtime_value(2).is_err(),
        "out-of-bounds composite component projection should fail closed",
    );
}

#[test]
fn data_key_structural_constructor_matches_typed_constructor() {
    let entity = EntityTag::new(17);

    assert_constructor_equivalence(entity, -42_i64);
    assert_constructor_equivalence(entity, i128::MIN);
    assert_constructor_equivalence(entity, 42_u64);
    assert_constructor_equivalence(entity, u128::MAX);
    assert_constructor_equivalence(entity, Principal::from_slice(&[1, 2, 3, 4]));
    assert_constructor_equivalence(entity, Subaccount::from_array([7; 32]));
    assert_constructor_equivalence(entity, Timestamp::from_millis(1_710_013_530_123));
    assert_constructor_equivalence(entity, Ulid::from_u128(42));
    assert_constructor_equivalence(
        entity,
        Account::from_owner_and_subaccount(
            Principal::from_slice(&[9, 8, 7]),
            Some(Subaccount::from_array([5; 32])),
        ),
    );
    assert_constructor_equivalence(entity, Unit);
    assert_constructor_equivalence(entity, ());
}

#[test]
fn data_key_structural_constructor_accepts_composite_value_list() {
    let entity = EntityTag::new(19);
    let expected =
        DecodedDataStoreKey::new_primary_key_value(entity, &composite_primary_key_value_fixture());
    let structural =
        DecodedDataStoreKey::try_from_structural_key(entity, &composite_value_list_fixture())
            .expect("composite value-list key should encode");

    assert_eq!(structural, expected);
    assert_eq!(
        structural.to_raw().unwrap().as_bytes(),
        expected.to_raw().unwrap().as_bytes(),
    );
}

#[test]
fn data_key_structural_constructor_rejects_malformed_composite_value_lists() {
    let entity = EntityTag::new(21);
    let malformed = [
        Value::List(vec![]),
        Value::List(vec![Value::Nat64(1)]),
        Value::List(vec![Value::Nat64(1), Value::Unit]),
        Value::List(vec![Value::Nat64(1), Value::List(vec![Value::Nat64(2)])]),
    ];

    for value in malformed {
        let err = DecodedDataStoreKey::try_from_structural_key(entity, &value)
            .expect_err("malformed composite value-list key should reject");

        assert_eq!(err.class(), ErrorClass::Unsupported);
    }
}

#[test]
fn primary_key_decode_roundtrips_supported_typed_keys() {
    assert_primary_key_roundtrip(-42_i8);
    assert_primary_key_roundtrip(-43_i16);
    assert_primary_key_roundtrip(-44_i32);
    assert_primary_key_roundtrip(-45_i64);
    assert_primary_key_roundtrip(i128::MIN);
    assert_primary_key_roundtrip(42_u8);
    assert_primary_key_roundtrip(43_u16);
    assert_primary_key_roundtrip(44_u32);
    assert_primary_key_roundtrip(45_u64);
    assert_primary_key_roundtrip(u128::MAX);
    assert_primary_key_roundtrip(Principal::from_slice(&[1, 2, 3, 4]));
    assert_primary_key_roundtrip(Subaccount::from_array([7; 32]));
    assert_primary_key_roundtrip(Timestamp::from_millis(1_710_013_530_123));
    assert_primary_key_roundtrip(Ulid::from_u128(42));
    assert_primary_key_roundtrip(Account::from_owner_and_subaccount(
        Principal::from_slice(&[9, 8, 7]),
        Some(Subaccount::from_array([5; 32])),
    ));
    assert_primary_key_roundtrip(Unit);
    assert_primary_key_roundtrip(());
}

#[test]
fn key_value_codec_roundtrips_every_supported_scalar_key() {
    assert_key_value_roundtrip(-42_i8);
    assert_key_value_roundtrip(-43_i16);
    assert_key_value_roundtrip(-44_i32);
    assert_key_value_roundtrip(-45_i64);
    assert_key_value_roundtrip(i128::MIN);
    assert_key_value_roundtrip(42_u8);
    assert_key_value_roundtrip(43_u16);
    assert_key_value_roundtrip(44_u32);
    assert_key_value_roundtrip(45_u64);
    assert_key_value_roundtrip(u128::MAX);
    assert_key_value_roundtrip(Principal::from_slice(&[1, 2, 3, 4]));
    assert_key_value_roundtrip(Subaccount::from_array([7; 32]));
    assert_key_value_roundtrip(Timestamp::from_millis(1_710_013_530_123));
    assert_key_value_roundtrip(Ulid::from_u128(42));
    assert_key_value_roundtrip(Account::from_owner_and_subaccount(
        Principal::from_slice(&[9, 8, 7]),
        Some(Subaccount::from_array([5; 32])),
    ));
    assert_key_value_roundtrip(Unit);
    assert_key_value_roundtrip(());
}

#[test]
fn primary_key_decode_rejects_variant_mismatch_and_out_of_range_keys() {
    let variant_err = u64::from_primary_key_value(&PrimaryKeyComponent::Int64(7).into())
        .expect_err("nat decode must reject signed storage-key variants");
    let range_err = u8::from_primary_key_value(&PrimaryKeyComponent::Nat64(300).into())
        .expect_err("narrow integer decode must reject out-of-range values");

    assert_eq!(variant_err.class(), ErrorClass::Corruption);
    assert_eq!(variant_err.origin(), ErrorOrigin::Store);
    assert_eq!(range_err.class(), ErrorClass::Corruption);
    assert_eq!(range_err.origin(), ErrorOrigin::Store);
}

#[test]
fn data_key_constructors_reject_non_primary_key_values_consistently() {
    let entity = EntityTag::new(23);
    let unsupported_values = [
        Value::Text("not-a-primary-key".to_string()),
        Value::Bool(true),
        Value::List(vec![Value::Nat64(1)]),
        Value::Null,
    ];

    for value in unsupported_values {
        let structural_err = DecodedDataStoreKey::try_from_structural_key(entity, &value)
            .expect_err("structural constructor must reject non-primary-key values");

        assert_eq!(structural_err.class(), ErrorClass::Unsupported);
    }
}

#[test]
fn data_key_raw_prefix_bounds_cover_supported_structural_key_domain() {
    let entity = EntityTag::new(29);
    let range = RawDataStoreKeyRange::entity_prefix(entity);
    let supported_values = [
        Value::Account(Account::from_owner_and_subaccount(
            Principal::from_slice(&[3, 1, 4]),
            Some(Subaccount::from_array([1; 32])),
        )),
        Value::Int64(-17),
        Value::Principal(Principal::from_slice(&[1, 2, 3])),
        Value::Subaccount(Subaccount::from_array([2; 32])),
        Value::Timestamp(Timestamp::from_secs(7)),
        Value::Nat64(42),
        Value::Ulid(Ulid::from_u128(99)),
        Value::Unit,
    ];

    assert_eq!(
        range.lower_inclusive(),
        entity.value().to_be_bytes().as_slice()
    );
    assert_eq!(
        range.upper_exclusive().expect("ordinary entity has upper"),
        (entity.value() + 1).to_be_bytes().as_slice(),
    );

    for value in supported_values {
        let data_key = DecodedDataStoreKey::try_from_structural_key(entity, &value)
            .expect("supported structural key should encode");
        let raw_key = data_key.to_raw().expect("supported key should encode");
        assert!(
            taxonomy_range_contains_raw_key(&range, &raw_key),
            "supported structural key {value:?} must stay within entity bounds",
        );
    }
}

#[test]
fn data_key_structural_dedup_matches_typed_key_dedup() {
    let entity = EntityTag::new(31);

    assert_structural_dedup_matches_typed_dedup(entity, vec![7_u64, 1, 7, 3, 1, 9]);
    assert_structural_dedup_matches_typed_dedup(
        entity,
        vec![
            Ulid::from_u128(9),
            Ulid::from_u128(1),
            Ulid::from_u128(9),
            Ulid::from_u128(2),
            Ulid::from_u128(1),
        ],
    );
}

#[test]
fn data_key_entity_tag_roundtrip_is_big_endian() {
    let mut raw_bytes = max_width_data_store_key_fixture()
        .to_raw()
        .unwrap()
        .into_bytes();
    raw_bytes[..RawDataStoreKey::ENTITY_TAG_SIZE_USIZE]
        .copy_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
    let raw = RawDataStoreKey::from_persisted_bytes(raw_bytes);
    let decoded = DecodedDataStoreKey::try_from_raw(&raw).expect("entity tag bytes should decode");
    assert_eq!(decoded.entity_tag().value(), 0x0102_0304_0506_0708u64);
}

#[test]
fn data_key_rejects_corrupt_key() {
    let mut raw_bytes = max_width_data_store_key_fixture()
        .to_raw()
        .unwrap()
        .into_bytes();
    let off = RawDataStoreKey::ENTITY_TAG_SIZE_USIZE;
    raw_bytes[off] = 0xFF;
    let raw = RawDataStoreKey::from_persisted_bytes(raw_bytes);
    assert!(DecodedDataStoreKey::try_from_raw(&raw).is_err());
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn data_key_fuzz_roundtrip_is_canonical() {
    let mut seed = 0xDEAD_BEEF_u64;

    for _ in 0..1_000 {
        let mut bytes = [0u8; RawDataStoreKey::MAX_STORED_SIZE_USIZE];
        for b in &mut bytes {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *b = (seed >> 24) as u8;
        }

        let raw = RawDataStoreKey::from_persisted_bytes(bytes.to_vec());
        if let Ok(decoded) = DecodedDataStoreKey::try_from_raw(&raw) {
            let re = decoded.to_raw().unwrap();
            assert_eq!(raw.as_bytes(), re.as_bytes());
        }
    }
}

#[test]
fn raw_data_store_key_storable_roundtrip() {
    let key = max_width_data_store_key_fixture().to_raw().unwrap();
    let bytes = key.to_bytes();
    let decoded = <RawDataStoreKey as Storable>::from_bytes(Cow::Borrowed(&bytes));
    assert_eq!(key, decoded);
}

#[test]
fn raw_data_store_key_from_bytes_wrong_length_fails_closed() {
    let decoded = RawDataStoreKey::from_persisted_bytes(vec![1u8, 2u8, 3u8]);

    assert!(
        DecodedDataStoreKey::try_from_raw(&decoded).is_err(),
        "wrong-length raw bytes must not decode into a valid DecodedDataStoreKey"
    );
}

#[test]
fn data_key_raw_entity_prefix_range_contains_only_matching_entity() {
    let entity = EntityTag::new(41);
    let range = RawDataStoreKeyRange::entity_prefix(entity);
    let matching = DecodedDataStoreKey::new(entity, &scalar_key(PrimaryKeyComponent::Nat64(1)))
        .to_raw()
        .expect("matching key should encode");
    let previous =
        DecodedDataStoreKey::new(EntityTag::new(40), &scalar_key(PrimaryKeyComponent::Unit))
            .to_raw()
            .expect("previous key should encode");
    let next = DecodedDataStoreKey::new(
        EntityTag::new(42),
        &scalar_key(PrimaryKeyComponent::Nat64(0)),
    )
    .to_raw()
    .expect("next key should encode");

    assert!(taxonomy_range_contains_raw_key(&range, &matching));
    assert!(!taxonomy_range_contains_raw_key(&range, &previous));
    assert!(!taxonomy_range_contains_raw_key(&range, &next));
}
