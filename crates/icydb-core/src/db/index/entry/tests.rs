use super::{
    IndexEntryCorruption, IndexEntryExistenceWitness, IndexEntryValue, MAX_INDEX_ENTRY_BYTES,
};
use crate::{
    db::{
        index::{IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey},
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
    },
    types::{EntityTag, Principal},
};
use ic_stable_structures::Storable;
use std::borrow::Cow;

fn raw_key_for(key: PrimaryKeyComponent) -> RawIndexStoreKey {
    let component = vec![0x42];
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(EntityTag::new(0x159), 1),
        IndexKeyKind::User,
        std::slice::from_ref(&component),
        &PrimaryKeyValue::from(key),
    )
    .expect("test index key should build")
    .to_raw()
    .expect("test index key should encode")
}

fn raw_key_for_primary_key_value(key: &PrimaryKeyValue) -> RawIndexStoreKey {
    let component = vec![0x42];
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(EntityTag::new(0x159), 1),
        IndexKeyKind::User,
        std::slice::from_ref(&component),
        key,
    )
    .expect("test index key should build")
    .to_raw()
    .expect("test index key should encode")
}

#[test]
fn index_entry_value_round_trip() {
    let key = PrimaryKeyComponent::Int64(1);
    let raw_key = raw_key_for(key);

    let raw = IndexEntryValue::presence();
    let decoded = raw
        .decode_row_witness(&raw_key)
        .expect("decode index entry")
        .primary_key_value()
        .scalar_component()
        .expect("decode scalar row identity");

    assert_eq!(decoded, key);
    assert_eq!(
        raw.as_bytes(),
        &[IndexEntryExistenceWitness::Present.to_stored_byte()]
    );
}

#[test]
fn index_entry_value_decode_primary_key_component_recovers_key_owned_row_identity() {
    let key = PrimaryKeyComponent::Int64(9);
    let raw_key = raw_key_for(key);
    let raw = IndexEntryValue::presence();

    assert_eq!(
        raw.decode_row_witness(&raw_key)
            .expect("decode key-owned row identity")
            .primary_key_value()
            .scalar_component()
            .expect("decode scalar row identity"),
        key
    );
}

#[test]
fn index_entry_value_presence_decodes_row_identity_from_raw_key() {
    let raw_key_key = PrimaryKeyComponent::Nat64(42);
    let raw_key = raw_key_for(raw_key_key);
    let raw = IndexEntryValue::presence();

    assert_eq!(
        raw.decode_row_witness(&raw_key)
            .expect("decode key-owned row witness")
            .primary_key_value()
            .scalar_component()
            .expect("decode scalar row identity"),
        raw_key_key
    );
    assert_eq!(
        raw.as_bytes(),
        &[IndexEntryExistenceWitness::Present.to_stored_byte()],
        "raw index-entry values must stay presence-only"
    );
}

#[test]
fn index_entry_value_decode_row_witness_recovers_present_witness() {
    let key = PrimaryKeyComponent::Int64(9);
    let raw_key = raw_key_for(key);
    let raw = IndexEntryValue::presence();
    let row_witness = raw
        .decode_row_witness(&raw_key)
        .expect("decode row witness");

    assert_eq!(
        row_witness
            .primary_key_value()
            .scalar_component()
            .expect("scalar row witness"),
        key
    );
    assert_eq!(
        row_witness.primary_key_value(),
        &PrimaryKeyValue::Scalar(key)
    );
    assert_eq!(
        row_witness.existence_witness(),
        IndexEntryExistenceWitness::Present
    );
}

#[test]
fn index_entry_value_decodes_composite_row_identity_from_raw_key() {
    let composite = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(9),
        PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
    ])
    .expect("composite primary key should build");
    let key = PrimaryKeyValue::Composite(composite);
    let raw_key = raw_key_for_primary_key_value(&key);
    let raw = IndexEntryValue::presence();

    let row_witness = raw
        .decode_row_witness(&raw_key)
        .expect("decode composite row witness");
    assert_eq!(row_witness.primary_key_value(), &key);
}

#[test]
fn index_entry_value_roundtrip_via_bytes() {
    let key = PrimaryKeyComponent::Int64(9);
    let raw_key = raw_key_for(key);

    let raw = IndexEntryValue::presence();
    let encoded = Storable::to_bytes(&raw);
    let raw = IndexEntryValue::from_bytes(encoded);
    let decoded = raw
        .decode_row_witness(&raw_key)
        .expect("decode index entry")
        .primary_key_value()
        .scalar_component()
        .expect("decode scalar row identity");

    assert_eq!(decoded, key);
}

#[test]
fn index_entry_value_rejects_empty() {
    let raw_key = raw_key_for(PrimaryKeyComponent::Int64(1));
    let bytes = vec![];
    let raw = IndexEntryValue::from_bytes(Cow::Owned(bytes));
    std::assert_matches!(
        raw.decode_row_witness(&raw_key),
        Err(IndexEntryCorruption::EmptyEntry)
    );
}

#[test]
fn index_entry_value_rejects_invalid_witness() {
    let raw_key = raw_key_for(PrimaryKeyComponent::Int64(1));
    let raw = IndexEntryValue::from_bytes(Cow::Owned(vec![9]));
    std::assert_matches!(
        raw.decode_row_witness(&raw_key),
        Err(IndexEntryCorruption::InvalidWitness)
    );
}

#[test]
fn index_entry_value_rejects_oversized_payload() {
    let raw_key = raw_key_for(PrimaryKeyComponent::Int64(1));
    let bytes = vec![0u8; MAX_INDEX_ENTRY_BYTES as usize + 1];
    let raw = IndexEntryValue::from_bytes(Cow::Owned(bytes));
    std::assert_matches!(
        raw.decode_row_witness(&raw_key),
        Err(IndexEntryCorruption::TooLarge)
    );
}

#[test]
fn index_entry_value_rejects_invalid_raw_key_primary_suffix() {
    let raw = IndexEntryValue::presence();
    let invalid_raw_key = <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![0]));
    std::assert_matches!(
        raw.decode_row_witness(&invalid_raw_key),
        Err(IndexEntryCorruption::InvalidKey)
    );
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn index_entry_value_decode_fuzz_does_not_panic() {
    const RUNS: u64 = 1_000;
    const MAX_LEN: usize = 256;

    let mut seed = 0xA5A5_5A5A_u64;
    for _ in 0..RUNS {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let len = (seed as usize) % MAX_LEN;

        let mut bytes = vec![0u8; len];
        for byte in &mut bytes {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *byte = (seed >> 24) as u8;
        }

        let raw = IndexEntryValue::from_bytes(Cow::Owned(bytes));
        let _ = raw.decode_row_witness(&raw_key_for(PrimaryKeyComponent::Int64(1)));
    }
}
