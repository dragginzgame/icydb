//! Deterministic malformed-byte corpus for DB-owned persisted decode envelopes.

use crate::{
    db::{
        codec::{ROW_FORMAT_VERSION_CURRENT, decode_row_payload_bytes},
        data::{
            decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
            validate_structural_field_by_kind_bytes, validate_structural_value_storage_bytes,
        },
        index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::decode_persisted_schema_snapshot,
    },
    model::field::FieldKind,
    traits::Storable,
    types::EntityTag,
};
use std::borrow::Cow;

const VALUE_TAG_TEXT: u8 = 0x12;
const VALUE_TAG_LIST: u8 = 0x20;
static TEXT_FIELD_KIND: FieldKind = FieldKind::Text { max_len: None };
static TEXT_LIST_FIELD_KIND: FieldKind = FieldKind::List(&TEXT_FIELD_KIND);

fn assert_err<T, E>(label: &str, result: Result<T, E>) {
    assert!(result.is_err(), "{label} should fail closed");
}

fn row_envelope(magic: [u8; 2], version: u8, declared_len: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(7 + payload.len());
    bytes.extend_from_slice(&magic);
    bytes.push(version);
    bytes.extend_from_slice(&declared_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn valid_raw_index_key() -> RawIndexStoreKey {
    let component = vec![0x42];
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(EntityTag::new(0x1902), 1),
        IndexKeyKind::User,
        std::slice::from_ref(&component),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    )
    .expect("test index key should build")
    .to_raw()
    .expect("test index key should encode")
}

fn unchecked_raw_index_key(bytes: Vec<u8>) -> RawIndexStoreKey {
    <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(bytes))
}

fn value_storage_len_prefixed(tag: u8, declared_len: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + payload.len());
    bytes.push(tag);
    bytes.extend_from_slice(&declared_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn persisted_row_envelope_malformed_corpus_fails_closed() {
    let cases = [
        ("empty row envelope", Vec::new()),
        ("truncated row magic", vec![b'I']),
        (
            "bad row magic",
            row_envelope(*b"XX", ROW_FORMAT_VERSION_CURRENT, 0, &[]),
        ),
        (
            "future row version",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT.saturating_add(1), 0, &[]),
        ),
        (
            "declared row payload too long",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT, 4, &[0xAA]),
        ),
        (
            "trailing row payload bytes",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT, 0, &[0xAA]),
        ),
        (
            "huge declared row payload without bytes",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT, u32::MAX, &[]),
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, decode_row_payload_bytes(&bytes));
    }
}

#[test]
fn persisted_schema_snapshot_malformed_corpus_fails_closed() {
    let cases = [
        ("empty schema snapshot", Vec::new()),
        ("truncated candid schema snapshot", vec![0x44]),
        (
            "nonsense schema snapshot bytes",
            vec![0xDE, 0xAD, 0xBE, 0xEF],
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, decode_persisted_schema_snapshot(&bytes));
    }
}

#[test]
fn persisted_index_envelope_malformed_corpus_fails_closed() {
    let valid_key = valid_raw_index_key();
    let mut unknown_kind = valid_key.as_bytes().to_vec();
    unknown_kind[0] = 0xFF;
    let mut truncated = valid_key.as_bytes().to_vec();
    truncated.pop();
    let mut trailing = valid_key.as_bytes().to_vec();
    trailing.push(0x42);

    let key_cases = [
        ("empty index key", Vec::new()),
        ("unknown index key kind", unknown_kind),
        ("truncated index key", truncated),
        ("trailing index key bytes", trailing),
    ];
    for (label, bytes) in key_cases {
        let raw = unchecked_raw_index_key(bytes);
        assert_err(label, IndexKey::try_from_raw(&raw));
    }

    let entry_cases = [
        ("empty index entry", Vec::new()),
        ("invalid index entry witness", vec![0xFF]),
        ("oversized index entry", vec![0; 2]),
    ];
    for (label, bytes) in entry_cases {
        let entry = IndexEntryValue::from_bytes(Cow::Owned(bytes));
        assert_err(label, entry.validate());
    }
}

#[test]
fn structural_value_storage_malformed_corpus_fails_closed() {
    let cases = [
        ("empty structural value", Vec::new()),
        ("unknown structural value tag", vec![0xFF]),
        (
            "truncated structural text",
            value_storage_len_prefixed(VALUE_TAG_TEXT, 4, b"a"),
        ),
        (
            "huge declared structural list without items",
            value_storage_len_prefixed(VALUE_TAG_LIST, u32::MAX, &[]),
        ),
        (
            "trailing structural bytes",
            value_storage_len_prefixed(VALUE_TAG_LIST, 0, &[0x00]),
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, validate_structural_value_storage_bytes(&bytes));
        assert_err(label, decode_structural_value_storage_bytes(&bytes));
    }
}

#[test]
fn structural_field_by_kind_malformed_corpus_fails_closed() {
    let cases = [
        (
            "truncated by-kind text",
            FieldKind::Text { max_len: None },
            value_storage_len_prefixed(VALUE_TAG_TEXT, 4, b"a"),
        ),
        ("empty by-kind nat64", FieldKind::Nat64, Vec::new()),
        (
            "huge declared by-kind text list without items",
            TEXT_LIST_FIELD_KIND,
            value_storage_len_prefixed(VALUE_TAG_LIST, u32::MAX, &[]),
        ),
    ];

    for (label, kind, bytes) in cases {
        assert_err(label, validate_structural_field_by_kind_bytes(&bytes, kind));
        assert_err(label, decode_structural_field_by_kind_bytes(&bytes, kind));
    }
}
