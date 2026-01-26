use crate::{
    MAX_INDEX_FIELDS,
    db::{
        identity::{EntityName, IndexName},
        index::*,
    },
    key::Key,
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    traits::Storable,
    traits::{
        CanisterKind, EntityKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind,
        ValidateAuto, ValidateCustom, View, ViewError, Visitable,
    },
    types::Ulid,
    value::Value,
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cell::RefCell};

#[test]
fn index_key_rejects_undersized_bytes() {
    let buf = vec![0u8; IndexKey::STORED_SIZE as usize - 1];
    let raw = RawIndexKey::from_bytes(Cow::Borrowed(&buf));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(
        err.contains("corrupted"),
        "expected corruption error, got: {err}"
    );
}

#[test]
fn index_key_rejects_oversized_bytes() {
    let buf = vec![0u8; IndexKey::STORED_SIZE as usize + 1];
    let raw = RawIndexKey::from_bytes(Cow::Borrowed(&buf));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(
        err.contains("corrupted"),
        "expected corruption error, got: {err}"
    );
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn index_key_rejects_len_over_max() {
    let key = IndexKey::empty(IndexId::max_storable());
    let raw = key.to_raw();
    let len_offset = IndexName::STORED_SIZE as usize;
    let mut bytes = raw.as_bytes().to_vec();
    bytes[len_offset] = (MAX_INDEX_FIELDS as u8) + 1;
    let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(
        err.contains("corrupted"),
        "expected corruption error, got: {err}"
    );
}

#[test]
fn index_key_rejects_invalid_index_name() {
    let key = IndexKey::empty(IndexId::max_storable());
    let raw = key.to_raw();
    let mut bytes = raw.as_bytes().to_vec();
    bytes[0] = 0;
    bytes[1] = 0;
    let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(
        err.contains("corrupted"),
        "expected corruption error, got: {err}"
    );
}

#[test]
fn index_key_rejects_fingerprint_padding() {
    let key = IndexKey::empty(IndexId::max_storable());
    let raw = key.to_raw();
    let values_offset = IndexName::STORED_SIZE as usize + 1;
    let mut bytes = raw.as_bytes().to_vec();
    bytes[values_offset] = 1;
    let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(
        err.contains("corrupted"),
        "expected corruption error, got: {err}"
    );
}

#[test]
#[expect(clippy::large_types_passed_by_value)]
fn index_key_ordering_matches_bytes() {
    fn make_key(index_id: IndexId, len: u8, first: u8, second: u8) -> IndexKey {
        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        values[0] = [first; 16];
        values[1] = [second; 16];

        let mut bytes = [0u8; IndexKey::STORED_SIZE as usize];
        let name_bytes = index_id.0.to_bytes();
        bytes[..name_bytes.len()].copy_from_slice(&name_bytes);

        let mut offset = IndexName::STORED_SIZE as usize;
        bytes[offset] = len;
        offset += 1;

        for value in values {
            bytes[offset..offset + 16].copy_from_slice(&value);
            offset += 16;
        }

        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
        IndexKey::try_from_raw(&raw).expect("valid IndexKey")
    }

    let entity = EntityName::from_static_unchecked("entity");
    let idx_a = IndexId(IndexName::from_parts_unchecked(&entity, &["a"]));
    let idx_b = IndexId(IndexName::from_parts_unchecked(&entity, &["b"]));

    let keys = vec![
        make_key(idx_a, 1, 1, 0),
        make_key(idx_a, 2, 1, 2),
        make_key(idx_a, 1, 2, 0),
        make_key(idx_b, 1, 0, 0),
    ];

    let mut sorted_by_ord = keys.clone();
    sorted_by_ord.sort();

    let mut sorted_by_bytes = keys;
    sorted_by_bytes.sort_by(|a, b| a.to_raw().as_bytes().cmp(b.to_raw().as_bytes()));

    assert_eq!(
        sorted_by_ord, sorted_by_bytes,
        "IndexKey Ord and byte ordering diverged"
    );
}

#[test]
fn raw_index_entry_round_trip() {
    let mut entry = IndexEntry::new(Key::Int(1));
    entry.insert_key(Key::Uint(2));

    let raw = RawIndexEntry::try_from_entry(&entry).expect("encode index entry");
    let decoded = raw.try_decode().expect("decode index entry");

    assert_eq!(decoded.len(), entry.len());
    assert!(decoded.contains(&Key::Int(1)));
    assert!(decoded.contains(&Key::Uint(2)));
}

#[test]
fn raw_index_entry_roundtrip_via_bytes() {
    let mut entry = IndexEntry::new(Key::Int(9));
    entry.insert_key(Key::Uint(10));

    let raw = RawIndexEntry::try_from_entry(&entry).expect("encode index entry");
    let encoded = Storable::to_bytes(&raw);
    let raw = RawIndexEntry::from_bytes(encoded);
    let decoded = raw.try_decode().expect("decode index entry");

    assert_eq!(decoded.len(), entry.len());
    assert!(decoded.contains(&Key::Int(9)));
    assert!(decoded.contains(&Key::Uint(10)));
}

#[test]
fn raw_index_entry_rejects_empty() {
    let bytes = vec![0, 0, 0, 0];
    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.try_decode(),
        Err(IndexEntryCorruption::EmptyEntry)
    ));
}

#[test]
fn raw_index_entry_rejects_truncated_payload() {
    let key = Key::Int(1);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&key.to_bytes().expect("key encode"));
    bytes.truncate(bytes.len() - 1);
    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.try_decode(),
        Err(IndexEntryCorruption::LengthMismatch)
    ));
}

#[test]
fn raw_index_entry_rejects_oversized_payload() {
    let bytes = vec![0u8; MAX_INDEX_ENTRY_BYTES as usize + 1];
    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.try_decode(),
        Err(IndexEntryCorruption::TooLarge { .. })
    ));
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn raw_index_entry_rejects_corrupted_length_field() {
    let count = (MAX_INDEX_ENTRY_KEYS + 1) as u32;
    let raw = RawIndexEntry::from_bytes(Cow::Owned(count.to_be_bytes().to_vec()));
    assert!(matches!(
        raw.try_decode(),
        Err(IndexEntryCorruption::TooManyKeys { .. })
    ));
}

#[test]
fn raw_index_entry_rejects_duplicate_keys() {
    let key = Key::Int(1);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&2u32.to_be_bytes());
    bytes.extend_from_slice(&key.to_bytes().expect("key encode"));
    bytes.extend_from_slice(&key.to_bytes().expect("key encode"));
    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.try_decode(),
        Err(IndexEntryCorruption::DuplicateKey)
    ));
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn index_key_decode_fuzz_roundtrip_is_canonical() {
    const RUNS: u64 = 1_000;

    let mut seed = 0xBADC_0FFE_u64;
    for _ in 0..RUNS {
        let mut bytes = [0u8; IndexKey::STORED_SIZE as usize];
        for b in &mut bytes {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *b = (seed >> 24) as u8;
        }

        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
        if let Ok(decoded) = IndexKey::try_from_raw(&raw) {
            let re = decoded.to_raw();
            assert_eq!(
                raw.as_bytes(),
                re.as_bytes(),
                "decoded IndexKey must be canonical"
            );
        }
    }
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn raw_index_entry_decode_fuzz_does_not_panic() {
    const RUNS: u64 = 1_000;
    const MAX_LEN: usize = 256;

    let mut seed = 0xA5A5_5A5A_u64;
    for _ in 0..RUNS {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let len = (seed as usize) % MAX_LEN;

        let mut bytes = vec![0u8; len];
        for b in &mut bytes {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *b = (seed >> 24) as u8;
        }

        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        let _ = raw.try_decode();
    }
}

const NULL_ENTITY_PATH: &str = "index_null_test::NullIndexEntity";
const NULL_INDEX_STORE_PATH: &str = "index_null_test::NullIndexStore";
const NULL_INDEX_FIELDS: [&str; 1] = ["tag"];
const NULL_INDEX_MODEL: IndexModel = IndexModel::new(
    "index_null_test::tag_unique",
    NULL_INDEX_STORE_PATH,
    &NULL_INDEX_FIELDS,
    true,
);
const NULL_INDEXES: [&IndexModel; 1] = [&NULL_INDEX_MODEL];
const NULL_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "tag",
        kind: EntityFieldKind::Text,
    },
];
const NULL_MODEL: EntityModel = EntityModel {
    path: NULL_ENTITY_PATH,
    entity_name: "NullIndexEntity",
    primary_key: &NULL_FIELDS[0],
    fields: &NULL_FIELDS,
    indexes: &NULL_INDEXES,
};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct NullIndexEntity {
    id: Ulid,
    tag: Option<String>,
}

impl Path for NullIndexEntity {
    const PATH: &'static str = NULL_ENTITY_PATH;
}

impl View for NullIndexEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        Ok(view)
    }
}

impl SanitizeAuto for NullIndexEntity {}
impl SanitizeCustom for NullIndexEntity {}
impl ValidateAuto for NullIndexEntity {}
impl ValidateCustom for NullIndexEntity {}
impl Visitable for NullIndexEntity {}

impl FieldValues for NullIndexEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(Value::Ulid(self.id)),
            "tag" => match &self.tag {
                Some(tag) => Some(Value::Text(tag.clone())),
                None => Some(Value::None),
            },
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct NullIndexCanister;

impl Path for NullIndexCanister {
    const PATH: &'static str = "index_null_test::NullIndexCanister";
}

impl CanisterKind for NullIndexCanister {}

struct NullIndexStore;

impl Path for NullIndexStore {
    const PATH: &'static str = NULL_INDEX_STORE_PATH;
}

impl StoreKind for NullIndexStore {
    type Canister = NullIndexCanister;
}

impl EntityKind for NullIndexEntity {
    type PrimaryKey = Ulid;
    type Store = NullIndexStore;
    type Canister = NullIndexCanister;

    const ENTITY_NAME: &'static str = "NullIndexEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "tag"];
    const INDEXES: &'static [&'static IndexModel] = &NULL_INDEXES;
    const MODEL: &'static EntityModel = &NULL_MODEL;

    fn key(&self) -> Key {
        self.id.into()
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

canic_memory::eager_static! {
    static NULL_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(canic_memory::ic_memory!(IndexStore, 12)));
}

canic_memory::eager_init!({
    canic_memory::ic_memory_range!(0, 20);
});

#[test]
fn unique_index_allows_multiple_non_indexable_values() {
    NULL_INDEX_STORE.with_borrow_mut(|store| store.clear());

    let first = NullIndexEntity {
        id: Ulid::from_u128(1),
        tag: None,
    };
    let second = NullIndexEntity {
        id: Ulid::from_u128(2),
        tag: None,
    };

    let outcome_first = NULL_INDEX_STORE
        .with_borrow_mut(|store| store.insert_index_entry(&first, &NULL_INDEX_MODEL));
    let outcome_second = NULL_INDEX_STORE
        .with_borrow_mut(|store| store.insert_index_entry(&second, &NULL_INDEX_MODEL));

    assert!(
        matches!(outcome_first, Ok(IndexInsertOutcome::Skipped)),
        "expected non-indexable value to skip indexing"
    );
    assert!(
        matches!(outcome_second, Ok(IndexInsertOutcome::Skipped)),
        "expected non-indexable value to skip indexing"
    );
    assert!(
        NULL_INDEX_STORE.with_borrow(|store| store.is_empty()),
        "index store should remain empty for non-indexable values"
    );
}
