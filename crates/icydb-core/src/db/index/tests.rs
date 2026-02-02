use crate::{
    MAX_INDEX_FIELDS,
    db::{
        identity::{EntityName, IndexName},
        index::{
            IndexEntryCorruption, IndexId, IndexKey, MAX_INDEX_ENTRY_BYTES, MAX_INDEX_ENTRY_KEYS,
            RawIndexEntry, RawIndexKey,
        },
        store::StorageKey,
    },
    traits::Storable,
};
use std::borrow::Cow;

#[test]
fn index_key_rejects_undersized_bytes() {
    let buf = vec![0u8; IndexKey::STORED_SIZE_USIZE - 1];
    let raw = RawIndexKey::from_bytes(Cow::Borrowed(&buf));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(err.contains("corrupted"));
}

#[test]
fn index_key_rejects_oversized_bytes() {
    let buf = vec![0u8; IndexKey::STORED_SIZE_USIZE + 1];
    let raw = RawIndexKey::from_bytes(Cow::Borrowed(&buf));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(err.contains("corrupted"));
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn index_key_rejects_len_over_max() {
    let key = IndexKey::empty(IndexId::max_storable());
    let raw = key.to_raw();
    let len_offset = IndexName::STORED_SIZE_BYTES as usize;
    let mut bytes = raw.as_bytes().to_vec();
    bytes[len_offset] = (MAX_INDEX_FIELDS as u8) + 1;
    let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(err.contains("corrupted"));
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
    assert!(err.contains("corrupted"));
}

#[test]
fn index_key_rejects_fingerprint_padding() {
    let key = IndexKey::empty(IndexId::max_storable());
    let raw = key.to_raw();
    let values_offset = IndexName::STORED_SIZE_USIZE + 1;
    let mut bytes = raw.as_bytes().to_vec();
    bytes[values_offset] = 1;
    let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
    let err = IndexKey::try_from_raw(&raw).unwrap_err();
    assert!(err.contains("corrupted"));
}

#[test]
#[expect(clippy::large_types_passed_by_value)]
fn index_key_ordering_matches_bytes() {
    fn make_key(index_id: IndexId, value_count: u8, first: u8, second: u8) -> IndexKey {
        let mut bytes = [0u8; IndexKey::STORED_SIZE_USIZE];

        let name_bytes = index_id.0.to_bytes();
        bytes[..name_bytes.len()].copy_from_slice(&name_bytes);

        let mut offset = IndexName::STORED_SIZE_USIZE;
        bytes[offset] = value_count;
        offset += 1;

        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        values[0] = [first; 16];
        values[1] = [second; 16];

        for value in values {
            bytes[offset..offset + 16].copy_from_slice(&value);
            offset += 16;
        }

        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
        IndexKey::try_from_raw(&raw).expect("valid IndexKey")
    }

    let entity = EntityName::try_from_str("entity").unwrap();

    let idx_a = IndexId(IndexName::try_from_parts(&entity, &["a"]).unwrap());
    let idx_b = IndexId(IndexName::try_from_parts(&entity, &["b"]).unwrap());

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

    assert_eq!(sorted_by_ord, sorted_by_bytes);
}

#[test]
fn raw_index_entry_round_trip() {
    let keys = vec![StorageKey::Int(1), StorageKey::Uint(2)];

    let raw = RawIndexEntry::try_from_storage_keys(keys.clone()).expect("encode index entry");
    let decoded = raw.decode_keys().expect("decode index entry");

    assert_eq!(decoded.len(), keys.len());
    assert!(decoded.contains(&StorageKey::Int(1)));
    assert!(decoded.contains(&StorageKey::Uint(2)));
}

#[test]
fn raw_index_entry_roundtrip_via_bytes() {
    let keys = vec![StorageKey::Int(9), StorageKey::Uint(10)];

    let raw = RawIndexEntry::try_from_storage_keys(keys.clone()).expect("encode index entry");
    let encoded = Storable::to_bytes(&raw);
    let raw = RawIndexEntry::from_bytes(encoded);
    let decoded = raw.decode_keys().expect("decode index entry");

    assert_eq!(decoded.len(), keys.len());
    assert!(decoded.contains(&StorageKey::Int(9)));
    assert!(decoded.contains(&StorageKey::Uint(10)));
}

#[test]
fn raw_index_entry_rejects_empty() {
    let bytes = vec![0, 0, 0, 0];
    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.decode_keys(),
        Err(IndexEntryCorruption::EmptyEntry)
    ));
}

#[test]
fn raw_index_entry_rejects_truncated_payload() {
    let key = StorageKey::Int(1);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&key.to_bytes().expect("encode"));
    bytes.truncate(bytes.len() - 1);

    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.decode_keys(),
        Err(IndexEntryCorruption::LengthMismatch)
    ));
}

#[test]
fn raw_index_entry_rejects_oversized_payload() {
    let bytes = vec![0u8; MAX_INDEX_ENTRY_BYTES as usize + 1];
    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.decode_keys(),
        Err(IndexEntryCorruption::TooLarge { .. })
    ));
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn raw_index_entry_rejects_corrupted_length_field() {
    let count = (MAX_INDEX_ENTRY_KEYS + 1) as u32;
    let raw = RawIndexEntry::from_bytes(Cow::Owned(count.to_be_bytes().to_vec()));
    assert!(matches!(
        raw.decode_keys(),
        Err(IndexEntryCorruption::TooManyKeys { .. })
    ));
}

#[test]
fn raw_index_entry_rejects_duplicate_keys() {
    let key = StorageKey::Int(1);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&2u32.to_be_bytes());
    bytes.extend_from_slice(&key.to_bytes().expect("encode"));
    bytes.extend_from_slice(&key.to_bytes().expect("encode"));

    let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
    assert!(matches!(
        raw.decode_keys(),
        Err(IndexEntryCorruption::DuplicateKey)
    ));
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn index_key_decode_fuzz_roundtrip_is_canonical() {
    const RUNS: u64 = 1_000;

    let mut seed = 0xBADC_0FFE_u64;
    for _ in 0..RUNS {
        let mut bytes = [0u8; IndexKey::STORED_SIZE_BYTES as usize];
        for b in &mut bytes {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *b = (seed >> 24) as u8;
        }

        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
        if let Ok(decoded) = IndexKey::try_from_raw(&raw) {
            let re = decoded.to_raw();
            assert_eq!(raw.as_bytes(), re.as_bytes());
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
        let _ = raw.decode_keys();
    }
}
