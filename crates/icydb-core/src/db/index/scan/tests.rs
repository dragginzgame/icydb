use crate::{
    db::{
        direction::Direction,
        index::{IndexEntryValue, IndexStore, IndexStoreVisit, RawIndexStoreKey},
    },
    testing::test_memory,
    traits::Storable,
};
use std::{borrow::Cow, ops::Bound};

#[test]
fn visit_raw_entries_in_range_preserves_directional_store_order() {
    let mut index_store = IndexStore::init(test_memory(91));
    for value in [1_u8, 2, 3] {
        let raw_key = <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![value]));
        let raw_entry = IndexEntryValue::presence();
        index_store.insert(raw_key, raw_entry);
    }

    let lower = Bound::Included(<RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(
        vec![1],
    )));
    let upper = Bound::Included(<RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(
        vec![3],
    )));
    let mut asc = Vec::new();
    index_store
        .visit_raw_entries_in_range((&lower, &upper), Direction::Asc, |raw_key, _| {
            asc.push(raw_key.as_bytes()[0]);
            Ok(false)
        })
        .expect("asc scan should succeed");
    assert_eq!(asc, vec![1, 2, 3], "asc scan should follow raw key order");

    let mut desc = Vec::new();
    index_store
        .visit_raw_entries_in_range((&lower, &upper), Direction::Desc, |raw_key, _| {
            desc.push(raw_key.as_bytes()[0]);
            Ok(false)
        })
        .expect("desc scan should succeed");
    assert_eq!(
        desc,
        vec![3, 2, 1],
        "desc scan should reverse raw key order"
    );
}

#[test]
fn visit_entries_preserves_store_order_and_supports_early_stop() {
    let mut index_store = IndexStore::init(test_memory(92));
    for value in [3_u8, 1, 2] {
        let raw_key = <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![value]));
        let raw_entry = IndexEntryValue::presence();
        index_store.insert(raw_key, raw_entry);
    }

    let mut visited = Vec::new();
    let _: Result<(), std::convert::Infallible> = index_store.visit_entries(|raw_key, _| {
        visited.push(raw_key.as_bytes()[0]);
        Ok(if visited.len() == 2 {
            IndexStoreVisit::Stop
        } else {
            IndexStoreVisit::Continue
        })
    });

    assert_eq!(
        visited,
        vec![1, 2],
        "index entry traversal should preserve raw store order and stop without allocation"
    );
}
