//! Module: index::scan
//! Responsibility: raw-range index store traversal.
//! Does not own: cursor continuation, executor metrics, predicate execution, or row decoding.
//! Boundary: executor/query range readers wrap this layer with runtime policy.

mod raw;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            direction::Direction,
            index::{IndexStore, RawIndexEntry, RawIndexKey},
        },
        testing::test_memory,
        traits::Storable,
        value::StorageKey,
    };
    use std::{borrow::Cow, ops::Bound};

    #[test]
    fn visit_raw_entries_in_range_preserves_directional_store_order() {
        let mut store = IndexStore::init(test_memory(91));
        for value in [1_u8, 2, 3] {
            let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![value]));
            let raw_entry = RawIndexEntry::try_from_keys([StorageKey::Uint(u64::from(value))])
                .expect("encode index entry");
            store.insert(raw_key, raw_entry);
        }

        let lower = Bound::Included(RawIndexKey::from_bytes(Cow::Owned(vec![1])));
        let upper = Bound::Included(RawIndexKey::from_bytes(Cow::Owned(vec![3])));
        let mut asc = Vec::new();
        store
            .visit_raw_entries_in_range((&lower, &upper), Direction::Asc, |raw_key, _| {
                asc.push(raw_key.as_bytes()[0]);
                Ok(false)
            })
            .expect("asc scan should succeed");
        assert_eq!(asc, vec![1, 2, 3], "asc scan should follow raw key order");

        let mut desc = Vec::new();
        store
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
}
