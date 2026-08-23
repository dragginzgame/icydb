use crate::{
    db::{
        direction::Direction,
        index::{IndexEntryValue, IndexStore, IndexStoreVisit, RawIndexStoreKey},
    },
    testing::test_memory,
};
use ic_stable_structures::Storable;
use std::{borrow::Cow, cell::Cell, ops::Bound};

fn raw_key(value: u8) -> RawIndexStoreKey {
    <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![value]))
}

#[test]
fn visit_raw_entries_in_range_preserves_directional_store_order() {
    let mut index_store = IndexStore::init_journaled(test_memory(91));
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
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let entry_reads_before = IndexStore::current_entry_read_count();
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
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    assert_eq!(
        IndexStore::current_entry_read_count().saturating_sub(entry_reads_before),
        6,
        "journaled raw-range diagnostics must count every delivered entry",
    );
}

#[test]
fn visit_entries_preserves_store_order_and_supports_early_stop() {
    let mut index_store = IndexStore::init_journaled(test_memory(92));
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

#[test]
fn heap_index_store_preserves_range_order_and_early_stop() {
    let mut index_store = IndexStore::init_heap();
    for value in [3_u8, 1, 2] {
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
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let entry_reads_before = IndexStore::current_entry_read_count();
    let mut asc = Vec::new();
    index_store
        .visit_raw_entries_in_range((&lower, &upper), Direction::Asc, |raw_key, _| {
            asc.push(raw_key.as_bytes()[0]);
            Ok(false)
        })
        .expect("heap asc scan should succeed");
    assert_eq!(asc, vec![1, 2, 3]);

    let mut desc = Vec::new();
    index_store
        .visit_raw_entries_in_range((&lower, &upper), Direction::Desc, |raw_key, _| {
            desc.push(raw_key.as_bytes()[0]);
            Ok(false)
        })
        .expect("heap desc scan should succeed");
    assert_eq!(desc, vec![3, 2, 1]);
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    assert_eq!(
        IndexStore::current_entry_read_count().saturating_sub(entry_reads_before),
        6,
        "heap raw-range diagnostics must count every delivered entry",
    );

    let mut stopped = Vec::new();
    let _: Result<(), std::convert::Infallible> = index_store.visit_entries(|raw_key, _| {
        stopped.push(raw_key.as_bytes()[0]);
        Ok(if stopped.len() == 2 {
            IndexStoreVisit::Stop
        } else {
            IndexStoreVisit::Continue
        })
    });
    assert_eq!(stopped, vec![1, 2]);
}

#[test]
fn merged_ranges_preserve_logical_order_direction_and_early_stop() {
    let mut index_store = IndexStore::init_journaled(test_memory(94));
    for value in [10_u8, 11, 12, 20, 21, 22] {
        index_store.insert(raw_key(value), IndexEntryValue::presence());
    }
    index_store
        .fold_journaled_materialized_view()
        .expect("canonical index seed should fold");

    let bounds = [
        (Bound::Included(raw_key(10)), Bound::Included(raw_key(12))),
        (Bound::Included(raw_key(20)), Bound::Included(raw_key(22))),
    ];
    let decode_order = |key: &RawIndexStoreKey| {
        let raw = key.as_bytes()[0];
        Ok::<u8, crate::error::InternalError>(match raw {
            10..=12 => raw.saturating_sub(10).saturating_mul(2).saturating_add(1),
            20..=22 => raw.saturating_sub(20).saturating_mul(2).saturating_add(2),
            _ => return Err(crate::error::InternalError::executor_invariant()),
        })
    };

    let mut asc = Vec::new();
    assert!(
        index_store
            .visit_raw_entries_in_merged_ranges(
                bounds.as_slice(),
                Direction::Asc,
                |_bytes| Ok(()),
                decode_order,
                |order, _key, _value| {
                    asc.push(order);
                    Ok(asc.len() == 4)
                },
            )
            .expect("canonical merged ASC ranges should execute"),
    );
    assert_eq!(asc, vec![1, 2, 3, 4]);

    let mut desc = Vec::new();
    assert!(
        index_store
            .visit_raw_entries_in_merged_ranges(
                bounds.as_slice(),
                Direction::Desc,
                |_bytes| Ok(()),
                decode_order,
                |order, _key, _value| {
                    desc.push(order);
                    Ok(false)
                },
            )
            .expect("canonical merged DESC ranges should execute"),
    );
    assert_eq!(desc, vec![6, 5, 4, 3, 2, 1]);
}

#[test]
fn merged_ranges_admit_complete_structural_state_before_reading() {
    let mut index_store = IndexStore::init_journaled(test_memory(95));
    for value in [10_u8, 11, 20, 21] {
        index_store.insert(raw_key(value), IndexEntryValue::presence());
    }
    index_store
        .fold_journaled_materialized_view()
        .expect("canonical index seed should fold");

    let bounds = [
        (Bound::Included(raw_key(10)), Bound::Included(raw_key(11))),
        (Bound::Included(raw_key(20)), Bound::Included(raw_key(21))),
    ];
    let retained_bound_bytes = bounds
        .iter()
        .flat_map(|(lower, upper)| [lower, upper])
        .map(|bound| match bound {
            Bound::Included(key) | Bound::Excluded(key) => key.as_bytes().len(),
            Bound::Unbounded => 0,
        })
        .sum::<usize>();
    let admitted = Cell::new(0usize);
    let decode_calls = Cell::new(0usize);
    let visit_calls = Cell::new(0usize);

    let _error = index_store
        .visit_raw_entries_in_merged_ranges(
            bounds.as_slice(),
            Direction::Asc,
            |bytes| {
                admitted.set(bytes);
                Err(crate::error::InternalError::executor_internal())
            },
            |_key| {
                decode_calls.set(decode_calls.get().saturating_add(1));
                Ok::<u8, crate::error::InternalError>(0)
            },
            |_order, _key, _value| {
                visit_calls.set(visit_calls.get().saturating_add(1));
                Ok(false)
            },
        )
        .expect_err("structural admission rejection should stop the merged scan");

    assert!(admitted.get() > retained_bound_bytes);
    assert_eq!(decode_calls.get(), 0);
    assert_eq!(visit_calls.get(), 0);
}
