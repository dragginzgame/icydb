use icydb::__internal::core::db::{
    index::{IndexEntry, IndexKey, RawIndexEntry},
    store::{DataKey, RawRow},
};
use icydb::{
    db::query::{Query, ReadConsistency},
    design::prelude::*,
    error::ErrorClass,
    serialize,
};
use test_design::{e2e::db::Index, schema::TestIndexStore};

pub struct DeleteUniqueSuite;

impl DeleteUniqueSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            (
                "delete_unique_dangling_index_entry_errors",
                Self::delete_unique_dangling_index_entry_errors,
            ),
            (
                "delete_unique_index_corruption_errors",
                Self::delete_unique_index_corruption_errors,
            ),
            (
                "delete_unique_key_type_mismatch_errors",
                Self::delete_unique_key_type_mismatch_errors,
            ),
            (
                "delete_unique_missing_primary_row_errors",
                Self::delete_unique_missing_primary_row_errors,
            ),
        ];

        for (name, test_fn) in tests {
            crate::clear_test_data_store();
            println!("Running test: {name}");
            test_fn();
        }
    }

    fn delete_unique_dangling_index_entry_errors() {
        let saved = db!().insert(Index::new(1, 55)).unwrap();

        let index = Self::unique_index();
        let index_key = IndexKey::new(&saved, index).expect("index key should be present");

        crate::DATA_REGISTRY
            .with(|reg| {
                reg.with_store_mut(<Index as EntityKind>::Store::PATH, |store| {
                    let data_key = DataKey::new::<Index>(saved.key());
                    store.remove(&data_key.to_raw());
                })
            })
            .unwrap();

        let plan = Query::<Index>::new(ReadConsistency::Strict)
            .filter(eq("y", 55))
            .plan()
            .expect("plan");
        let err = crate::db_core()
            .delete::<Index>()
            .execute(plan)
            .map_err(icydb::Error::from)
            .unwrap_err();

        assert_eq!(err.class, ErrorClass::Corruption);

        let _ = crate::INDEX_REGISTRY.with(|reg| {
            reg.with_store_mut(TestIndexStore::PATH, |store| {
                store.remove(&index_key.to_raw())
            })
        });
    }

    fn delete_unique_index_corruption_errors() {
        let saved = db!().insert(Index::new(1, 88)).unwrap();
        let other = db!().insert(Index::new(2, 99)).unwrap();

        let index = Self::unique_index();
        let index_key = IndexKey::new(&saved, index).expect("index key should be present");

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let raw = index_key.to_raw();
                    let mut entry = store
                        .get(&raw)
                        .expect("index entry should exist")
                        .try_decode()
                        .expect("index entry should decode");
                    entry.insert_key(other.key());
                    let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
                    store.insert(raw, raw_entry);
                })
            })
            .unwrap();

        let plan = Query::<Index>::new(ReadConsistency::MissingOk)
            .filter(eq("y", 88))
            .plan()
            .expect("plan");
        let err = crate::db_core()
            .delete::<Index>()
            .execute(plan)
            .map_err(icydb::Error::from)
            .unwrap_err();

        assert_eq!(err.class, ErrorClass::Corruption);
    }

    fn delete_unique_key_type_mismatch_errors() {
        let index = Self::unique_index();
        let index_key =
            IndexKey::new(&Index::new(1, 777), index).expect("index key should be present");
        let bad_key = Key::Uint(123);

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let entry = IndexEntry::new(bad_key);
                    let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
                    store.insert(index_key.to_raw(), raw_entry);
                })
            })
            .unwrap();

        let bytes = serialize(&Index::new(9, 777)).unwrap();
        crate::DATA_REGISTRY
            .with(|reg| {
                reg.with_store_mut(<Index as EntityKind>::Store::PATH, |store| {
                    let data_key = DataKey::new::<Index>(bad_key);
                    store.insert(data_key.to_raw(), RawRow::try_new(bytes).unwrap());
                })
            })
            .unwrap();

        let plan = Query::<Index>::new(ReadConsistency::MissingOk)
            .filter(eq("y", 777))
            .plan()
            .expect("plan");
        let err = crate::db_core()
            .delete::<Index>()
            .execute(plan)
            .map_err(icydb::Error::from)
            .unwrap_err();

        assert_eq!(err.class, ErrorClass::Corruption);
    }

    fn delete_unique_missing_primary_row_errors() {
        let index = Self::unique_index();
        let index_key =
            IndexKey::new(&Index::new(1, 444), index).expect("index key should be present");
        let missing_key = Key::Ulid(Ulid::generate());

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let entry = IndexEntry::new(missing_key);
                    let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
                    store.insert(index_key.to_raw(), raw_entry);
                })
            })
            .unwrap();

        let plan = Query::<Index>::new(ReadConsistency::Strict)
            .filter(eq("y", 444))
            .plan()
            .expect("plan");
        let err = crate::db_core()
            .delete::<Index>()
            .execute(plan)
            .map_err(icydb::Error::from)
            .unwrap_err();

        assert_eq!(err.class, ErrorClass::Corruption);
    }

    fn unique_index() -> &'static icydb::model::index::IndexModel {
        Index::INDEXES
            .iter()
            .find(|idx| idx.fields == ["y"])
            .expect("expected unique index on y")
    }
}
